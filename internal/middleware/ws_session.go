package middleware

import (
	"context"
	"net/http"

	"IM2/pkg/proto/transport"
	"IM2/pkg/resultx"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/rest"
)

// WSSessionChecker 能在 WS 建连时校验 JWT 中的 deviceID 和 ver claim 与 Redis session 是否一致。
type WSSessionChecker interface {
	CheckWSSession(ctx context.Context, tokenString string) error
}

// SessionVersionChecker 已废弃，保留为兼容。请使用 WSSessionChecker。
//
// Deprecated: 使用 WSSessionChecker。
type SessionVersionChecker interface {
	WSSessionChecker
	CheckSessionVersion(ctx context.Context, tokenString string) error
}

// WithWsSessionAuth WebSocket 连接专用鉴权中间件。
// 必须叠加在 WithJwtAuth 之后（即 userID 已写入 context）。
//
// 校验流程：
//
//  1. AT 的 exp 已由前置中间件验证通过。
//  2. 读取 session：session 不存在（TTL 过期）→ 401，客户端刷新 token 后重建 WS。
//  3. ver 不一致：session 已被新登录覆盖 → ERR_KICKED_OUT，WS 层关闭旧连接。
//  4. deviceID 为空：token 非法 → 拒绝建连。
//
// 使用示例（路由注册）：
//
//	rest.Route{
//	    Method:  http.MethodGet,
//	    Path:    "/ws",
//	    Handler: wsHandler,
//	}.WithMiddlewares(
//	    middleware.WithJwtAuth(tokenManager),
//	    middleware.WithWsSessionAuth(tokenManager),
//	)
func WithWsSessionAuth(checker WSSessionChecker) rest.Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			tokenString := tokenmanager.ExtractToken(r)
			if tokenString == "" {
				// 理论上经过 WithJwtAuth 后不会为空，保守兆底
				err := xerr.New(transport.ErrorCode_ERR_UNAUTHORIZED, "身份已过期")
				resultx.ErrorProtoCtx(ctx, w, r, err)
				return
			}

			// 同时校验 JWT.deviceID 非空 和 JWT.ver == session.version
			if err := checker.CheckWSSession(ctx, tokenString); err != nil {
				if v, ok := err.(*xerr.Error); ok {
					resultx.ErrorProtoCtx(ctx, w, r, v)
					return
				}
				resultx.ErrorProtoCtx(ctx, w, r, xerr.Wrap(err, transport.ErrorCode_ERR_KICKED_OUT, "会话校验失败"))
				return
			}

			next(w, r)
		}
	}
}
