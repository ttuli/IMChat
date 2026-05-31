package middleware

import (
	"context"
	"fmt"
	"net/http"

	"IM2/pkg/resultx"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/rest"
)

// TokenValidator token 验证器接口
type TokenValidator interface {
	// ValidateToken 验证 token 并返回 userID
	ValidateToken(ctx context.Context, tokenString string) (uint64, error)
}

// WithRedisJwtAuth JWT 验证中间件
func WithRedisJwtAuth(validator TokenValidator) rest.Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// 1. 提取 token
			tokenString := tokenmanager.ExtractToken(r)
			if tokenString == "" {
				resultx.ErrorProtoCtx(ctx, w, r, xerr.New(xerr.ErrUnauthorized, "身份已过期"))
				return
			}
			fmt.Println(tokenString)
			// 2. 验证 token
			userID, err := validator.ValidateToken(ctx, tokenString)
			if err != nil {
				fmt.Println("err")
				fmt.Println(err)
				fmt.Println("err")
				if v, ok := err.(*xerr.Error); ok {
					resultx.ErrorProtoCtx(ctx, w, r, v)
					return
				}
				resultx.ErrorProtoCtx(ctx, w, r, xerr.Wrap(err, xerr.ErrUnauthorized, "身份已过期"))
				return
			}

			// 3. 将 userID 存入 context
			r = r.WithContext(context.WithValue(ctx, tokenmanager.ContextKeyUserID, userID))

			next(w, r)
		}
	}
}
