package message

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"strconv"
	"time"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/pkg/proto/transport"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
)

// DefaultTurnTTLSeconds 未配置 TTL 时的默认凭证有效期。
// 取 12 小时：足够覆盖一次登录会话内的多通电话，避免每通都回源；
// 又远短于 refreshToken，泄漏后的可滥用窗口有限。
const DefaultTurnTTLSeconds int64 = 12 * 3600

type GetTurnCredentialLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取 TURN 短时凭证
func NewGetTurnCredentialLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTurnCredentialLogic {
	return &GetTurnCredentialLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetTurnCredential 按 TURN REST API 规范现算一组短时凭证。
//
//	username   = "<过期 Unix 秒>:<userId>"
//	credential = base64(HMAC-SHA1(static-auth-secret, username))
//
// coturn 在 use-auth-secret 模式下不查用户表，而是用同一密钥重算 HMAC 做比对，
// 并把 username 冒号前的部分当作过期时间自行校验。因此服务端无需存储任何凭证。
//
// username 里带上 userId 只为排查时能从 coturn 日志定位到人，coturn 不解释这部分。
func (l *GetTurnCredentialLogic) GetTurnCredential(_ *types.GetTurnCredentialReq) (*types.GetTurnCredentialResp, error) {
	cfg := l.svcCtx.Config.Turn

	// 没配 TURN 时不要返回一个只有 STUN 的半成品：客户端会以为拿到了可用配置，
	// 直到对称 NAT 场景才失败，且失败点离根因很远。这里直接报错更好定位。
	if cfg.Secret == "" || len(cfg.Urls) == 0 {
		return nil, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "TURN 服务未配置")
	}

	userID := tokenmanager.ExtractIDFromCtx(l.ctx)
	if userID == 0 {
		return nil, xerr.New(transport.ErrorCode_ERR_UNAUTHORIZED, "未登录")
	}

	ttl := cfg.TTLSeconds
	if ttl <= 0 {
		ttl = DefaultTurnTTLSeconds
	}
	expiresAt := time.Now().Unix() + ttl

	username := strconv.FormatInt(expiresAt, 10) + ":" + strconv.FormatUint(userID, 10)

	mac := hmac.New(sha1.New, []byte(cfg.Secret))
	mac.Write([]byte(username))
	credential := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	servers := make([]*types.IceServer, 0, 2)
	// STUN 在前：多数情况下打洞就能成，客户端按顺序尝试，先放开销小的
	if len(cfg.StunUrls) > 0 {
		servers = append(servers, &types.IceServer{Urls: cfg.StunUrls})
	}
	servers = append(servers, &types.IceServer{
		Urls:       cfg.Urls,
		Username:   username,
		Credential: credential,
	})

	return &types.GetTurnCredentialResp{
		IceServers: servers,
		ExpiresAt:  expiresAt,
	}, nil
}
