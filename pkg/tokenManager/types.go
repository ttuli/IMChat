package tokenmanager

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ---- Token 类型 ----

type TokenType int

const (
	AccessToken  TokenType = iota
	RefreshToken TokenType = iota
)

// ---- 配置 ----

type TokenConfig struct {
	RedisConf redis.RedisConf

	JWTConfig struct {
		Secret        string `json:"secret"`
		Expire        int64  `json:"expire"`
		RefreshExpire int64  `json:"refreshExpire"`
	}
}

// ---- TokenManager ----

type TokenManager struct {
	c TokenConfig
	*redis.Redis
}

func NewTokenManager(c TokenConfig) *TokenManager {
	return &TokenManager{
		c:     c,
		Redis: redis.MustNewRedis(c.RedisConf),
	}
}

// ---- Redis Key 常量 ----

const (
	// Context key
	ContextKeyUserID = "user_id"

	// JWT Claim Keys
	ClaimKeyExp      = "exp"
	ClaimKeyIat      = "iat"
	ClaimKeyType     = "token_type"
	ClaimKeyPlatform = "platform"
	ClaimKeyDeviceID = "device_id"
	ClaimKeyVersion  = "ver" // session version，登录时写入 AT，用于 WS 重连校验

	// JWT Token Type 值
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"

	// Redis key 前缀
	// at:{userID}:{deviceId}   — access token（有 TTL）
	// rt:{userID}:{deviceId}   — refresh token（remember_me=true 时设置，TTL=RefreshExpire）
	// session:{userID}         — 用户会话元数据（无 TTL）：machine_id、version
	atKeyPrefix      = "at:"
	rtKeyPrefix      = "rt:"
	sessionKeyPrefix = "session:"
)

// buildATKey 构建 Access Token 的 Redis key: at:{userID}:{deviceId}
func buildATKey(userID uint64, deviceID string) string {
	return fmt.Sprintf("%s%d:%s", atKeyPrefix, userID, deviceID)
}

// buildRTKey 构建 Refresh Token 的 Redis key: rt:{userID}:{deviceId}
func buildRTKey(userID uint64, deviceID string) string {
	return fmt.Sprintf("%s%d:%s", rtKeyPrefix, userID, deviceID)
}

// buildSessionKey 构建会话元数据的 Redis key: session:{userID}
func buildSessionKey(userID uint64) string {
	return fmt.Sprintf("%s%d", sessionKeyPrefix, userID)
}

// ---- 工具函数 ----

// ExtractToken 从 HTTP 请求中提取 Bearer token，
// 优先从 Authorization header，其次从查询参数 token。
func ExtractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		if parts := strings.SplitN(auth, " ", 2); len(parts) == 2 && parts[0] == "Bearer" {
			return parts[1]
		}
	}
	return r.URL.Query().Get("token")
}

// ExtractIDFromCtx 从 context 中提取用户 ID。
func ExtractIDFromCtx(ctx context.Context) uint64 {
	if id, ok := ctx.Value(ContextKeyUserID).(uint64); ok {
		return id
	}
	return 0
}
