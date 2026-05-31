package tokenmanager

import (
	"IM2/pkg/xerr"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type TokenType int

const (
	AccessToken TokenType = iota
	RefreshToken
)

type TokenConfig struct {
	RedisConf redis.RedisConf

	JWTConfig struct {
		Secret        string `json:"secret"`
		Expire        int64  `json:"expire"`
		RefreshExpire int64  `json:"refreshExpire"`
	}
}

type RefreshTokenConfig struct {
	Platform string `json:"platform"`
	DeviceId string `json:"device_id"`
}

type TokenManager struct {
	c TokenConfig
	*redis.Redis
}

const (
	ContextKeyUserID = "user_id"

	// Token key 前缀格式: token:{userID}:{deviceId}:{tokenType}
	// 例如: token:1000001:deviceA:at, token:1000001:deviceA:rt
	TokenKeyPrefix = "token:"
	TokenTypeAT    = "at" // Access Token
	TokenTypeRT    = "rt" // Refresh Token

	// 在线设备列表 key 前缀: user_active_devices:{userID}
	UserDevicesKeyPrefix = "user_active_devices:"

	// Claim Keys
	ClaimKeyExp      = "exp"
	ClaimKeyIat      = "iat"
	ClaimKeyType     = "token_type"
	ClaimKeyPlatform = "platform"
	ClaimKeyDeviceID = "device_id"

	// Token Types (用于 JWT claims)
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"
)

// BuildTokenKey 构建 token 的 Redis key
// 格式: token:{userID}:{deviceId}:{tokenType}
func BuildTokenKey(userID uint64, deviceId string, tokenType TokenType) string {
	typeStr := TokenTypeAT
	if tokenType == RefreshToken {
		typeStr = TokenTypeRT
	}
	return fmt.Sprintf("%s%d:%s:%s", TokenKeyPrefix, userID, deviceId, typeStr)
}

// BuildUserDevicesKey 构建用户在线设备列表的 Redis key
func BuildUserDevicesKey(userID uint64) string {
	return fmt.Sprintf("%s%d", UserDevicesKeyPrefix, userID)
}

// extractToken 从请求中提取 token
// 优先从 Authorization header 提取，其次从查询参数提取
func ExtractToken(r *http.Request) string {
	// 从 Authorization header 提取
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && parts[0] == "Bearer" {
			return parts[1]
		}
	}

	// 从查询参数提取 (用于 WebSocket 等场景)
	return r.URL.Query().Get("token")
}

func ExtractIDFromCtx(ctx context.Context) uint64 {
	if id, ok := ctx.Value(ContextKeyUserID).(uint64); ok {
		return id
	}
	return 0
}

func NewTokenManager(c TokenConfig) *TokenManager {
	return &TokenManager{
		c:     c,
		Redis: redis.MustNewRedis(c.RedisConf),
	}
}

func (t *TokenManager) GenerateJWTToken(userID uint64, tokenType TokenType, claims jwt.MapClaims) (string, error) {
	now := time.Now()
	if claims == nil {
		claims = make(jwt.MapClaims)
	}

	var expireSeconds int64
	switch tokenType {
	case AccessToken:
		expireSeconds = t.c.JWTConfig.Expire
		claims[ClaimKeyType] = TokenTypeAccess
	case RefreshToken:
		expireSeconds = t.c.JWTConfig.RefreshExpire
		claims[ClaimKeyType] = TokenTypeRefresh
	default:
		return "", fmt.Errorf("unsupported token type: %d", tokenType)
	}

	claims[ContextKeyUserID] = strconv.FormatUint(userID, 10)
	claims[ClaimKeyExp] = now.Add(time.Duration(expireSeconds) * time.Second).Unix()
	claims[ClaimKeyIat] = now.Unix()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(t.c.JWTConfig.Secret))
	if err != nil {
		return "", err
	}

	deviceId := ""
	if d, ok := claims[ClaimKeyDeviceID].(string); ok {
		deviceId = d
	}
	if err := t.storeToken(userID, deviceId, tokenType, tokenString, expireSeconds); err != nil {
		return "", err
	}

	// 仅在生成 AccessToken 时触发设备管理逻辑，避免重复踢出
	if tokenType == AccessToken {
		devicesKey := BuildUserDevicesKey(userID)
		// 原子地将当前设备加入集合并设置过期时间，避免 SADD 成功后 crash 导致僵尸 key
		saddWithExpireScript := `
		local added = redis.call('SADD', KEYS[1], ARGV[1])
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return added`
		_, _ = t.EvalCtx(context.Background(), saddWithExpireScript,
			[]string{devicesKey},
			deviceId, strconv.FormatInt(t.c.JWTConfig.RefreshExpire, 10),
		)
		// 踢出该用户所有其他设备的 Token
		_ = t.KickOutOtherDevices(userID, deviceId)
	}

	return tokenString, nil
}

// storeToken 存储 token 到 Redis
// Key 格式: token:{userID}:{deviceId}:{at|rt}
// Value: token 字符串
func (t *TokenManager) storeToken(userID uint64, deviceId string, tokenType TokenType, tokenString string, expire int64) error {
	key := BuildTokenKey(userID, deviceId, tokenType)
	return t.Setex(key, tokenString, int(expire))
}

// KickOutOtherDevices 踢出该用户除 currentDevice 以外的所有其他设备的 Token
func (t *TokenManager) KickOutOtherDevices(userID uint64, currentDevice string) error {
	devicesKey := BuildUserDevicesKey(userID)

	// 获取所有在线设备
	devices, err := t.Smembers(devicesKey)
	if err != nil {
		return err
	}

	// 遍历旧设备，删除其 Token 并从集合中移除
	for _, dev := range devices {
		if dev != currentDevice && dev != "" {
			// 删除旧设备的 AT 和 RT
			_, _ = t.Del(BuildTokenKey(userID, dev, AccessToken))
			_, _ = t.Del(BuildTokenKey(userID, dev, RefreshToken))
			// 从在线设备集合中移除
			_, _ = t.Srem(devicesKey, dev)
		}
	}
	return nil
}

// InvalidateTokenByUserID 根据用户 ID 和 deviceId 使 token 失效
// deleteRefreshToken: 是否同时删除 refresh token
func (t *TokenManager) InvalidateTokenByUserID(userID uint64, deviceId string, deleteRefreshToken bool) error {
	keys := []string{BuildTokenKey(userID, deviceId, AccessToken)}
	if deleteRefreshToken {
		keys = append(keys, BuildTokenKey(userID, deviceId, RefreshToken))
	}

	// 同时从在线设备集合中移除
	_, _ = t.Srem(BuildUserDevicesKey(userID), deviceId)

	_, err := t.Del(keys...)
	return err
}

// InvalidateToken 使指定的 token 失效（兼容旧接口）
// 会解析 token 获取 userID，然后删除对应的 key
func (t *TokenManager) InvalidateToken(tokenString string) error {
	userID, platform, tokenType, err := t.ParseTokenInfo(tokenString)
	if err != nil {
		return fmt.Errorf("parse token failed: %w", err)
	}

	key := BuildTokenKey(userID, platform, tokenType)
	_, err = t.Del(key)
	return err
}

// ParseTokenInfo 从 token 中解析 userID, platform 和 tokenType
func (t *TokenManager) ParseTokenInfo(tokenString string) (uint64, string, TokenType, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(t.c.JWTConfig.Secret), nil
	})

	if err != nil {
		return 0, "", 0, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, "", 0, fmt.Errorf("invalid claims type")
	}

	// 解析 userID
	userIDStr, ok := claims[ContextKeyUserID].(string)
	if !ok {
		return 0, "", 0, fmt.Errorf("user_id not found in claims")
	}
	userID, err := strconv.ParseUint(userIDStr, 10, 64)
	if err != nil {
		return 0, "", 0, fmt.Errorf("invalid user_id format: %w", err)
	}

	// 解析 tokenType
	tokenTypeStr, ok := claims[ClaimKeyType].(string)
	if !ok {
		return 0, "", 0, fmt.Errorf("token_type not found in claims")
	}

	// 解析 platform
	platform, _ := claims[ClaimKeyPlatform].(string)
	if platform == "" {
		platform = "unknown"
	}

	var tokenType TokenType
	switch tokenTypeStr {
	case TokenTypeAccess:
		tokenType = AccessToken
	case TokenTypeRefresh:
		tokenType = RefreshToken
	default:
		return 0, "", 0, fmt.Errorf("unknown token type: %s", tokenTypeStr)
	}

	return userID, platform, tokenType, nil
}

func (t *TokenManager) ParseUserIDFromToken(tokenString string) (uint64, error) {
	userID, _, _, err := t.ParseTokenInfo(tokenString)
	return userID, err
}

// ValidateToken 验证 token 并返回 userID
// 实现 middleware.TokenValidator 接口
func (t *TokenManager) ValidateToken(ctx context.Context, tokenString string) (uint64, error) {
	// 1. 解析 JWT 并验证签名
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(t.c.JWTConfig.Secret), nil
	}, jwt.WithoutClaimsValidation()) // 跳过库的自动 iat/nbf/exp 校验，由下方手动校验（兼容服务间时钟偏差）
	if err != nil {
		return 0, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, fmt.Errorf("invalid claims type")
	}

	// 2. 验证过期时间（允许 5s 时钟偏差，兼容本地开发 mirrord 场景）
	exp, ok := claims["exp"].(float64)
	if !ok {
		return 0, fmt.Errorf("exp claim not found or invalid")
	}
	const clockSkew = 5 // 秒，容忍服务间时钟偏差
	if time.Now().Unix() > int64(exp)+clockSkew {
		return 0, fmt.Errorf("token has expired")
	}

	// 3. 提取 userID
	userIDStr, ok := claims[ContextKeyUserID].(string)
	if !ok {
		return 0, fmt.Errorf("user_id not found in claims")
	}
	userID, err := strconv.ParseUint(userIDStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user_id format: %w", err)
	}

	// 4. 解析 token 类型
	tokenTypeStr, ok := claims[ClaimKeyType].(string)
	if !ok {
		return 0, fmt.Errorf("token_type not found in claims")
	}

	var tokenType TokenType
	switch tokenTypeStr {
	case TokenTypeAccess:
		tokenType = AccessToken
	case TokenTypeRefresh:
		tokenType = RefreshToken
	default:
		return 0, fmt.Errorf("unknown token type: %s", tokenTypeStr)
	}

	// 4.5 解析 platform
	deviceID, _ := claims[ClaimKeyDeviceID].(string)

	// 5. 从 Redis 验证 token 是否存在且匹配
	tokenKey := BuildTokenKey(userID, deviceID, tokenType)
	storedToken, err := t.GetCtx(ctx, tokenKey)
	if err != nil {
		return 0, fmt.Errorf("redis error: %w", err)
	}
	if storedToken == "" || storedToken != tokenString {
		// token 未过期但不在 Redis 中（或不匹配），说明该设备已被踢出或顶号
		return 0, xerr.New(xerr.ErrKickedOut, "账号已在其他设备登录，您已被强制下线")
	}

	return userID, nil
}
