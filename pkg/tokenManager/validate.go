package tokenmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"

	"github.com/golang-jwt/jwt/v4"
)

// SessionData 从 Redis session:{userID} 中读取的会话元数据。
type SessionData struct {
	MachineID string `json:"machine_id"`
	Version   uint64 `json:"version"` // 无符号递增，登录时写入 AT ver claim
}

// ValidateToken 验证 access token 并返回 userID。
// 实现 middleware.TokenValidator 接口。
func (t *TokenManager) ValidateToken(ctx context.Context, tokenString string) (uint64, error) {
	// 1. 解析并验证签名（跳过库自动 exp 校验，由下方手动校验）
	token, err := jwt.Parse(tokenString, t.keyFunc(), jwt.WithoutClaimsValidation())
	if err != nil {
		return 0, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, fmt.Errorf("invalid claims type")
	}

	// 2. 手动校验过期时间（容忍 5s 时钟偏差）
	exp, ok := claims[ClaimKeyExp].(float64)
	if !ok {
		return 0, fmt.Errorf("exp claim not found or invalid")
	}
	const clockSkew = 5
	if time.Now().Unix() > int64(exp)+clockSkew {
		return 0, fmt.Errorf("token has expired")
	}

	// 3. 提取 userID
	userID, err := extractUserID(claims)
	if err != nil {
		return 0, err
	}

	return userID, nil
}

// ParseTokenInfo 解析 token 中的 userID、deviceID 和 tokenType（不校验过期）。
func (t *TokenManager) ParseTokenInfo(tokenString string) (userID uint64, deviceID string, tokenType TokenType, err error) {
	token, err := jwt.Parse(tokenString, t.keyFunc())
	if err != nil {
		return 0, "", 0, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, "", 0, fmt.Errorf("invalid claims type")
	}

	userID, err = extractUserID(claims)
	if err != nil {
		return 0, "", 0, err
	}

	tokenType, err = extractTokenType(claims)
	if err != nil {
		return 0, "", 0, err
	}

	deviceID, _ = claims[ClaimKeyDeviceID].(string)
	return userID, deviceID, tokenType, nil
}

// ParseUserIDFromToken 快速从 token 中提取 userID。
func (t *TokenManager) ParseUserIDFromToken(tokenString string) (uint64, error) {
	userID, _, _, err := t.ParseTokenInfo(tokenString)
	return userID, err
}

// GetSession 从 Redis 读取 session:{userID} 中的会话元数据。
// session 不存在时返回 401（可能已过期或从未登录），客户端应触发刷新 token。
func (t *TokenManager) GetSession(ctx context.Context, userID uint64) (*SessionData, error) {
	raw, err := t.GetCtx(ctx, buildSessionKey(userID))
	if err != nil {
		return nil, fmt.Errorf("redis get session: %w", err)
	}
	if raw == "" {
		// session 过期或被删除，返回 401，客户端触发刷新 token
		return nil, xerr.New(transport.ErrorCode_ERR_UNAUTHORIZED, "token 已过期，请重新登录")
	}
	var s SessionData
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}
	return &s, nil
}

// HasRefreshToken 检查 Redis 中是否存在 rt:{userID}:{deviceID} 键。
// 用于 Refresh 流程中动态判断是否为 remember_me 登录路径：
//   - 存在 → 登录时勾选了 remember_me，刷新时需同步更新 RT 键（RememberMe=true）
//   - 不存在 → 普通登录，刷新时不写 RT 键（RememberMe=false）
func (t *TokenManager) HasRefreshToken(ctx context.Context, userID uint64, deviceID string) (bool, error) {
	val, err := t.GetCtx(ctx, buildRTKey(userID, deviceID))
	if err != nil {
		return false, fmt.Errorf("redis check rt key: %w", err)
	}
	return val != "", nil
}

// CheckWSSession 在 WS 建连时校验 JWT 中的 deviceID 和 ver claim 是否与 Redis session 一致。
//
// 校验逻辑：
//   - session 不存在（TTL 过期）→ 返回 401，客户端刷新 token 后重建 WS。
//   - ver 不一致：说明 session 已被新登录覆盖 → 返回被踢出错误，WS 层应关闭旧连接。
//   - deviceID 不一致：token 作弊或设备异常 → 不允许建连。
//
// 前置条件：AT 自身的 exp 已在 HTTP 层校验通过，此处不重复验证。
func (t *TokenManager) CheckWSSession(ctx context.Context, tokenString string) error {
	// 1. 解析 JWT，不重复验证签名（由调用方保证已验证）
	token, err := jwt.Parse(tokenString, t.keyFunc(), jwt.WithoutClaimsValidation())
	if err != nil {
		return fmt.Errorf("parse token: %w", err)
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return fmt.Errorf("invalid claims")
	}

	userID, err := extractUserID(claims)
	if err != nil {
		return err
	}

	// 从 JWT 提取 deviceID
	tokenDeviceID, _ := claims[ClaimKeyDeviceID].(string)

	// 从 JWT 提取 ver（JSON 数字解析为 float64）
	var tokenVer uint64
	if v, ok := claims[ClaimKeyVersion].(float64); ok {
		tokenVer = uint64(v)
	}

	// ver=0 表示老路径生成，跳过校验
	if tokenVer == 0 {
		return nil
	}

	// 2. 读取 Redis session（不存在 → 401）
	session, err := t.GetSession(ctx, userID)
	if err != nil {
		return err
	}

	// 3. 校验 version：不一致说明 session 已被新登录覆盖
	if tokenVer != session.Version {
		return xerr.New(transport.ErrorCode_ERR_KICKED_OUT, "账号已在其他设备登录，您已被强制下线")
	}

	// 4. 校验 deviceID：防止 token 作弊或设备异常（同一用户不同设备的 token 混用）
	// 注意：deviceID 校验失败不代表被踢出，而是 token 本身非法
	if tokenDeviceID == "" {
		return fmt.Errorf("device_id missing in token")
	}
	// session 中不存储 deviceID，此处仅确保 token 自身的 deviceID 非空。
	// 如需进一步设备级别隔离，可在 session 中存储 deviceID 列表。

	return nil
}

// CheckSessionVersion 已废弃，保留为兼容。请使用 CheckWSSession。
//
// Deprecated: 使用 CheckWSSession。
func (t *TokenManager) CheckSessionVersion(ctx context.Context, tokenString string) error {
	return t.CheckWSSession(ctx, tokenString)
}

// ---- 内部工具 ----

// keyFunc 返回 JWT 验签函数。
func (t *TokenManager) keyFunc() jwt.Keyfunc {
	return func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(t.c.JWTConfig.Secret), nil
	}
}

// verifyTokenInRedis 校验 Redis 中存储的 token 是否与请求的匹配。
func (t *TokenManager) verifyTokenInRedis(ctx context.Context, userID uint64, deviceID string, tokenType TokenType, tokenString string) error {
	var key string
	switch tokenType {
	case AccessToken:
		key = buildATKey(userID, deviceID)
	case RefreshToken:
		key = buildRTKey(userID, deviceID)
	}

	stored, err := t.GetCtx(ctx, key)
	if err != nil {
		return fmt.Errorf("redis error: %w", err)
	}
	if stored == "" || stored != tokenString {
		return xerr.New(transport.ErrorCode_ERR_KICKED_OUT, "账号已在其他设备登录，您已被强制下线")
	}
	return nil
}

func extractUserID(claims jwt.MapClaims) (uint64, error) {
	s, ok := claims[ContextKeyUserID].(string)
	if !ok {
		return 0, fmt.Errorf("user_id not found in claims")
	}
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user_id format: %w", err)
	}
	return id, nil
}

func extractTokenType(claims jwt.MapClaims) (TokenType, error) {
	s, ok := claims[ClaimKeyType].(string)
	if !ok {
		return 0, fmt.Errorf("token_type not found in claims")
	}
	switch s {
	case TokenTypeAccess:
		return AccessToken, nil
	case TokenTypeRefresh:
		return RefreshToken, nil
	default:
		return 0, fmt.Errorf("unknown token type: %s", s)
	}
}
