package tokenmanager

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// LoginOptions 登录时传入的额外选项。
type LoginOptions struct {
	// MachineID 为登录机器标识，写入 session:{userID}
	MachineID string
	// RememberMe 为 true 时，额外在 Redis 中持久化 refresh token
	RememberMe bool
}

// OnLogin 在用户登录成功后调用，负责：
//  1. 创建或覆盖 session:{userID}（TTL=Expire），获取本次 session version（无符号递增）
//  2. 生成带 ver claim 的 Access Token
//  3. 若 rememberMe=true，设置 rt:{userID}:{deviceID}（TTL=RefreshExpire）
//
// 注意：此处不主动踢出旧连接。踢出逻辑发生在 WS 建连时：
// 发现已有连接且 deviceID/version 不匹配时，由 WS 层关闭旧连接。
//
// 返回 (accessToken, refreshToken, error)。
func (t *TokenManager) OnLogin(ctx context.Context, userID uint64, opts LoginOptions) (accessToken, refreshToken string, err error) {
	deviceID := opts.MachineID

	// 1. 写会话元数据，拿到本次 version
	version, err := t.setSession(ctx, userID, opts.MachineID)
	if err != nil {
		return "", "", fmt.Errorf("set session: %w", err)
	}

	// 2. 生成 AT，携带 ver claim（不存 Redis，依赖 session TTL 失效 → 401）
	accessToken, err = t.GenerateAccessToken(ctx, userID, deviceID, version, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate access token: %w", err)
	}

	// 3. 生成 RT（仅签名）
	refreshToken, err = t.GenerateRefreshToken(userID, deviceID, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate refresh token: %w", err)
	}

	// 4. remember_me 时持久化 RT
	if opts.RememberMe && deviceID != "" {
		if err = t.storeRefreshToken(ctx, userID, deviceID, refreshToken); err != nil {
			return "", "", fmt.Errorf("store refresh token: %w", err)
		}
	}
	return accessToken, refreshToken, nil
}

// setSession 创建或覆盖 session:{userID}，TTL 与 AT 相同（Expire 秒）。
// 若已存在：覆盖 machine_id 并将 version 无符号递增；首次登录则 version=1。
// 使用 Lua 脚本保证原子性，返回最终写入的 version（uint64）。
//
// TTL 与 AT 保持一致：session 过期 → WS 校验失败 → 客户端收到 401 → 触发刷新 token。
// 新登录覆盖旧 session → 旧 AT 携带的 version 与新 session 不一致 → WS 建连时踢出旧连接。
func (t *TokenManager) setSession(ctx context.Context, userID uint64, machineID string) (uint64, error) {
	key := buildSessionKey(userID)
	ttl := t.c.JWTConfig.Expire // 与 AT 相同 TTL

	// Lua 保证原子读改写：version 用无符号递增，溢出归 1（实际不会发生）。
	// SETEX 使 session 与 AT 同时到期，避免无 TTL 的 session 残留。
	script := `
local raw = redis.call('GET', KEYS[1])
local version = 1
if raw then
    local v = cjson.decode(raw)
    if v and v.version then
        version = (v.version + 1) % 18446744073709551616
        if version == 0 then version = 1 end
    end
end
local newVal = cjson.encode({machine_id = ARGV[1], version = version})
redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), newVal)
return version`

	res, err := t.EvalCtx(ctx, script, []string{key}, machineID, fmt.Sprintf("%d", ttl))
	if err != nil {
		return 0, err
	}

	// EvalCtx 返回 interface{}，Redis 整数脚本返回 int64
	switch v := res.(type) {
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	default:
		return 1, nil // fallback
	}
}

// storeRefreshToken 将 refresh token 写入 rt:{userID}:{deviceID}，
// TTL = RefreshExpire（秒）。
func (t *TokenManager) storeRefreshToken(ctx context.Context, userID uint64, deviceID string, refreshToken string) error {
	key := buildRTKey(userID, deviceID)
	return t.SetexCtx(ctx, key, refreshToken, int(t.c.JWTConfig.RefreshExpire))
}

// GenerateAccessToken 生成带 ver claim 的 Access Token（不存 Redis）。
// version（uint64）来自本次 setSession 的返回值，写入 JWT "ver" claim。
//
// 设计说明：
//   - AT 不存 Redis，依赖 JWT 自身的 exp + session TTL 双重保障。
//   - session 被覆盖（新登录）→ WS 建连时 version 不一致 → 踢出旧连接。
//   - session TTL 过期 → WS/HTTP 校验失败 → 返回 401 → 客户端触发刷新。
//   - 踢出旧 WS 连接的职责交由 WS 层在建连时处理，此处不再调用 KickOutOtherDevices。
func (t *TokenManager) GenerateAccessToken(ctx context.Context, userID uint64, deviceID string, version uint64, extra jwt.MapClaims) (string, error) {
	if extra == nil {
		extra = make(jwt.MapClaims)
	}
	extra[ClaimKeyVersion] = version

	return t.signJWT(userID, AccessToken, deviceID, extra)
}

// GenerateRefreshToken 生成 Refresh Token（仅签名，不存 Redis）。
// 调用方根据 RememberMe 决定是否通过 OnLogin 持久化。
func (t *TokenManager) GenerateRefreshToken(userID uint64, deviceID string, extra jwt.MapClaims) (string, error) {
	return t.signJWT(userID, RefreshToken, deviceID, extra)
}

// signJWT 构建并签名 JWT，返回 token 字符串。
func (t *TokenManager) signJWT(userID uint64, tokenType TokenType, deviceID string, extra jwt.MapClaims) (string, error) {
	now := time.Now()

	claims := make(jwt.MapClaims, len(extra)+5)
	for k, v := range extra {
		claims[k] = v
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
	claims[ClaimKeyDeviceID] = deviceID
	claims[ClaimKeyExp] = now.Add(time.Duration(expireSeconds) * time.Second).Unix()
	claims[ClaimKeyIat] = now.Unix()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(t.c.JWTConfig.Secret))
}
