package tokenmanager

import (
	"context"
	"fmt"
)

// InvalidateTokenByUserID 使指定用户设备的 token 失效。
// 若 deleteRefreshToken=true，同时删除 refresh token。
func (t *TokenManager) InvalidateTokenByUserID(ctx context.Context, userID uint64, deviceID string, deleteRefreshToken bool) error {
	keys := []string{buildATKey(userID, deviceID)}
	if deleteRefreshToken {
		keys = append(keys, buildRTKey(userID, deviceID))
	}
	_, err := t.Del(keys...)
	return err
}

// InvalidateToken 从 token 字符串解析信息后使其失效（兼容旧接口）。
func (t *TokenManager) InvalidateToken(tokenString string) error {
	userID, deviceID, tokenType, err := t.ParseTokenInfo(tokenString)
	if err != nil {
		return fmt.Errorf("parse token failed: %w", err)
	}

	var key string
	switch tokenType {
	case AccessToken:
		key = buildATKey(userID, deviceID)
	case RefreshToken:
		key = buildRTKey(userID, deviceID)
	}
	_, err = t.Del(key)
	return err
}

// KickOutOtherDevices 删除该用户除 currentDeviceID 以外所有设备的 AT/RT。
// 依赖：at:{userID}:* 和 rt:{userID}:* 的 SCAN 匹配。
func (t *TokenManager) KickOutOtherDevices(ctx context.Context, userID uint64, currentDeviceID string) error {
	// 扫描该用户的所有 at key，找出其他设备并删除
	pattern := fmt.Sprintf("%s%d:*", atKeyPrefix, userID)
	keys, err := t.scanKeys(ctx, pattern)
	if err != nil {
		return err
	}

	for _, key := range keys {
		deviceID := extractDeviceFromATKey(userID, key)
		if deviceID == "" || deviceID == currentDeviceID {
			continue
		}
		_, _ = t.Del(buildATKey(userID, deviceID), buildRTKey(userID, deviceID))
	}
	return nil
}

// DeleteSession 删除 session:{userID}（登出全设备时使用）。
func (t *TokenManager) DeleteSession(ctx context.Context, userID uint64) error {
	_, err := t.Del(buildSessionKey(userID))
	return err
}

// ---- 内部工具 ----

// scanKeys 使用 SCAN 扫描匹配 pattern 的所有 key。
func (t *TokenManager) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64

	scanScript := `
local cursor = tonumber(ARGV[1])
local pattern = ARGV[2]
local result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100)
return result`

	for {
		res, err := t.EvalCtx(ctx, scanScript, []string{}, fmt.Sprintf("%d", cursor), pattern)
		if err != nil {
			return nil, fmt.Errorf("scan keys: %w", err)
		}

		arr, ok := res.([]interface{})
		if !ok || len(arr) != 2 {
			break
		}

		// 更新 cursor
		switch v := arr[0].(type) {
		case []byte:
			fmt.Sscanf(string(v), "%d", &cursor)
		case string:
			fmt.Sscanf(v, "%d", &cursor)
		case int64:
			cursor = uint64(v)
		}

		// 收集 keys
		if batch, ok := arr[1].([]interface{}); ok {
			for _, k := range batch {
				switch kv := k.(type) {
				case []byte:
					keys = append(keys, string(kv))
				case string:
					keys = append(keys, kv)
				}
			}
		}

		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

// extractDeviceFromATKey 从 at:{userID}:{deviceID} 中提取 deviceID。
func extractDeviceFromATKey(userID uint64, key string) string {
	prefix := fmt.Sprintf("%s%d:", atKeyPrefix, userID)
	if len(key) <= len(prefix) {
		return ""
	}
	return key[len(prefix):]
}
