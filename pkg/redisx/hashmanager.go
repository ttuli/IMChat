package redisx

import (
	"context"
	"fmt"
)

// HashManager 封装 Redis Hash 操作。
//
// 方法名统一以 H 开头，与 String 操作区分，避免嵌入 Client 时产生方法名冲突。
// HashManager 以匿名嵌入方式组合到 Client，其方法直接在 Client 上可用。
type HashManager struct {
	client *Client
}

// HGetCtx 获取 Hash 中指定 field 的值。若 key 或 field 不存在，返回 ("", nil)。
func (h *HashManager) HGetCtx(ctx context.Context, key, field string) (string, error) {
	v, err := h.client.rdb.HgetCtx(ctx, h.client.buildKey(key), field)
	if err != nil {
		return "", fmt.Errorf("redisx: hget %s field=%s: %w", key, field, err)
	}
	return v, nil
}

// HMSetCtx 批量设置 Hash 中的多个 field-value 对。
func (h *HashManager) HMSetCtx(ctx context.Context, key string, fieldsAndValues map[string]string) error {
	if err := h.client.rdb.HmsetCtx(ctx, h.client.buildKey(key), fieldsAndValues); err != nil {
		return fmt.Errorf("redisx: hmset %s: %w", key, err)
	}
	return nil
}

// HDelCtx 删除 Hash 中的一个或多个 field。
func (h *HashManager) HDelCtx(ctx context.Context, key string, fields ...string) error {
	_, err := h.client.rdb.HdelCtx(ctx, h.client.buildKey(key), fields...)
	if err != nil {
		return fmt.Errorf("redisx: hdel %s fields=%v: %w", key, fields, err)
	}
	return nil
}

// HIncrByCtx 原子地将 Hash 中 field 对应的整数值增加 increment，返回增加后的值。
func (h *HashManager) HIncrByCtx(ctx context.Context, key, field string, increment int64) (int64, error) {
	v, err := h.client.rdb.HincrbyCtx(ctx, h.client.buildKey(key), field, int(increment))
	if err != nil {
		return 0, fmt.Errorf("redisx: hincrby %s field=%s: %w", key, field, err)
	}
	return int64(v), nil
}
