package redisx

import (
	"context"
	"fmt"
)

// StringManager 封装 Redis String/KV 操作，含 Bitmap。
//
// 所有方法内部自动调用 buildKey() 处理 key 前缀，业务层无需手动拼接。
// StringManager 以匿名嵌入方式组合到 Client，其方法直接在 Client 上可用。
type StringManager struct {
	client *Client
}

// GetCtx 获取 key 对应的值。若 key 不存在，返回 ("", nil)。
func (s *StringManager) GetCtx(ctx context.Context, key string) (string, error) {
	return s.client.rdb.GetCtx(ctx, s.client.buildKey(key))
}

// SetexCtx 设置 key 的值并指定过期时间（秒）。
func (s *StringManager) SetexCtx(ctx context.Context, key, val string, seconds int) error {
	return s.client.rdb.SetexCtx(ctx, s.client.buildKey(key), val, seconds)
}

// DelCtx 删除一个或多个 key，返回实际删除的数量。
func (s *StringManager) DelCtx(ctx context.Context, keys ...string) (int, error) {
	builtKeys := make([]string, len(keys))
	for i, k := range keys {
		builtKeys[i] = s.client.buildKey(k)
	}
	return s.client.rdb.Del(builtKeys...)
}

// ExpireCtx 为 key 设置过期时间（秒）。
func (s *StringManager) ExpireCtx(ctx context.Context, key string, seconds int) error {
	return s.client.rdb.Expire(s.client.buildKey(key), seconds)
}

// MgetCtx 批量获取多个 key 的值，结果顺序与 keys 入参一致，不存在的 key 对应空字符串。
func (s *StringManager) MgetCtx(ctx context.Context, keys ...string) ([]string, error) {
	builtKeys := make([]string, len(keys))
	for i, k := range keys {
		builtKeys[i] = s.client.buildKey(k)
	}
	return s.client.rdb.MgetCtx(ctx, builtKeys...)
}

// SetnxExCtx 原子 SET-if-Not-eXists，并指定过期时间（秒）。
// 返回 true 表示写入成功（key 之前不存在），false 表示 key 已存在（本次未写入）。
func (s *StringManager) SetnxExCtx(ctx context.Context, key, val string, seconds int) (bool, error) {
	return s.client.rdb.SetnxExCtx(ctx, s.client.buildKey(key), val, seconds)
}

// ---------- Bitmap ----------

// SetBitCtx 将 key 在 offset 处的 bit 设为 val（0 或 1），返回修改前的旧值。
func (s *StringManager) SetBitCtx(ctx context.Context, key string, offset int64, val int) (int, error) {
	v, err := s.client.rdb.SetBit(s.client.buildKey(key), offset, val)
	if err != nil {
		return 0, fmt.Errorf("redisx: setbit %s offset=%d: %w", key, offset, err)
	}
	return v, nil
}

// GetBitCtx 获取 key 在 offset 处的 bit 值（0 或 1）。
func (s *StringManager) GetBitCtx(ctx context.Context, key string, offset int64) (int64, error) {
	v, err := s.client.rdb.GetBit(s.client.buildKey(key), offset)
	if err != nil {
		return 0, fmt.Errorf("redisx: getbit %s offset=%d: %w", key, offset, err)
	}
	return int64(v), nil
}
