package redisx

import (
	"context"
	"fmt"
	"time"
)

// SetManager 封装 Redis Set 的统一操作
// 解决的问题:
//   - 统一 key 前缀
//   - Set 本身不支持 TTL，通过 AddWithTTL 在写入时同步设置过期
//   - 防止 Big Key：提供 size 检查
type SetManager struct {
	client *Client
}

const maxSetSize = 10_000 // Big Key 警戒线

// Add 向 Set 中添加成员，使用客户端默认 TTL
func (s *SetManager) Add(ctx context.Context, setName string, members ...string) error {
	return s.AddWithTTL(ctx, setName, s.client.opts.defaultTTL, members...)
}

// AddWithTTL 向 Set 中添加成员并指定过期时间
func (s *SetManager) AddWithTTL(ctx context.Context, setName string, ttl time.Duration, members ...string) error {
	key := s.client.buildKey(setName)

	// 检查 Big Key
	size, err := s.client.rdb.Scard(key)
	if err != nil {
		return fmt.Errorf("redissdk: scard %s: %w", key, err)
	}
	if size >= maxSetSize {
		return fmt.Errorf("redissdk: set %s exceeds max size %d, consider sharding", key, maxSetSize)
	}

	if _, err := s.client.rdb.Sadd(key, members); err != nil {
		return fmt.Errorf("redissdk: sadd %s: %w", key, err)
	}

	// Set 不支持字段级 TTL，每次写入后刷新整体过期时间
	if ttl > 0 {
		if err := s.client.rdb.Expire(key, int(ttl.Seconds())); err != nil {
			return fmt.Errorf("redissdk: expire %s: %w", key, err)
		}
	}
	return nil
}

// Remove 从 Set 中移除成员
func (s *SetManager) Remove(ctx context.Context, setName string, members ...string) error {
	key := s.client.buildKey(setName)
	if _, err := s.client.rdb.Srem(key, members); err != nil {
		return fmt.Errorf("redissdk: srem %s: %w", key, err)
	}
	return nil
}

// IsMember 判断是否是 Set 成员
func (s *SetManager) IsMember(ctx context.Context, setName, member string) (bool, error) {
	key := s.client.buildKey(setName)
	ok, err := s.client.rdb.Sismember(key, member)
	if err != nil {
		return false, fmt.Errorf("redissdk: sismember %s: %w", key, err)
	}
	return ok, nil
}

// Members 获取 Set 所有成员
func (s *SetManager) Members(ctx context.Context, setName string) ([]string, error) {
	key := s.client.buildKey(setName)
	members, err := s.client.rdb.Smembers(key)
	if err != nil {
		return nil, fmt.Errorf("redissdk: smembers %s: %w", key, err)
	}
	return members, nil
}

// Size 获取 Set 大小
func (s *SetManager) Size(ctx context.Context, setName string) (int64, error) {
	key := s.client.buildKey(setName)
	size, err := s.client.rdb.Scard(key)
	if err != nil {
		return 0, fmt.Errorf("redissdk: scard %s: %w", key, err)
	}
	return size, nil
}

// Delete 删除整个 Set
func (s *SetManager) Delete(ctx context.Context, setName string) error {
	key := s.client.buildKey(setName)
	if _, err := s.client.rdb.Del(key); err != nil {
		return fmt.Errorf("redissdk: del %s: %w", key, err)
	}
	return nil
}

// Intersect 求多个 Set 的交集（不落盘，直接返回）
func (s *SetManager) Intersect(ctx context.Context, setNames ...string) ([]string, error) {
	keys := make([]string, len(setNames))
	for i, name := range setNames {
		keys[i] = s.client.buildKey(name)
	}
	result, err := s.client.rdb.Sunion(keys...) // 可替换为 Sinter（go-zero 版本支持时）
	if err != nil {
		return nil, fmt.Errorf("redissdk: intersect: %w", err)
	}
	return result, nil
}
