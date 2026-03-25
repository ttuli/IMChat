package redisx

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ZSetManager 封装 Redis Sorted Set（有序集合）操作。
//
// 所有方法内部自动调用 buildKey() 处理 key 前缀。
// ZSetManager 以匿名嵌入方式组合到 Client，其方法直接在 Client 上可用。
type ZSetManager struct {
	client *Client
}

// ZAddCtx 向有序集合中添加一个元素，分值为 score。若成员已存在则更新 score。
func (z *ZSetManager) ZAddCtx(ctx context.Context, key string, score float64, member string) error {
	if _, err := z.client.rdb.ZaddCtx(ctx, z.client.buildKey(key), int64(score), member); err != nil {
		return fmt.Errorf("redisx: zadd %s member=%s: %w", key, member, err)
	}
	return nil
}

// ZAddBatchCtx 向有序集合中批量添加元素。
func (z *ZSetManager) ZAddBatchCtx(ctx context.Context, key string, pairs []redis.Pair) error {
	builtKey := z.client.buildKey(key)
	for _, p := range pairs {
		if _, err := z.client.rdb.ZaddCtx(ctx, builtKey, int64(p.Score), p.Key); err != nil {
			return fmt.Errorf("redisx: zadd batch %s member=%s: %w", key, p.Key, err)
		}
	}
	return nil
}

// ZRangeByScoreWithScoresCtx 按分值范围升序获取成员及其分值。
func (z *ZSetManager) ZRangeByScoreWithScoresCtx(ctx context.Context, key string, start, stop int64) ([]redis.Pair, error) {
	pairs, err := z.client.rdb.ZrangebyscoreWithScoresCtx(ctx, z.client.buildKey(key), start, stop)
	if err != nil {
		return nil, fmt.Errorf("redisx: zrangebyscore %s [%d, %d]: %w", key, start, stop, err)
	}
	return pairs, nil
}

// ZRemRangeByRankCtx 按排名范围移除成员（0-indexed，-1 表示最后一个）。
// 常用于限制集合大小，如 ZRemRangeByRankCtx(ctx, key, 0, -501) 保留最近 500 个元素。
func (z *ZSetManager) ZRemRangeByRankCtx(ctx context.Context, key string, start, stop int64) (int64, error) {
	n, err := z.client.rdb.ZremrangebyrankCtx(ctx, z.client.buildKey(key), start, stop)
	if err != nil {
		return 0, fmt.Errorf("redisx: zremrangebyrank %s [%d, %d]: %w", key, start, stop, err)
	}
	return int64(n), nil
}
