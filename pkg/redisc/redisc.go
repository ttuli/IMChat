package redisc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// 每个bitmap分片的大小（2^32 = 4,294,967,296）
	BitmapShardSize = 1 << 32
	// ID映射的key
	UserIDMapKey = "user:id_map"
	// 反向映射key（可选）
	UserReverseMapKey = "user:reverse_map"
	// ID计数器
	UserIDCounterKey = "user:id_counter"
	// 在线状态bitmap key前缀
	OnlineUserBitmapPrefix = "online_user"
)

type RedisModel struct {
	*redis.Redis
}
type idPair struct {
	snowflakeID uint64
	seqID       uint64
}

func MustNewRedis(conf redis.RedisConf) *RedisModel {
	return &RedisModel{
		Redis: redis.MustNewRedis(conf),
	}
}

func (m *RedisModel) getOrCreateSequentialID(ctx context.Context, snowflakeID uint64) (uint64, error) {
	result, err := m.Redis.HgetCtx(ctx, UserIDMapKey, strconv.FormatUint(snowflakeID, 10))
	if err != nil && err != redis.Nil {
		return 0, err
	}

	if result != "" {
		seqID, err := strconv.ParseUint(result, 10, 64)
		if err == nil {
			return seqID, nil
		}
	}

	// 2. 映射不存在，创建新映射（使用Lua脚本保证原子性）
	script := `
		local snowflake_id = ARGV[1]
		local map_key = KEYS[1]
		local counter_key = KEYS[2]
		local reverse_map_key = KEYS[3]
		
		-- 再次检查是否已存在（防止并发）
		local existing = redis.call('HGET', map_key, snowflake_id)
		if existing then
			return tonumber(existing)
		end
		
		-- 生成新的连续ID
		local seq_id = redis.call('INCR', counter_key)
		
		-- 保存映射关系
		redis.call('HSET', map_key, snowflake_id, seq_id)
		redis.call('HSET', reverse_map_key, seq_id, snowflake_id)
		
		return seq_id
	`

	res, err := m.Redis.EvalCtx(ctx, script, []string{
		UserIDMapKey,
		UserIDCounterKey,
		UserReverseMapKey,
	}, strconv.FormatUint(snowflakeID, 10))

	if err != nil {
		return 0, fmt.Errorf("create sequential ID failed: %w", err)
	}

	seqID, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid sequential ID type")
	}

	return uint64(seqID), nil
}

func (m *RedisModel) SetOnline(ctx context.Context, snowflakeID uint64) error {
	// 获取连续ID
	seqID, err := m.getOrCreateSequentialID(ctx, snowflakeID)
	if err != nil {
		return fmt.Errorf("get sequential ID failed: %w", err)
	}

	// 计算bitmap分片和偏移量
	shardID := seqID / BitmapShardSize
	offset := seqID % BitmapShardSize

	// 设置bitmap
	bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
	_, err = m.Redis.SetBitCtx(ctx, bitmapKey, int64(offset), 1)
	if err != nil {
		return fmt.Errorf("set bitmap failed: %w", err)
	}

	return nil
}

func (m *RedisModel) SetOffline(ctx context.Context, snowflakeID uint64) error {
	seqID, err := m.getOrCreateSequentialID(ctx, snowflakeID)
	if err != nil {
		return fmt.Errorf("get sequential ID failed: %w", err)
	}

	shardID := seqID / BitmapShardSize
	offset := seqID % BitmapShardSize

	bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
	_, err = m.Redis.SetBitCtx(ctx, bitmapKey, int64(offset), 0)
	if err != nil {
		return fmt.Errorf("set bitmap failed: %w", err)
	}

	return nil
}

func (m *RedisModel) IsOnline(ctx context.Context, snowflakeID uint64) (bool, error) {
	seqID, err := m.getOrCreateSequentialID(ctx, snowflakeID)
	if err != nil {
		return false, fmt.Errorf("get sequential ID failed: %w", err)
	}

	shardID := seqID / BitmapShardSize
	offset := seqID % BitmapShardSize

	bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
	bit, err := m.Redis.GetBitCtx(ctx, bitmapKey, int64(offset))
	if err != nil {
		return false, fmt.Errorf("get bitmap failed: %w", err)
	}

	return bit == 1, nil
}

func (m *RedisModel) BatchSetOnline(ctx context.Context, snowflakeIDs []uint64) error {
	if len(snowflakeIDs) == 0 {
		return nil
	}

	// 1. 批量获取或创建连续ID
	seqIDs := make([]uint64, len(snowflakeIDs))
	for i, snowflakeID := range snowflakeIDs {
		seqID, err := m.getOrCreateSequentialID(ctx, snowflakeID)
		if err != nil {
			return fmt.Errorf("get sequential ID failed for %d: %w", snowflakeID, err)
		}
		seqIDs[i] = seqID
	}

	// 2. 使用pipeline批量设置bitmap
	err := m.Redis.Pipelined(func(p redis.Pipeliner) error {
		for _, seqID := range seqIDs {
			shardID := seqID / BitmapShardSize
			offset := seqID % BitmapShardSize
			bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
			err := p.SetBit(ctx, bitmapKey, int64(offset), 1).Err()
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch set bitmap failed: %w", err)
	}

	return nil
}

func (m *RedisModel) BatchIsOnline(ctx context.Context, snowflakeIDs []uint64) (map[uint64]bool, error) {
	result := make(map[uint64]bool, len(snowflakeIDs))

	if len(snowflakeIDs) == 0 {
		return result, nil
	}

	// 1. 批量获取连续ID

	pairs := make([]idPair, 0, len(snowflakeIDs))
	for _, snowflakeID := range snowflakeIDs {
		seqID, err := m.getOrCreateSequentialID(ctx, snowflakeID)
		if err != nil {
			return nil, fmt.Errorf("get sequential ID failed for %d: %w", snowflakeID, err)
		}
		pairs = append(pairs, idPair{snowflakeID: snowflakeID, seqID: seqID})
	}

	// 2. 使用pipeline批量查询bitmap
	var cmds []*redis.IntCmd // ✅ 先保存命令引用

	err := m.Redis.Pipelined(func(p redis.Pipeliner) error {
		for _, pair := range pairs {
			shardID := pair.seqID / BitmapShardSize
			offset := pair.seqID % BitmapShardSize
			bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
			cmd := p.GetBit(ctx, bitmapKey, int64(offset))
			cmds = append(cmds, cmd)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batch get bitmap failed: %w", err)
	}

	for i, cmd := range cmds {
		bit, err := cmd.Result()
		if err != nil {
			result[pairs[i].snowflakeID] = false // 默认值
			continue
		}
		result[pairs[i].snowflakeID] = bit == 1
	}

	return result, nil
}

func (m *RedisModel) ClearAllOnlineStatus(ctx context.Context) error {
	// 获取当前最大的连续ID以确定分片数量
	maxSeqID, err := m.Redis.GetCtx(ctx, UserIDCounterKey)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("get max sequential ID failed: %w", err)
	}

	// 如果没有用户，直接返回
	if err == redis.Nil || maxSeqID == "" {
		return nil
	}

	maxID, _ := strconv.ParseUint(maxSeqID, 10, 64)
	maxShardID := maxID / BitmapShardSize

	// 使用pipeline批量删除所有bitmap分片
	var keys []string
	for shardID := uint64(0); shardID <= maxShardID; shardID++ {
		bitmapKey := fmt.Sprintf("%s:{%d}", OnlineUserBitmapPrefix, shardID)
		keys = append(keys, bitmapKey)
	}

	if len(keys) == 0 {
		return nil
	}

	// 批量删除
	pipeErr := m.Redis.Pipelined(func(p redis.Pipeliner) error {
		for _, key := range keys {
			p.Del(ctx, key)
		}
		return nil
	})

	if pipeErr != nil {
		return fmt.Errorf("clear online status failed: %w", pipeErr)
	}

	return nil
}
