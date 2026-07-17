package redisx

import (
	"context"
	"fmt"
	"math"
)

// BloomFilter 基于 Redis BITSET 实现的布隆过滤器
// 设计说明:
//   - 不依赖 RedisBloom 模块，原生 Redis 即可运行
//   - 使用多个独立 hash 函数写入不同 bit 位
//   - 只能判断"一定不存在"或"可能存在"，不支持删除
type BloomFilter struct {
	client   *Client
	name     string  // filter 名称，作为 bitmap key
	capacity uint    // 预期元素数量
	errRate  float64 // 允许误判率
}

// 根据容量和误判率计算最优参数
func (b *BloomFilter) params() (m uint, k uint) {
	// m = -n*ln(p) / (ln2)^2
	m = uint(math.Ceil(-float64(b.capacity) * math.Log(b.errRate) / (math.Ln2 * math.Ln2)))
	// k = m/n * ln2
	k = uint(math.Ceil(float64(m) / float64(b.capacity) * math.Ln2))
	return
}

// hash 生成第 i 个 hash 函数对应的 bit 位
// 使用双重哈希：h(i) = h1(x) + i*h2(x)，避免引入第三方库
func (b *BloomFilter) positions(value string, m, k uint) []uint {
	h1 := fnv1a(value)
	h2 := fnv1a(value + ":h2") // 简单扰动产生第二个 hash

	positions := make([]uint, k)
	for i := uint(0); i < k; i++ {
		positions[i] = (h1 + i*h2) % m
	}
	return positions
}

// fnv1a 32-bit FNV-1a hash
func fnv1a(s string) uint {
	const (
		offset uint = 2166136261
		prime  uint = 16777619
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint(s[i])
		h *= prime
	}
	return h
}

// Add 向布隆过滤器中添加元素
func (b *BloomFilter) Add(ctx context.Context, value string) error {
	key := b.client.buildKey("bloom:" + b.name)
	m, k := b.params()
	positions := b.positions(value, m, k)

	// 用 pipeline 批量 SETBIT，减少 RTT
	for _, pos := range positions {
		if _, err := b.client.rdb.SetBit(key, int64(pos), 1); err != nil {
			return fmt.Errorf("redissdk: bloom setbit %s pos=%d: %w", key, pos, err)
		}
	}
	return nil
}

// MightExist 判断元素是否可能存在
//
// 返回 false  => 元素一定不存在
// 返回 true   => 元素可能存在（存在误判率）
func (b *BloomFilter) MightExist(ctx context.Context, value string) (bool, error) {
	key := b.client.buildKey("bloom:" + b.name)
	m, k := b.params()
	positions := b.positions(value, m, k)

	for _, pos := range positions {
		val, err := b.client.rdb.GetBit(key, int64(pos))
		if err != nil {
			return false, fmt.Errorf("redissdk: bloom getbit %s pos=%d: %w", key, pos, err)
		}
		if val == 0 {
			return false, nil // 一定不存在
		}
	}
	return true, nil // 可能存在
}

// Reset 清空布隆过滤器（删除 bitmap key）
func (b *BloomFilter) Reset(ctx context.Context) error {
	key := b.client.buildKey("bloom:" + b.name)
	if _, err := b.client.rdb.Del(key); err != nil {
		return fmt.Errorf("redissdk: bloom reset %s: %w", key, err)
	}
	return nil
}