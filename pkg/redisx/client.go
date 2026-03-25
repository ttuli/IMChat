package redisx

import (
	"context"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ---------- 选项定义 ----------

type options struct {
	keyPrefix       string        // key 全局前缀
	defaultTTL      time.Duration // 默认过期时间
	bloomCapacity   uint          // 布隆过滤器容量
	bloomErrRate    float64       // 布隆过滤器误判率
	enableNullCache bool          // 是否缓存空值（防穿透）
	nullCacheTTL    time.Duration // 空值缓存时间
}

type Option func(*options)

// WithKeyPrefix 设置全局 key 前缀，格式: {prefix}:{key}
func WithKeyPrefix(prefix string) Option {
	return func(o *options) {
		o.keyPrefix = prefix
	}
}

// WithDefaultTTL 设置默认过期时间
func WithDefaultTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.defaultTTL = ttl
	}
}

// WithBloomFilter 配置布隆过滤器参数
func WithBloomFilter(capacity uint, errRate float64) Option {
	return func(o *options) {
		o.bloomCapacity = capacity
		o.bloomErrRate = errRate
	}
}

// WithNullCache 开启空值缓存，防止缓存穿透
func WithNullCache(ttl time.Duration) Option {
	return func(o *options) {
		o.enableNullCache = true
		o.nullCacheTTL = ttl
	}
}

// ---------- 客户端 ----------

// Client 是 redisx 的统一入口。
//
// 设计说明:
//   - rdb 是私有字段，外部无法绕过前缀逻辑直接操作底层 Redis
//   - StringManager / HashManager / ZSetManager 以匿名嵌入方式组合进来，
//     其方法直接提升至 Client，外部可通过嵌入 *Client 来 hook/覆盖这些方法
//   - setManager 以具名私有字段形式保留，通过 SetManager() 访问（含 Big Key 防护等额外逻辑）
//   - BloomFilter 通过 BloomFilter(name) 懒创建，与 setManager 保持一致的访问模式
type Client struct {
	rdb        *redis.Redis
	opts       options
	setManager *SetManager
	*StringManager          // 方法直接提升至 Client
	*HashManager            // 方法直接提升至 Client
	*ZSetManager            // 方法直接提升至 Client
	bloomCache map[string]*BloomFilter
	mu         sync.RWMutex
}

func NewClient(conf redis.RedisConf, optFns ...Option) (*Client, error) {
	opts := options{
		keyPrefix:     "",
		defaultTTL:    30 * time.Minute,
		bloomCapacity: 100_000,
		bloomErrRate:  0.01,
		nullCacheTTL:  60 * time.Second,
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	rdb, err := redis.NewRedis(conf)
	if err != nil {
		return nil, err
	}

	c := &Client{
		rdb:        rdb,
		opts:       opts,
		bloomCache: make(map[string]*BloomFilter),
	}
	c.setManager = &SetManager{client: c}
	c.StringManager = &StringManager{client: c}
	c.HashManager = &HashManager{client: c}
	c.ZSetManager = &ZSetManager{client: c}
	return c, nil
}

// buildKey 拼接带前缀的 key
func (c *Client) buildKey(key string) string {
	if c.opts.keyPrefix == "" {
		return key
	}
	return c.opts.keyPrefix + ":" + key
}

// BloomFilter 返回指定名称的布隆过滤器（懒创建，并发安全）
func (c *Client) BloomFilter(name string) *BloomFilter {
	c.mu.RLock()
	if bf, ok := c.bloomCache[name]; ok {
		c.mu.RUnlock()
		return bf
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if bf, ok := c.bloomCache[name]; ok {
		return bf
	}
	bf := &BloomFilter{
		client:   c,
		name:     name,
		capacity: c.opts.bloomCapacity,
		errRate:  c.opts.bloomErrRate,
	}
	c.bloomCache[name] = bf
	return bf
}

// ---------- 通用跨类型操作 ----------

// PipelinedCtx 执行 Redis Pipeline。
// 注意：fn 回调内的 key 由业务方自行构造，不会自动追加 keyPrefix。
func (c *Client) PipelinedCtx(ctx context.Context, fn func(redis.Pipeliner) error) error {
	return c.rdb.PipelinedCtx(ctx, fn)
}

// EvalCtx 执行 Lua 脚本，keys 中每个元素会自动追加 keyPrefix。
// 若不需要前缀处理，请构造好 key 后直接传入即可。
func (c *Client) EvalCtx(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	builtKeys := make([]string, len(keys))
	for i, k := range keys {
		builtKeys[i] = c.buildKey(k)
	}
	return c.rdb.EvalCtx(ctx, script, builtKeys, args...)
}