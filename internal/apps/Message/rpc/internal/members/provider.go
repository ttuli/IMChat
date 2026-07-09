package members

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Group/rpc/group"
	"IM2/pkg/logger"
	"IM2/pkg/redisx"

	"golang.org/x/sync/singleflight"
)

const (
	memberSetPrefix = "group:members:"
	// 成员缓存 TTL：群操作事件失效是加速手段，TTL 是事件丢失时的最终收敛兜底
	memberCacheTTL = 10 * time.Minute
	// TTL 抖动上限，避免同一批群缓存同时过期造成回源尖峰
	memberCacheTTLJitter = 2 * time.Minute

	rpcTimeout = 3 * time.Second
)

// refillScript 原子回填成员缓存：全量替换 + 续期。
// ARGV[1] = ttl 秒数，ARGV[2..] = 成员 ID。
const refillScript = `
redis.call('DEL', KEYS[1])
for i = 2, #ARGV do
	redis.call('SADD', KEYS[1], ARGV[i])
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`

// Provider 群成员来源：Redis SET 缓存，miss 时经 singleflight 回源 Group RPC。
// Group 服务是成员关系的唯一权威，user_session 表仅作为离线会话索引，不作为投递成员来源
// （退群用户的 user_session 行会保留以维持历史会话入口）。
type Provider struct {
	cache    *redisx.Client
	groupRpc grouprpc.GroupRpc
	sf       singleflight.Group
}

// NewProvider 创建群成员 Provider
func NewProvider(cache *redisx.Client, groupRpc grouprpc.GroupRpc) *Provider {
	return &Provider{
		cache:    cache,
		groupRpc: groupRpc,
	}
}

// GetMemberIDs 获取群成员 ID 列表。
// 群至少有群主一名成员，因此空集合视为缓存 miss。
func (p *Provider) GetMemberIDs(ctx context.Context, groupID uint64) ([]uint64, error) {
	key := memberSetKey(groupID)

	raw, err := p.cache.EvalCtx(ctx, `return redis.call('SMEMBERS', KEYS[1])`, []string{key})
	if err == nil {
		if ids := parseMemberIDs(raw); len(ids) > 0 {
			return ids, nil
		}
	} else {
		logger.Errorf("[MemberProvider] read member cache of group %d failed: %v", groupID, err)
	}

	v, err, _ := p.sf.Do(key, func() (interface{}, error) {
		return p.loadFromGroupRpc(ctx, groupID)
	})
	if err != nil {
		return nil, err
	}
	return v.([]uint64), nil
}

// Invalidate 失效指定群的成员缓存（群操作事件触发）
func (p *Provider) Invalidate(ctx context.Context, groupID uint64) {
	if _, err := p.cache.DelCtx(ctx, memberSetKey(groupID)); err != nil {
		logger.Errorf("[MemberProvider] invalidate member cache of group %d failed: %v", groupID, err)
	}
}

// loadFromGroupRpc 回源 Group RPC 并回填 Redis
func (p *Provider) loadFromGroupRpc(ctx context.Context, groupID uint64) ([]uint64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	resp, err := p.groupRpc.GetGroupMemberIDs(rpcCtx, &group.GetGroupMemberIDsReq{GroupId: groupID})
	if err != nil {
		return nil, fmt.Errorf("load members of group %d from group rpc: %w", groupID, err)
	}

	ids := make([]uint64, 0, len(resp.Members))
	args := make([]interface{}, 0, len(resp.Members)+1)
	ttl := memberCacheTTL + time.Duration(rand.Int63n(int64(memberCacheTTLJitter)))
	args = append(args, int(ttl.Seconds()))
	for _, m := range resp.Members {
		ids = append(ids, m.UserId)
		args = append(args, strconv.FormatUint(m.UserId, 10))
	}

	if len(ids) > 0 {
		if _, cacheErr := p.cache.EvalCtx(ctx, refillScript, []string{memberSetKey(groupID)}, args...); cacheErr != nil {
			logger.Errorf("[MemberProvider] refill member cache of group %d failed: %v", groupID, cacheErr)
		}
	}
	return ids, nil
}

// parseMemberIDs 解析 SMEMBERS 的返回值
func parseMemberIDs(raw interface{}) []uint64 {
	rows, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	ids := make([]uint64, 0, len(rows))
	for _, row := range rows {
		id, err := strconv.ParseUint(fmt.Sprintf("%v", row), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func memberSetKey(groupID uint64) string {
	return memberSetPrefix + strconv.FormatUint(groupID, 10)
}
