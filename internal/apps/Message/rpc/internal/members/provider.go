package members

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Group/rpc/group"
	"IM2/pkg/logger"
	"IM2/pkg/routing"

	"golang.org/x/sync/singleflight"
)

const rpcTimeout = 3 * time.Second

// Provider 群成员来源：路由表（Redis SET，Group 服务同步直写维护），
// miss 时经 singleflight 回源 Group RPC 并全量回填。
// Group 服务是成员关系的唯一权威，user_session 表仅作为离线会话索引，不作为投递成员来源
// （退群用户的 user_session 行会保留以维持历史会话入口）。
type Provider struct {
	routes   *routing.Table
	groupRpc grouprpc.GroupRpc
	sf       singleflight.Group
}

// NewProvider 创建群成员 Provider
func NewProvider(routes *routing.Table, groupRpc grouprpc.GroupRpc) *Provider {
	return &Provider{
		routes:   routes,
		// groupRpc: groupRpc,
	}
}

// GetMemberIDs 获取群成员 ID 列表。
// 群至少有群主一名成员，因此空集合视为缓存 miss。
func (p *Provider) GetMemberIDs(ctx context.Context, groupID uint64) ([]uint64, error) {
	ids, err := p.routes.GetGroupMembers(ctx, groupID)
	if err != nil {
		logger.Errorf("[MemberProvider] read member route of group %d failed: %v", groupID, err)
	} else if len(ids) > 0 {
		return ids, nil
	}

	v, err, _ := p.sf.Do(fmt.Sprintf("group:%d", groupID), func() (interface{}, error) {
		return p.loadFromGroupRpc(ctx, groupID)
	})
	if err != nil {
		return nil, err
	}
	return v.([]uint64), nil
}

// loadFromGroupRpc 回源 Group RPC 并全量回填路由表
func (p *Provider) loadFromGroupRpc(ctx context.Context, groupID uint64) ([]uint64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	resp, err := p.groupRpc.GetGroupMemberIDs(rpcCtx, &group.GetGroupMemberIDsReq{GroupId: groupID})
	if err != nil {
		return nil, fmt.Errorf("load members of group %d from group rpc: %w", groupID, err)
	}

	ids := make([]uint64, 0, len(resp.Members))
	for _, m := range resp.Members {
		ids = append(ids, m.UserId)
	}

	if len(ids) > 0 {
		// ttl=0 → 使用路由表默认 TTL + 抖动
		if cacheErr := p.routes.ReplaceGroupMembers(ctx, groupID, ids, 0); cacheErr != nil {
			logger.Errorf("[MemberProvider] refill member route of group %d failed: %v", groupID, cacheErr)
		}
	}
	return ids, nil
}
