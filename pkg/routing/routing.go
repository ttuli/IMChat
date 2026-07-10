// Package routing 提供集群消息路由表的纯数据操作（基于 Redis）。
//
// 路由表由三类数据构成，多个服务读写同一份数据：
//   - 用户路由  ws:route:{userID}   → 网关节点 ID（网关在 WS 连接建立后注册、心跳续期）
//   - 节点心跳  ws:node:{nodeID}    → 节点存活标记（网关注册并定期续期）
//   - 群成员    group:members:{gid} → 成员 ID 集合（Group 服务同步直写增量，
//     读取方 miss 时回源 Group RPC 后全量回填，TTL 是写入失败时的最终收敛兜底）
//
// 本包只做数据维护与查询，不涉及任何消息投递（NATS 等）；投递策略由调用方
// 根据查询结果（RouteStatus）自行决定。键格式被网关、Message、Group 多个服务
// 共享，修改时必须同步所有读写方。
package routing

import (
	"errors"
	"fmt"
	"time"

	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// userRouteKeyPrefix 用户路由键前缀
	userRouteKeyPrefix = "ws:route:"
	// nodeKeyPrefix 节点心跳键前缀
	nodeKeyPrefix = "ws:node:"
	// groupMemberKeyPrefix 群成员集合键前缀
	groupMemberKeyPrefix = "group:members:"
)

const (
	// UserRouteTTL 用户路由过期时间（由网关路由心跳续期）
	UserRouteTTL = 24 * time.Hour
	// NodeTTL 节点心跳键过期时间，超时未续期视为节点已死
	NodeTTL = 60 * time.Second
	// DefaultGroupTTL 群成员集合基础 TTL：直写/事件是加速手段，TTL 保证最终收敛
	DefaultGroupTTL = 10 * time.Minute
	// groupTTLJitter TTL 抖动上限，避免同一批群集合同时过期造成回源尖峰
	groupTTLJitter = 2 * time.Minute
)

// ErrNodeAlreadyRegistered 节点 ID 已被其他存活实例占用
var ErrNodeAlreadyRegistered = errors.New("routing: node already registered")

// RouteStatus 用户路由查询结果
type RouteStatus int

const (
	// RouteUnknown 路由状态不可信（Redis 异常 / 路由指向已死节点），调用方应广播兜底
	RouteUnknown RouteStatus = iota
	// RouteOffline 确认不在线（路由键不存在），调用方可只存不推
	RouteOffline
	// RouteOnline 路由有效且节点存活，可精准投递
	RouteOnline
)

// Table 路由表。并发安全，可被多个 goroutine 共享。
type Table struct {
	client *redisx.Client
}

// NewTable 基于已有 redisx 客户端创建路由表。
// 客户端不能配置 keyPrefix，否则键与其他服务写入的路由数据不一致。
func NewTable(client *redisx.Client) *Table {
	return &Table{client: client}
}

// NewTableFromConf 从 Redis 配置创建路由表（内部客户端不带 keyPrefix）
func NewTableFromConf(conf redis.RedisConf) (*Table, error) {
	client, err := redisx.NewClient(conf)
	if err != nil {
		return nil, err
	}
	return NewTable(client), nil
}

// userRouteKey 用户路由键
func userRouteKey(userID uint64) string {
	return fmt.Sprintf("%s%d", userRouteKeyPrefix, userID)
}

// nodeKey 节点心跳键
func nodeKey(nodeID string) string {
	return nodeKeyPrefix + nodeID
}

// groupMemberKey 群成员集合键
func groupMemberKey(groupID uint64) string {
	return fmt.Sprintf("%s%d", groupMemberKeyPrefix, groupID)
}
