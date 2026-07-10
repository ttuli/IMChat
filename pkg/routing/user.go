package routing

import (
	"context"
	"fmt"
)

// registerUserScript 原子地抢占路由并返回旧持有节点。
// 单次往返完成「读旧值 + 写新值」，消除并发注册时的读写间隙。
const registerUserScript = `
local old = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
if old and old ~= ARGV[1] then
	return old
end
return ''
`

// unregisterUserScript 仅当路由仍指向本节点时删除（compare-and-delete），
// 防止旧连接的注销误删其他节点刚注册的新路由。
const unregisterUserScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`

// renewUserScript 路由心跳续期：
//   - 路由缺失（意外过期/Redis 抖动丢失）→ 以本节点身份重新注册，修复静默漏推
//   - 路由属于本节点 → 正常续期
//   - 路由已被其他节点抢占 → 不覆盖，返回 0（本地连接已过时，由调用方清理）
const renewUserScript = `
local cur = redis.call('GET', KEYS[1])
if not cur then
	redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
	return 1
end
if cur == ARGV[1] then
	redis.call('EXPIRE', KEYS[1], ARGV[2])
	return 1
end
return 0
`

// lookupUserScript 单次往返完成「查路由 + 校验节点存活」。
// 路由存在但节点心跳键已消失，说明路由是过期脏数据（节点宕机未清理）。
// 注：脚本内拼接第二个 key，仅适用于单实例/主从 Redis（本项目部署形态），不兼容 Cluster。
const lookupUserScript = `
local node = redis.call('GET', KEYS[1])
if not node then
	return {'', 0}
end
local alive = redis.call('EXISTS', ARGV[1] .. node)
return {node, alive}
`

// RegisterUser 原子注册用户路由到指定节点。
// 返回此前持有该路由的旧节点 ID（无旧路由或旧路由即本节点时为空），
// 调用方可据此通知旧节点清理滞留连接。
func (t *Table) RegisterUser(ctx context.Context, userID uint64, nodeID string) (oldNode string, err error) {
	raw, err := t.client.EvalCtx(ctx, registerUserScript,
		[]string{userRouteKey(userID)}, nodeID, int(UserRouteTTL.Seconds()))
	if err != nil {
		return "", err
	}
	if raw == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", raw), nil
}

// UnregisterUser 取消用户路由。仅当路由仍指向 nodeID 时删除，
// 路由已被其他节点接管时不做任何操作。
func (t *Table) UnregisterUser(ctx context.Context, userID uint64, nodeID string) error {
	_, err := t.client.EvalCtx(ctx, unregisterUserScript,
		[]string{userRouteKey(userID)}, nodeID)
	return err
}

// RenewUser 续期用户路由。路由缺失时以 nodeID 身份重新注册；
// 返回 owned=false 表示路由已被其他节点持有（调用方应清理本地滞留连接）。
func (t *Table) RenewUser(ctx context.Context, userID uint64, nodeID string) (owned bool, err error) {
	raw, err := t.client.EvalCtx(ctx, renewUserScript,
		[]string{userRouteKey(userID)}, nodeID, int(UserRouteTTL.Seconds()))
	if err != nil {
		return false, err
	}
	res, _ := raw.(int64)
	return res == 1, nil
}

// GetUserNode 获取用户路由指向的节点 ID（不校验节点存活），路由不存在返回空串
func (t *Table) GetUserNode(ctx context.Context, userID uint64) (string, error) {
	return t.client.GetCtx(ctx, userRouteKey(userID))
}

// LookupUser 查询用户路由并校验目标节点存活（单次 Lua 往返）。
// Redis 异常时返回 RouteUnknown 与 err，调用方应广播兜底。
func (t *Table) LookupUser(ctx context.Context, userID uint64) (node string, status RouteStatus, err error) {
	raw, err := t.client.EvalCtx(ctx, lookupUserScript,
		[]string{userRouteKey(userID)}, nodeKeyPrefix)
	if err != nil {
		return "", RouteUnknown, err
	}

	row, ok := raw.([]interface{})
	if !ok || len(row) != 2 {
		return "", RouteUnknown, fmt.Errorf("routing: unexpected lookup result %v", raw)
	}
	node = fmt.Sprintf("%v", row[0])
	alive, _ := row[1].(int64)

	if node == "" {
		return "", RouteOffline, nil
	}
	if alive == 1 {
		return node, RouteOnline, nil
	}
	return "", RouteUnknown, nil
}

// LookupUsers 批量查询用户路由并按存活节点聚合。
// 返回 nodeTargets（节点 → 该节点上的在线用户）与 fallback（路由指向已死节点、
// 状态不可信需广播兜底的用户）；路由不存在的用户视为离线，不出现在任何返回值中。
// 查询整体失败时返回 err，调用方应将全部用户视为需兜底。
func (t *Table) LookupUsers(ctx context.Context, userIDs []uint64) (nodeTargets map[string][]uint64, fallback []uint64, err error) {
	if len(userIDs) == 0 {
		return nil, nil, nil
	}

	routeKeys := make([]string, len(userIDs))
	for i, uid := range userIDs {
		routeKeys[i] = userRouteKey(uid)
	}
	routes, err := t.client.MgetCtx(ctx, routeKeys...)
	if err != nil {
		return nil, nil, err
	}
	if len(routes) != len(userIDs) {
		return nil, nil, fmt.Errorf("routing: mget returned %d rows for %d users", len(routes), len(userIDs))
	}

	// 汇总候选节点并批量校验存活（ws:node:{id} 心跳键存在即存活）
	nodeAlive := make(map[string]bool)
	for _, node := range routes {
		if node != "" {
			nodeAlive[node] = false
		}
	}
	if len(nodeAlive) > 0 {
		nodes := make([]string, 0, len(nodeAlive))
		nodeKeys := make([]string, 0, len(nodeAlive))
		for node := range nodeAlive {
			nodes = append(nodes, node)
			nodeKeys = append(nodeKeys, nodeKey(node))
		}
		// 存活校验失败时 nodeAlive 保持全 false → 这些用户全部进 fallback，不漏投
		if vals, aliveErr := t.client.MgetCtx(ctx, nodeKeys...); aliveErr == nil && len(vals) == len(nodes) {
			for i, v := range vals {
				if v != "" {
					nodeAlive[nodes[i]] = true
				}
			}
		}
	}

	nodeTargets = make(map[string][]uint64)
	for i, uid := range userIDs {
		node := routes[i]
		switch {
		case node == "":
			// 离线：只存不推
		case nodeAlive[node]:
			nodeTargets[node] = append(nodeTargets[node], uid)
		default:
			// 路由指向已死节点（宕机未清理的脏路由）：广播兜底
			fallback = append(fallback, uid)
		}
	}
	return nodeTargets, fallback, nil
}

// RegisterNode 注册节点心跳键；节点 ID 已被其他存活实例占用时返回 ErrNodeAlreadyRegistered
func (t *Table) RegisterNode(ctx context.Context, nodeID string) error {
	ok, err := t.client.SetnxExCtx(ctx, nodeKey(nodeID), nodeID, int(NodeTTL.Seconds()))
	if err != nil {
		return err
	}
	if !ok {
		return ErrNodeAlreadyRegistered
	}
	return nil
}

// RenewNode 节点心跳续期。无条件重写心跳键：
// 键意外丢失（Redis 重启/误删）时自动重建，避免节点被长期误判为已死。
func (t *Table) RenewNode(ctx context.Context, nodeID string) error {
	return t.client.SetexCtx(ctx, nodeKey(nodeID), nodeID, int(NodeTTL.Seconds()))
}

// UnregisterNode 注销节点心跳键
func (t *Table) UnregisterNode(ctx context.Context, nodeID string) error {
	_, err := t.client.DelCtx(ctx, nodeKey(nodeID))
	return err
}
