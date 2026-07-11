package router

import (
	"context"
	"errors"
	"time"

	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/user"
	"IM2/pkg/routing"

	"google.golang.org/protobuf/proto"
)

// 路由心跳周期：短周期保证路由意外丢失（Redis 重启/误删）后能快速自愈，
// 否则单聊精准投递会把在线用户误判为离线（只存不推）
const routeHeartbeatInterval = 5 * time.Minute

// Router 消息路由器：路由表数据委托 routing 包维护（Redis），
// 本层负责节点身份、心跳编排与跨节点通知（NATS）。
type Router struct {
	table  *routing.Table
	nodeID string
	nats   *nats_util.Client
}

// NewRouter 创建路由器
func NewRouter(table *routing.Table, nats *nats_util.Client, nodeID string) *Router {
	return &Router{
		table:  table,
		nodeID: nodeID,
		nats:   nats,
	}
}

// RegisterUser 原子注册用户路由。
// 若该用户路由此前指向其他节点（快速重连/多端登录切换），通知旧节点清理其滞留连接，
// 保证「路由指向」与「实际持有连接的节点」收敛一致。
func (r *Router) RegisterUser(ctx context.Context, userID uint64) error {
	oldNode, err := r.table.RegisterUser(ctx, userID, r.nodeID)
	if err != nil {
		logger.Errorf("[Router] register user %d failed: %v", userID, err)
		return err
	}

	if oldNode != "" {
		// 旧节点可能仍持有该用户的连接：发内部踢下线通知让其清理。
		// 通知丢失时由路由心跳的冲突检测兜底。
		r.nats.PublishToNode(oldNode, buildKickoffMsg(userID))
	}

	logger.Infof("[Router] registered user %d on node %s (old=%s)", userID, r.nodeID, oldNode)
	return nil
}

// buildKickoffMsg 构造跨节点内部踢下线通知
func buildKickoffMsg(userID uint64) *transport.WSMessage {
	now := time.Now().UnixMilli()
	payload, _ := proto.Marshal(&user.UserKickoff{
		UserId:    userID,
		Reason:    "账号在其他设备登录",
		Timestamp: now,
	})
	return &transport.WSMessage{
		Type:            transport.MessageType_USER_KICKOFF,
		RouteTarget:     []uint64{userID},
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       now,
		Payload:         payload,
	}
}

// UnregisterUser 取消用户路由（仅当路由仍指向本节点时删除）
func (r *Router) UnregisterUser(ctx context.Context, userID uint64) error {
	if err := r.table.UnregisterUser(ctx, userID, r.nodeID); err != nil {
		logger.Errorf("[Router] unregister user %d failed: %v", userID, err)
		return err
	}
	logger.Infof("[Router] unregistered user %d from node %s", userID, r.nodeID)
	return nil
}

// GetUserNode 获取用户所在节点
func (r *Router) GetUserNode(ctx context.Context, userID uint64) (string, error) {
	nodeID, err := r.table.GetUserNode(ctx, userID)
	if err != nil {
		logger.Errorf("[Router] get node for user %d failed: %v", userID, err)
		return "", err
	}
	return nodeID, nil
}

// RegisterNode 注册节点信息并开始心跳
func (r *Router) RegisterNode(ctx context.Context) error {
	if err := r.table.RegisterNode(ctx, r.nodeID); err != nil {
		if !errors.Is(err, routing.ErrNodeAlreadyRegistered) {
			logger.Errorf("[Router] register node %s failed: %v", r.nodeID, err)
		}
		return err
	}
	logger.Infof("[Router] registered node %s", r.nodeID)
	return nil
}

// NodeHeartbeat 节点心跳续期
func (r *Router) NodeHeartbeat(ctx context.Context) error {
	if err := r.table.RenewNode(ctx, r.nodeID); err != nil {
		logger.Errorf("[Router] renew node %s heartbeat failed: %v", r.nodeID, err)
		return err
	}
	return nil
}

// StartHeartbeat 启动节点心跳协程
func (r *Router) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(routing.NodeTTL / 2)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.NodeHeartbeat(ctx); err != nil {
					logger.Errorf("[Router] heartbeat failed: %v", err)
				}
			}
		}
	}()
}

// RenewUserRoute 续期用户路由。
// 路由缺失时以本节点身份重新注册；返回 owned=false 表示路由已被其他节点持有
// （本地连接已被取代，调用方应清理）。
func (r *Router) RenewUserRoute(ctx context.Context, userID uint64) (owned bool, err error) {
	owned, err = r.table.RenewUser(ctx, userID, r.nodeID)
	if err != nil {
		logger.Errorf("[Router] renew route for user %d failed: %v", userID, err)
		return false, err
	}
	return owned, nil
}

// StartRouteHeartbeat 启动路由心跳协程，定期续期所有本地活跃用户的路由。
// onRouteConflict 在发现路由已被其他节点抢占时回调（跨节点踢下线通知丢失的兜底），
// 调用方应清理对应的本地滞留连接。
func (r *Router) StartRouteHeartbeat(ctx context.Context, getActiveUserIDs func() []uint64, onRouteConflict func(userID uint64)) {
	ticker := time.NewTicker(routeHeartbeatInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				userIDs := getActiveUserIDs()
				for _, uid := range userIDs {
					owned, err := r.RenewUserRoute(ctx, uid)
					if err != nil {
						logger.Errorf("[Router] renew route for user %d failed: %v", uid, err)
						continue
					}
					if !owned && onRouteConflict != nil {
						logger.Infof("[Router] route of user %d taken by another node, cleaning local connection", uid)
						onRouteConflict(uid)
					}
				}
				logger.Infof("[Router] renewed routes for %d users", len(userIDs))
			}
		}
	}()
}

// UnregisterNode 取消节点注册
func (r *Router) UnregisterNode(ctx context.Context) error {
	if err := r.table.UnregisterNode(ctx, r.nodeID); err != nil {
		logger.Errorf("[Router] unregister node %s failed: %v", r.nodeID, err)
		return err
	}
	logger.Infof("[Router] unregistered node %s", r.nodeID)
	return nil
}
