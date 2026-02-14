package router

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/pubsub"
	"IM2/pkg/xerr"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// 路由键前缀
	routeKeyPrefix = "ws:route:"
	// 节点信息键前缀
	nodeKeyPrefix = "ws:node:"
	// 路由过期时间
	routeExpire = 24 * time.Hour
	// 节点心跳过期时间
	nodeHeartbeatExpire = 60 * time.Second
)

// Router 消息路由器
type Router struct {
	client    *redis.Client
	nodeID    string
	publisher *pubsub.Publisher
}

// NewRouter 创建路由器
func NewRouter(client *redis.Client, natsConn *nats.Conn, codec protocol.Codec, nodeID string) *Router {
	return &Router{
		client:    client,
		nodeID:    nodeID,
		publisher: pubsub.NewPublisher(natsConn, codec, nodeID),
	}
}

// RegisterUser 注册用户路由
func (r *Router) RegisterUser(ctx context.Context, userID uint64) error {
	key := getRouteKey(userID)
	if err := r.client.Set(ctx, key, r.nodeID, routeExpire).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "register user route failed")
	}
	logx.Debugf("[Router] registered user %d on node %s", userID, r.nodeID)
	return nil
}

// UnregisterUser 取消用户路由
func (r *Router) UnregisterUser(ctx context.Context, userID uint64) error {
	key := getRouteKey(userID)
	// 只删除当前节点的路由(防止删除其他节点的路由)
	storedNodeID, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil // 已经不存在
		}
		return xerr.Wrap(err, xerr.ErrCache, "get user route failed")
	}

	if storedNodeID == r.nodeID {
		if err := r.client.Del(ctx, key).Err(); err != nil {
			return xerr.Wrap(err, xerr.ErrCache, "delete user route failed")
		}
		logx.Debugf("[Router] unregistered user %d from node %s", userID, r.nodeID)
	}
	return nil
}

// GetUserNode 获取用户所在节点
func (r *Router) GetUserNode(ctx context.Context, userID uint64) (string, error) {
	key := getRouteKey(userID)
	nodeID, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", xerr.New(xerr.ErrNotFound, "user not connected")
		}
		return "", xerr.Wrap(err, xerr.ErrCache, "get user route failed")
	}
	return nodeID, nil
}

// IsLocalUser 检查用户是否在本节点
func (r *Router) IsLocalUser(ctx context.Context, userID uint64) (bool, error) {
	nodeID, err := r.GetUserNode(ctx, userID)
	if err != nil {
		return false, err
	}
	return nodeID == r.nodeID, nil
}

// RouteMessage 路由消息到目标用户
func (r *Router) RouteMessage(ctx context.Context, targetUserID uint64, msg *protocol.Message) error {
	// 获取目标用户所在节点
	targetNodeID, err := r.GetUserNode(ctx, targetUserID)
	if err != nil {
		return err
	}

	// 如果在本节点，返回错误让调用者处理本地发送
	if targetNodeID == r.nodeID {
		return xerr.New(xerr.ErrInternalServer, "target user is on local node")
	}

	// 通过 Pub/Sub 转发到目标节点
	internalMsg := &protocol.InternalMessage{
		TargetUserID: targetUserID,
		Message:      *msg,
	}

	return r.publisher.PublishToNode(ctx, targetNodeID, internalMsg)
}

// RegisterNode 注册节点信息并开始心跳
func (r *Router) RegisterNode(ctx context.Context) error {
	key := getNodeKey(r.nodeID)
	if isExist, _ := r.client.Get(ctx, key).Result(); isExist != "" {
		return xerr.New(xerr.ErrInternalServer, "nodeId already connected")
	}
	if err := r.client.Set(ctx, key, r.nodeID, nodeHeartbeatExpire).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "register node failed")
	}
	logx.Infof("[Router] registered node %s", r.nodeID)
	return nil
}

// NodeHeartbeat 节点心跳续期
func (r *Router) NodeHeartbeat(ctx context.Context) error {
	key := getNodeKey(r.nodeID)
	if err := r.client.Expire(ctx, key, nodeHeartbeatExpire).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "node heartbeat failed")
	}
	return nil
}

// StartHeartbeat 启动节点心跳协程
func (r *Router) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(nodeHeartbeatExpire / 2)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.NodeHeartbeat(ctx); err != nil {
					logx.Errorf("[Router] heartbeat failed: %v", err)
				}
			}
		}
	}()
}

// RenewUserRoute 续期用户路由
func (r *Router) RenewUserRoute(ctx context.Context, userID uint64) error {
	key := getRouteKey(userID)
	if err := r.client.Expire(ctx, key, routeExpire).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "renew user route failed")
	}
	return nil
}

// StartRouteHeartbeat 启动路由心跳协程，定期续期所有活跃用户的路由
func (r *Router) StartRouteHeartbeat(ctx context.Context, getActiveUserIDs func() []uint64) {
	ticker := time.NewTicker(routeExpire / 3)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				userIDs := getActiveUserIDs()
				for _, uid := range userIDs {
					if err := r.RenewUserRoute(ctx, uid); err != nil {
						logx.Errorf("[Router] renew route for user %d failed: %v", uid, err)
					}
				}
				logx.Debugf("[Router] renewed routes for %d users", len(userIDs))
			}
		}
	}()
}

// UnregisterNode 取消节点注册
func (r *Router) UnregisterNode(ctx context.Context) error {
	key := getNodeKey(r.nodeID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "unregister node failed")
	}
	logx.Infof("[Router] unregistered node %s", r.nodeID)
	return nil
}

// getRouteKey 获取用户路由键
func getRouteKey(userID uint64) string {
	return fmt.Sprintf("%s%d", routeKeyPrefix, userID)
}

// getNodeKey 获取节点信息键
func getNodeKey(nodeID string) string {
	return fmt.Sprintf("%s%s", nodeKeyPrefix, nodeID)
}
