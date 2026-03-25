package server

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/common"
	"IM2/pkg/logger"

	"google.golang.org/protobuf/proto"
)

type GatewayServer struct {
	svcCtx *svc.ServiceContext
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGatewayServer(svcCtx *svc.ServiceContext) *GatewayServer {
	return &GatewayServer{
		svcCtx: svcCtx,
	}
}

// Start 启动服务
func (s *GatewayServer) Start() error {
	fmt.Println("Starting WebSocket Gateway Server logic...")

	s.ctx, s.cancel = context.WithCancel(context.Background())

	// 0. 启动遥测总线
	s.svcCtx.TelemetryBus.Start(s.ctx)

	// 1. 注册节点
	if err := s.svcCtx.Router.RegisterNode(s.ctx); err != nil {
		return fmt.Errorf("register node failed: %w", err)
	}

	// 2. 启动节点心跳 (非阻塞)
	s.svcCtx.Router.StartHeartbeat(s.ctx)

	// 3. 启动路由心跳 (定期续期活跃用户路由)
	s.svcCtx.Router.StartRouteHeartbeat(s.ctx, s.svcCtx.ConnectionManager.GetAllLocalUserIDs)

	// 4. 订阅跨节点路由消息
	nodeSubject := fmt.Sprintf("%s%s", s.svcCtx.Config.Nats.NodeSubjectPrefix, s.svcCtx.Config.WebSocket.NodeID)

	if err := s.svcCtx.Subscriber.Subscribe(s.ctx, nodeSubject, s.handleQueueSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe route message failed: %w", err)
	}
	if err := s.svcCtx.Subscriber.Subscribe(s.ctx, s.svcCtx.Config.Nats.BroadcastSubject, s.handleSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe broadcast message failed: %w", err)
	}
	if err := s.svcCtx.Subscriber.QueueSubscribe(s.ctx, s.svcCtx.Config.Nats.QueueBroadcastSubject,
		s.svcCtx.Config.Nats.QueueName, s.handleQueueSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe notice failed: %w", err)
	}

	go s.svcCtx.ConversationDao.StartSyncWorker()

	fmt.Println("WebSocket Gateway Server logic started successfully")
	return nil
}

// WebSocket Gateway Server logic stopped
func (s *GatewayServer) Stop() error {
	fmt.Println("Stopping WebSocket Gateway Server logic...")
	if s.cancel != nil {
		defer s.cancel()
	}

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	// 1. 关闭订阅
	if err := s.svcCtx.Subscriber.Close(); err != nil {
		logger.Errorf("close subscriber failed: %v", err)
	}

	// 2. 关闭连接管理器 (断开所有连接)
	if err := s.svcCtx.ConnectionManager.Close(); err != nil {
		logger.Errorf("close connection manager failed: %v", err)
	}

	// 3. 注销节点
	if err := s.svcCtx.Router.UnregisterNode(ctx); err != nil {
		logger.Errorf("unregister node failed: %v", err)
	}

	// 4. 关闭 NATS
	s.svcCtx.NatsConn.Close()

	// 5. 关闭 Conversation DAO
	s.svcCtx.ConversationDao.CloseDAO()

	// 6. 关闭 Redis (路由 KV)
	if err := s.svcCtx.RedisClient.Close(); err != nil {
		logger.Errorf("close redis client failed: %v", err)
	}

	// 7. 停止遥测总线
	s.svcCtx.TelemetryBus.Stop()

	fmt.Println("WebSocket Gateway Server logic stopped")
	return nil
}

func (s *GatewayServer) handleSubscribeMessage(ctx context.Context, msg *common.WSMessage) error {
	// 获取本地连接
	switch msg.RouteTargetType {
	case common.TargetType_USER:
		conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(msg.RouteTarget)
		if ok {
			return conn.Send(msg)
		}
	case common.TargetType_GROUP:
		if err := s.syncGroupMembership(msg); err != nil {
			return err
		}
		return s.svcCtx.ConnectionManager.SendToGroupLocal(ctx, msg.RouteTarget, msg)
	default:
		return nil
	}
	return nil
}

// syncGroupMembership 根据群组事件类型增量更新本地 groupMembers 映射。
// 在消息发送前调用，保证 SendToGroupLocal 使用最新的本地在线成员集合。
func (s *GatewayServer) syncGroupMembership(msg *common.WSMessage) error {
	if msg.Type != common.MessageType_GROUP_OP_NOTIFICATION {
		return nil
	}
	var notify common.GroupNotification
	if err := proto.Unmarshal(msg.Payload, &notify); err != nil {
		logger.Errorf("[syncGroupMembership] unmarshal failed: %v", err)
		return err
	}

	groupID := notify.GroupId

	switch notify.OpType {
	case common.GroupOperationType_GROUP_OP_JOIN:
		s.svcCtx.ConnectionManager.AddUsersToGroup(groupID, notify.TargetIds)

	case common.GroupOperationType_GROUP_OP_CREATE:
		members := append(notify.TargetIds, notify.OperatorId)
		s.svcCtx.ConnectionManager.AddUsersToGroup(groupID, members)

	case common.GroupOperationType_GROUP_OP_LEAVE,
		common.GroupOperationType_GROUP_OP_KICK:
		s.svcCtx.ConnectionManager.RemoveUsersFromGroup(groupID, notify.TargetIds)

	case common.GroupOperationType_GROUP_OP_DISMISS:
		// 解散：直接清除整个群的本地缓存
		s.svcCtx.ConnectionManager.InvalidateGroupCache(groupID)
	}

	return nil
}

func (s *GatewayServer) handleQueueSubscribeMessage(ctx context.Context, msg *common.WSMessage) error {
	switch msg.Type {
	case common.MessageType_GROUP_REQUEST:
		resp, err := s.svcCtx.GroupRpc.GetGroupManagers(ctx, &group.GetGroupManagersReq{
			GroupId: msg.RouteTarget,
		})
		if err != nil {
			s.svcCtx.TelemetryBus.Publish(err)
			return err
		}
		for _, manager := range resp.Managers {
			err := s.svcCtx.ConnectionManager.SendToUser(ctx, manager.UserId, msg)
			if err != nil {
				s.svcCtx.TelemetryBus.Publish(err)
			}
		}
		var apply common.GroupApply
		if err := proto.Unmarshal(msg.Payload, &apply); err != nil {
			s.svcCtx.TelemetryBus.Publish(err)
			return nil
		}
		if apply.HandlerId != 0 {
			err = s.svcCtx.ConnectionManager.SendToUser(ctx, apply.SenderId, msg)
			if err != nil {
				s.svcCtx.TelemetryBus.Publish(err)
			}
		}

		return nil
	}

	return nil
}
