package server

import (
	"context"
	"fmt"
	"time"

	"IM2/pkg/logger"
	"IM2/pkg/proto/social"
	"IM2/pkg/proto/transport"

	"google.golang.org/protobuf/proto"
)

type GatewayServer struct {
	svcCtx *ServiceContext
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGatewayServer(svcCtx *ServiceContext) *GatewayServer {
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

	// 3. 启动路由心跳 (定期续期活跃用户路由；路由丢失自动重注册，
	//    路由被其他节点抢占时清理本地滞留连接)
	s.svcCtx.Router.StartRouteHeartbeat(s.ctx, s.svcCtx.ConnectionManager.GetAllLocalUserIDs, func(userID uint64) {
		if conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(userID); ok {
			conn.Kick("账号在其他设备登录")
		}
	})

	// 4. 订阅跨节点路由消息
	nodeSubject := fmt.Sprintf("%s%s", s.svcCtx.Config.Nats.NodeSubjectPrefix, s.svcCtx.Config.WebSocket.NodeID)

	if err := s.svcCtx.Subscriber.Subscribe(s.ctx, nodeSubject, s.handleSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe route message failed: %w", err)
	}
	if err := s.svcCtx.Subscriber.Subscribe(s.ctx, s.svcCtx.Config.Nats.BroadcastSubject, s.handleSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe broadcast message failed: %w", err)
	}
	if err := s.svcCtx.Subscriber.QueueSubscribe(s.ctx, s.svcCtx.Config.Nats.QueueBroadcastSubject,
		s.svcCtx.Config.Nats.QueueName, s.handleQueueSubscribeMessage); err != nil {
		return fmt.Errorf("subscribe notice failed: %w", err)
	}

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

	// 5. 停止遥测总线
	s.svcCtx.TelemetryBus.Stop()

	fmt.Println("WebSocket Gateway Server logic stopped")
	return nil
}

func (s *GatewayServer) handleSubscribeMessage(ctx context.Context, data []byte) error {
	msg := &transport.WSMessage{}
	if err := s.svcCtx.Codec.Decode(data, msg); err != nil {
		s.svcCtx.TelemetryBus.Publish(err)
		return nil
	}

	// 拦截跨节点踢下线通知：用户在其他节点重新注册了路由，本节点持有的连接已过时。
	if msg.Type == transport.MessageType_USER_KICKOFF {
		for _, target := range msg.RouteTarget {
			// 二次校验：若路由当前已指回本节点（用户快速切换后又连回来了），
			// 说明这是迟到的过期通知，忽略，避免误踢最新连接。
			if node, _ := s.svcCtx.Router.GetUserNode(ctx, target); node == s.svcCtx.Config.WebSocket.NodeID {
				continue
			}
			if conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(target); ok {
				conn.Kick("账号在其他设备登录")
			}
		}
		return nil
	}

	// 获取本地连接
	switch msg.RouteTargetType {
	case transport.TargetType_USER:
		for _, target := range msg.RouteTarget {
			conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(target)
			if ok {
				conn.Send(msg)
			}
		}
		return nil
	case transport.TargetType_GROUP:
		// 定向扇出消息：deliver_to 携带本节点需投递的用户，按列表精准投本地连接，
		// 不再依赖网关侧群成员映射。该字段是集群内部路由信息，投递前清空，不下发客户端。
		if len(msg.DeliverTo) > 0 {
			targets := msg.DeliverTo
			msg.DeliverTo = nil
			for _, uid := range targets {
				if conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(uid); ok {
					conn.Send(msg)
				}
			}
			return nil
		}
		// 兼容路径：群操作通知等未带 deliver_to 的广播，
		// 查路由表（Redis，Group 服务直写维护）获取群成员，再过滤本地连接投递。
		// 集合缺失（TTL 过期且尚无消息触发回源）时无法定位本地成员，跳过；
		// 消息类通知均已持久化或可由客户端主动拉取对齐。
		for _, target := range msg.RouteTarget {
			members, err := s.svcCtx.Routes.GetGroupMembers(ctx, target)
			if err != nil {
				s.svcCtx.TelemetryBus.Publish(err)
				logger.Errorf("[handleSubscribeMessage] get members of group %d failed: %v", target, err)
				continue
			}
			if len(members) == 0 {
				logger.Infof("[handleSubscribeMessage] no route entry for group %d, skip local fanout", target)
				continue
			}
			s.svcCtx.ConnectionManager.SendToUsersLocal(ctx, members, msg)
		}
		return nil
	default:
		return nil
	}
}

func (s *GatewayServer) handleQueueSubscribeMessage(ctx context.Context, data []byte) error {
	msg := &transport.WSMessage{}
	if err := s.svcCtx.Codec.Decode(data, msg); err != nil {
		s.svcCtx.TelemetryBus.Publish(err)
		return nil
	}

	switch msg.Type {
	case transport.MessageType_GROUP_REQUEST:
		var apply social.GroupApply
		if err := proto.Unmarshal(msg.Payload, &apply); err != nil {
			s.svcCtx.TelemetryBus.Publish(err)
			return nil
		}
		for _, manager := range msg.RouteTarget {
			err := s.svcCtx.ConnectionManager.SendToUser(ctx, manager, msg)
			if err != nil {
				s.svcCtx.TelemetryBus.Publish(err)
			}
		}

		if apply.HandlerId != 0 {
			err := s.svcCtx.ConnectionManager.SendToUser(ctx, apply.SenderId, msg)
			if err != nil {
				s.svcCtx.TelemetryBus.Publish(err)
			}
		}
	}

	return nil
}
