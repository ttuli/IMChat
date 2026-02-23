package server

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/common"
	"IM2/pkg/logger"
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

	// 4. 订阅跨节点消息
	if err := s.svcCtx.Subscriber.Subscribe(s.ctx, s.handleInternalMessage); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	fmt.Println("WebSocket Gateway Server logic started successfully")
	return nil
}

// Stop 停止服务
func (s *GatewayServer) Stop() error {
	fmt.Println("Stopping WebSocket Gateway Server logic...")
	if s.cancel != nil {
		s.cancel()
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

	// 5. 关闭 Redis (路由 KV)
	if err := s.svcCtx.RedisClient.Close(); err != nil {
		logger.Errorf("close redis client failed: %v", err)
	}

	// 6. 停止遥测总线
	s.svcCtx.TelemetryBus.Stop()

	fmt.Println("WebSocket Gateway Server logic stopped")
	return nil
}

func (s *GatewayServer) handleInternalMessage(ctx context.Context, msg *common.InternalMessage) error {
	// 获取本地连接
	switch msg.TargetType {
	case common.TargetType_USER:
		conn, ok := s.svcCtx.ConnectionManager.GetLocalConnection(msg.TargetId)
		if ok {
			return conn.Send(msg.Message)
		}
	case common.TargetType_GROUP:
		return s.svcCtx.ConnectionManager.SendToGroupLocal(ctx, msg.TargetId, msg.Message)
	default:
		return nil
	}
	return nil
}
