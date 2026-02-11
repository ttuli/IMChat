package svc

import (
	"context"
	"fmt"
	"os"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/pubsub"
	"IM2/internal/apps/websocket/gateway/internal/router"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// ServiceContext 服务上下文
type ServiceContext struct {
	Config            config.Config
	ConnectionManager connection.Manager
	Router            *router.Router
	Subscriber        *pubsub.Subscriber
	RedisClient       *redis.Client
}

// NewServiceContext 创建服务上下文
func NewServiceContext(c config.Config) *ServiceContext {
	// 生成节点ID
	nodeID := c.WebSocket.NodeID
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
	}

	// 节点地址
	nodeAddr := c.WebSocket.NodeAddr
	if nodeAddr == "" {
		nodeAddr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}

	// 创建 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Host,
		Password: c.Redis.Pass,
		DB:       0,
	})

	// 创建路由器
	r := router.NewRouter(redisClient, nodeID, nodeAddr)

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID, r)

	// 创建订阅者
	sub := pubsub.NewSubscriber(redisClient, nodeID)

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Subscriber:        sub,
		RedisClient:       redisClient,
	}

	return svc
}

// Start 启动服务(注册节点、订阅消息)
func (s *ServiceContext) Start(ctx context.Context) error {
	// 注册节点
	if err := s.Router.RegisterNode(ctx); err != nil {
		return fmt.Errorf("register node failed: %w", err)
	}

	// 启动心跳
	s.Router.StartHeartbeat(ctx)

	// 订阅消息
	if err := s.Subscriber.Subscribe(ctx, s.handleInternalMessage); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	logx.Info("[ServiceContext] started successfully")
	return nil
}

// Stop 停止服务
func (s *ServiceContext) Stop(ctx context.Context) error {
	// 关闭订阅者
	if err := s.Subscriber.Close(); err != nil {
		logx.Errorf("[ServiceContext] close subscriber failed: %v", err)
	}

	// 关闭连接管理器
	if err := s.ConnectionManager.Close(); err != nil {
		logx.Errorf("[ServiceContext] close connection manager failed: %v", err)
	}

	// 取消节点注册
	if err := s.Router.UnregisterNode(ctx); err != nil {
		logx.Errorf("[ServiceContext] unregister node failed: %v", err)
	}

	// 关闭 Redis 客户端
	if err := s.RedisClient.Close(); err != nil {
		logx.Errorf("[ServiceContext] close redis client failed: %v", err)
	}

	logx.Info("[ServiceContext] stopped")
	return nil
}

// handleInternalMessage 处理跨节点消息
func (s *ServiceContext) handleInternalMessage(ctx context.Context, msg *protocol.InternalMessage) error {
	// 获取本地连接
	conn, ok := s.ConnectionManager.GetLocalConnection(msg.TargetUserID)
	if !ok {
		logx.Slowf("[ServiceContext] user %d not found on this node", msg.TargetUserID)
		return nil
	}

	// 发送消息
	return conn.Send(&msg.Message)
}
