package svc

import (
	"fmt"
	"log"
	"os"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/pubsub"
	"IM2/internal/apps/websocket/gateway/internal/router"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// ServiceContext 服务上下文
type ServiceContext struct {
	Config            config.Config
	ConnectionManager connection.Manager
	Router            *router.Router
	Subscriber        *pubsub.Subscriber
	RedisClient       *redis.Client
	NatsConn          *nats.Conn
	TokenManager      *tokenmanager.TokenManager
}

// NewServiceContext 创建服务上下文
func NewServiceContext(c config.Config) *ServiceContext {
	// 生成节点ID
	nodeID := c.WebSocket.NodeID
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
	}

	// 创建编解码器
	codec := protocol.NewJSONCodec()

	// 创建 Redis 客户端 (用于路由 KV 存储)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Host,
		Password: c.Redis.Pass,
		DB:       0,
	})

	// 创建 NATS 连接 (用于跨节点消息转发)
	natsUrl := c.Nats.Url
	if natsUrl == "" {
		natsUrl = nats.DefaultURL
	}
	natsConn, err := nats.Connect(natsUrl)
	if err != nil {
		log.Fatalf("connect to nats failed: %v", err)
	}

	// 创建路由器
	r := router.NewRouter(redisClient, natsConn, codec, nodeID)

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID, r)

	// 创建订阅者
	sub := pubsub.NewSubscriber(natsConn, codec, nodeID)

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Subscriber:        sub,
		RedisClient:       redisClient,
		NatsConn:          natsConn,
		TokenManager:      tokenmanager.NewTokenManager(c.TokenConfig),
	}

	return svc
}
