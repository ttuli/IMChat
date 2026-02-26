package svc

import (
	"fmt"
	"log"
	"os"

	"IM2/interceptor"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/dao"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/pubsub"
	"IM2/internal/apps/websocket/gateway/internal/router"
	"IM2/internal/apps/websocket/gateway/internal/telemetry"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/zrpc"
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
	TelemetryBus      *telemetry.Bus
	MessageDao        *dao.MessageDAO
	ConversationDao   *dao.ConversationDAO

	GroupRpc grouprpc.GroupRpc
}

// NewServiceContext 创建服务上下文
func NewServiceContext(c config.Config) *ServiceContext {
	// 生成节点ID
	nodeID := c.WebSocket.NodeID
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
		c.WebSocket.NodeID = nodeID
	}

	// 创建编解码器
	codec := protocol.NewProtoCodec()

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

	// 创建遥测总线
	bus := telemetry.NewBus(nodeID, 0)
	bus.RegisterHandler(telemetry.DefaultLogHandler)

	// 生成 JetStream 上下文
	js, err := natsConn.JetStream()
	if err != nil {
		log.Fatalf("failed to create jetstream context: %v", err)
	}

	// 创建路由器
	r := router.NewRouter(redisClient, js, codec, nodeID, bus, pubsub.SubjectConfig{
		NodeSubjectPrefix: c.Nats.NodeSubjectPrefix,
		DBSubject:         c.Nats.DBSubject,
	})

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID, r)

	// 创建订阅者
	sub := pubsub.NewSubscriber(js, codec, nodeID, bus)

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Subscriber:        sub,
		RedisClient:       redisClient,
		NatsConn:          natsConn,
		TokenManager:      tokenmanager.NewTokenManager(c.TokenConfig),
		TelemetryBus:      bus,
		MessageDao:        dao.NewMessageDAO(c.DAO.MysqlSource, c.DAO.CacheSource),
		ConversationDao:   dao.NewConversationDAO(c.DAO.MysqlSource, c.DAO.CacheSource),

		GroupRpc: grouprpc.NewGroupRpc(zrpc.MustNewClient(c.GroupRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor))),
	}

	return svc
}
