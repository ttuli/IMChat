package svc

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/pubsub"
	"IM2/internal/apps/websocket/gateway/internal/router"
	"IM2/internal/apps/websocket/gateway/internal/telemetry"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/bwmarrin/snowflake"
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
	JetStream         nats.JetStreamContext
	TokenManager      *tokenmanager.TokenManager
	TelemetryBus      *telemetry.Bus
	SnowflakeNode     *snowflake.Node
}

// NewServiceContext 创建服务上下文
func NewServiceContext(c config.Config) *ServiceContext {
	// 生成节点ID：优先使用 K8s Downward API 注入的 Pod Name，降级到 hostname+uuid (本地开发)
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
	}
	c.WebSocket.NodeID = nodeID
	// 创建编解码器
	codec := protocol.NewProtoCodec()

	// 初始化 Snowflake ID 生成器
	// 将字符串格式的 nodeID 映射为 10 bit (0-1023) 的整型节点 ID
	hashID := int64(crc32.ChecksumIEEE([]byte(nodeID)) % 1024)
	sfNode, err := snowflake.NewNode(hashID)
	if err != nil {
		log.Fatalf("init snowflake failed: %v", err)
	}

	// 创建 Redis 客户端 (用于路由 KV 存储)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     c.RouteStore.Host,
		Password: c.RouteStore.Pass,
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

	// 初始化 JetStream context（用于有去重保证的消息发布）
	js, err := natsConn.JetStream()
	if err != nil {
		log.Fatalf("init jetstream failed: %v", err)
	}

	// 创建遥测总线
	bus := telemetry.NewBus(nodeID, 0)
	bus.RegisterHandler(telemetry.DefaultLogHandler)

	// 创建路由器
	r := router.NewRouter(redisClient, natsConn, codec, nodeID, bus, pubsub.SubjectConfig{
		NodeSubjectPrefix:     c.Nats.NodeSubjectPrefix,
		DBSubject:             c.Nats.DBSubject,
		BroadcastSubject:      c.Nats.BroadcastSubject,
		QueueBroadcastSubject: c.Nats.QueueBroadcastSubject,
	})

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID, r)

	// 创建订阅者
	sub := pubsub.NewSubscriber(natsConn, codec, nodeID, bus)

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Subscriber:        sub,
		RedisClient:       redisClient,
		NatsConn:          natsConn,
		JetStream:         js,
		TokenManager:      tokenmanager.NewTokenManager(c.TokenConfig),
		TelemetryBus:      bus,
		SnowflakeNode:     sfNode,
	}

	return svc
}
