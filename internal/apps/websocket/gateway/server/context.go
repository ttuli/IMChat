package server

import (
	"fmt"
	"log"
	"os"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/telemetry"
	"IM2/internal/apps/websocket/gateway/router"
	"IM2/pkg/routing"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// ServiceContext 服务上下文
type ServiceContext struct {
	Config            config.Config
	ConnectionManager connection.Manager
	Router            *router.Router
	Subscriber        *router.Subscriber
	// Routes 集群路由表（Redis）：用户路由由本网关维护，群成员由 Group 服务维护、此处只读
	Routes            *routing.Table
	NatsConn          *nats.Conn
	JetStream         nats.JetStreamContext
	TokenManager      *tokenmanager.TokenManager
	TelemetryBus      *telemetry.Bus
	Codec             protocol.Codec
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

	// 网关无状态化：不再初始化 Snowflake 发号器
	// MsgId 生成已迁移到 Message 服务本地完成

	// 创建路由表 (Redis KV 存储，与 Message/Group 服务共享同一份路由数据)
	routes, err := routing.NewTableFromConf(c.RouteStore)
	if err != nil {
		log.Fatalf("init route table failed: %v", err)
	}

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
	r := router.NewRouter(routes, natsConn, codec, nodeID, bus, router.SubjectConfig{
		NodeSubjectPrefix:     c.Nats.NodeSubjectPrefix,
		DBSubject:             c.Nats.DBSubject,
		BroadcastSubject:      c.Nats.BroadcastSubject,
		QueueBroadcastSubject: c.Nats.QueueBroadcastSubject,
	})

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID, r)

	// 创建订阅者
	sub := router.NewSubscriber(natsConn, nodeID, func(err error) {
		bus.Publish(err)
	})

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Subscriber:        sub,
		Routes:            routes,
		NatsConn:          natsConn,
		JetStream:         js,
		TokenManager:      tokenmanager.NewTokenManager(c.TokenConfig),
		TelemetryBus:      bus,
		Codec:             codec,
	}

	return svc
}
