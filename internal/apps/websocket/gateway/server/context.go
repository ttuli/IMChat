package server

import (
	"fmt"
	"log"
	"os"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/connection"
	"IM2/internal/apps/websocket/gateway/router"
	"IM2/pkg/callstate"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/routing"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/google/uuid"
)

// ServiceContext 服务上下文
type ServiceContext struct {
	Config            config.Config
	ConnectionManager connection.Manager
	Router            *router.Router
	// Routes 集群路由表（Redis）：用户路由由本网关维护，群成员由 Group 服务维护、此处只读
	Routes *routing.Table
	// CallState 通话状态平面（Redis）：与路由表同一实例、不同键命名空间。
	// 是「这通电话是否还活着」的唯一权威，信令本身不落库。
	CallState *callstate.Store
	Nats      *nats_util.Client
	// Notifier 面向特定用户的投递器（路由聚合 + 广播兜底），
	// 供不绑定具体连接的后台流程使用（如通话超时收敛）
	Notifier     *nats_util.UserNotifier
	TokenManager *tokenmanager.TokenManager
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

	// 创建路由表 (Redis KV 存储，与 Message/Group 服务共享同一份路由数据)
	routes, err := routing.NewTableFromConf(c.RouteStore)
	if err != nil {
		log.Fatalf("init route table failed: %v", err)
	}

	// 通话状态平面（复用路由表的 Redis 实例，键前缀 call:* 与 ws:* 隔离）
	callStore, err := callstate.NewStoreFromConf(c.RouteStore)
	if err != nil {
		log.Fatalf("init call state store failed: %v", err)
	}

	// 创建 NATS 客户端 (跨节点消息转发 + JetStream 去重发布)
	natsClient, err := nats_util.NewClient(c.Nats.Url)
	if err != nil {
		log.Fatalf("connect to nats failed: %v", err)
	}

	// 创建路由器
	r := router.NewRouter(routes, natsClient, nodeID)

	// 创建连接管理器
	connMgr := connection.NewDefaultManager(nodeID)

	svc := &ServiceContext{
		Config:            c,
		ConnectionManager: connMgr,
		Router:            r,
		Routes:            routes,
		CallState:         callStore,
		Nats:              natsClient,
		Notifier:          nats_util.NewUserNotifier(natsClient, routes),
		TokenManager:      tokenmanager.NewTokenManager(c.TokenConfig),
	}

	return svc
}
