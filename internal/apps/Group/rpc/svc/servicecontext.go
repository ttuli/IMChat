package svc

import (
	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/dao"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/interceptor"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/routing"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config      config.Config
	GroupDAO    *dao.GroupDAO
	ApplyDAO    *dao.ApplyDAO
	IdGenerator idgenclient.Idgen
	Nats        *nats_util.Client
	// Routes 集群路由表：成员关系变更后同步直写群成员集合，
	// 替代旧 of USER_GROUP_SYNC NATS 广播（由网关维护本地映射）方案
	Routes *routing.Table
	// Notifier 群申请类通知（面向管理员/申请人）的精准投递器：
	// 按路由表查询目标所在网关节点后定向单播
	Notifier *nats_util.UserNotifier
}

func NewServiceContext(c config.Config) *ServiceContext {
	nc, err := nats_util.NewClient(c.NATS.Url)
	if err != nil {
		panic(err)
	}

	idGenerator := idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))

	// 路由表 Redis：未单独配置时复用 GroupDAO 的缓存实例
	routeConf := c.RouteStore
	if routeConf.Host == "" {
		routeConf = c.DAO.GroupDAO.RedisSource
	}
	routes, err := routing.NewTableFromConf(routeConf)
	if err != nil {
		panic(err)
	}

	return &ServiceContext{
		Config:      c,
		GroupDAO:    dao.NewGroupDAO(c.DAO.GroupDAO.DataSource, c.DAO.GroupDAO.RedisSource),
		ApplyDAO:    dao.NewApplyDAO(c.DAO.ApplyDAO),
		IdGenerator: idGenerator,
		Nats:        nc,
		Routes:      routes,
		Notifier:    nats_util.NewUserNotifier(nc, routes),
	}
}
