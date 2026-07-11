package svc

import (
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/apps/User/rpc/config"
	"IM2/internal/apps/User/rpc/internal/dao"
	"IM2/internal/interceptor"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/routing"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config         config.Config
	UserDAO        *dao.UserDAO
	FriendDAO      *dao.FriendDAO
	FriendApplyDAO *dao.FriendApplyDAO
	IdGenerator    idgenclient.Idgen
	Nats           *nats_util.Client
	// Notifier 好友类通知的精准投递器：按集群路由表查询目标所在网关节点后定向单播
	Notifier *nats_util.UserNotifier
}

func NewServiceContext(c config.Config) *ServiceContext {
	nc, err := nats_util.NewClient(c.NATS.Url)
	if err != nil {
		panic(err)
	}

	idGenerator := idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))

	// 路由表 Redis：未单独配置时复用 UserDAO 的缓存实例
	routeConf := c.RouteStore
	if routeConf.Host == "" {
		routeConf = c.DAO.UserDAO.RedisSource
	}
	routes, err := routing.NewTableFromConf(routeConf)
	if err != nil {
		panic(err)
	}

	return &ServiceContext{
		Config:         c,
		UserDAO:        dao.NewUserDAO(c.DAO.UserDAO.DataSource, c.DAO.UserDAO.RedisSource),
		FriendDAO:      dao.NewFriendDAO(c.DAO.FriendDAO),
		FriendApplyDAO: dao.NewFriendApplyDAO(c.DAO.FriendApplyDAO),
		IdGenerator:    idGenerator,
		Nats:           nc,
		Notifier:       nats_util.NewUserNotifier(nc, routes),
	}
}
