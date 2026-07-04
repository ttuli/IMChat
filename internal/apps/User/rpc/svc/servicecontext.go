package svc

import (
	"IM2/interceptor"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/apps/User/rpc/config"
	"IM2/internal/apps/User/rpc/internal/dao"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config         config.Config
	UserDAO        *dao.UserDAO
	FriendDAO      *dao.FriendDAO
	FriendApplyDAO *dao.FriendApplyDAO
	IdGenerator    idgenclient.Idgen
	NatsConn       *nats.Conn
	Js             nats.JetStreamContext
}

func NewServiceContext(c config.Config) *ServiceContext {
	nast, err := nats.Connect(c.NATS.Url)
	if err != nil {
		panic(err)
	}
	js, err := nast.JetStream()
	if err != nil {
		panic(err)
	}

	idGenerator := idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))

	return &ServiceContext{
		Config:         c,
		UserDAO:        dao.NewUserDAO(c.DAO.UserDAO.DataSource, c.DAO.UserDAO.RedisSource),
		FriendDAO:      dao.NewFriendDAO(c.DAO.FriendDAO),
		FriendApplyDAO: dao.NewFriendApplyDAO(c.DAO.FriendApplyDAO),
		IdGenerator:    idGenerator,
		NatsConn:       nast,
		Js:             js,
	}
}
 