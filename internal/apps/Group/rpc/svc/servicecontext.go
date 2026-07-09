package svc

import (
	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/dao"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/interceptor"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config      config.Config
	GroupDAO    *dao.GroupDAO
	ApplyDAO    *dao.ApplyDAO
	IdGenerator idgenclient.Idgen
	Nats        *nats.Conn
	Js          nats.JetStreamContext
}

func NewServiceContext(c config.Config) *ServiceContext {
	nc, err := nats.Connect(c.NATS.Url)
	if err != nil {
		panic(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}

	idGenerator := idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))

	return &ServiceContext{
		Config:      c,
		GroupDAO:    dao.NewGroupDAO(c.DAO.GroupDAO.DataSource, c.DAO.GroupDAO.RedisSource),
		ApplyDAO:    dao.NewApplyDAO(c.DAO.ApplyDAO),
		IdGenerator: idGenerator,
		Nats:        nc,
		Js:          js,
	}
}
