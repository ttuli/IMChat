package service

import (
	"IM2/interceptor"
	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/dao"
	"IM2/internal/apps/Idgen/rpc/idgenclient"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/zrpc"
)

// GroupService 群组服务实现
type GroupService struct {
	config      config.Config
	groupDAO    *dao.GroupDAO
	applyDAO    *dao.ApplyDAO
	idGenerator idgenclient.Idgen
	nats        *nats.Conn
	js          nats.JetStreamContext
}

// NewGroupService 创建群组服务
func NewGroupService(c config.Config) *GroupService {
	nc, err := nats.Connect(c.NATS.Url)
	if err != nil {
		panic(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	return &GroupService{
		config:   c,
		groupDAO: dao.NewGroupDAO(c.DAO.GroupDAO.DataSource, c.DAO.GroupDAO.RedisSource),
		applyDAO: dao.NewApplyDAO(c.DAO.ApplyDAO),
		idGenerator: idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor))),
		nats: nc,
		js:   js,
	}
}
