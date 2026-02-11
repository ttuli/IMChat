package svc

import (
	"IM2/internal/apps/Group/rpc/config"
	extconfig "IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/service"
	"IM2/internal/apps/Group/rpc/internal/service/defaultimpl"
)

type ServiceContext struct {
	Config       config.Config
	GroupService service.GroupService
}

func NewServiceContext(c config.Config) *ServiceContext {
	// 构建外部 config 传递给 service
	extc := extconfig.Config{
		RpcServerConf: c.RpcServerConf,
	}
	extc.DAO.GroupDAO.DataSource = c.DAO.GroupDAO.DataSource
	extc.DAO.GroupDAO.RedisSource = c.DAO.GroupDAO.RedisSource
	extc.DAO.ApplyDAO = c.DAO.ApplyDAO
	extc.IDRpc = c.IDRpc

	return &ServiceContext{
		Config:       c,
		GroupService: defaultimpl.NewGroupService(extc),
	}
}
