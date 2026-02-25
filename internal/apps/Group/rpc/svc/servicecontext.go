package svc

import (
	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/internal/service"
	"IM2/internal/apps/Group/rpc/internal/service/defaultimpl"
)

type ServiceContext struct {
	Config       config.Config
	GroupService service.GroupService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:       c,
		GroupService: defaultimpl.NewGroupService(c),
	}
}
