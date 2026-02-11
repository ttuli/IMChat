package svc

import (
	"IM2/internal/apps/User/rpc/config"
	"IM2/internal/apps/User/rpc/internal/service"
	"IM2/internal/apps/User/rpc/internal/service/defaultimpl"
)

type ServiceContext struct {
	Config      config.Config
	UserService service.UserService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:      c,
		UserService: defaultimpl.NewUserService(c),
	}
}
