package svc

import (
	"IM2/internal/apps/User/rpc/config"
	"IM2/internal/apps/User/rpc/internal/service"
)

type ServiceContext struct {
	Config      config.Config
	UserService *service.UserService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:      c,
		UserService: service.NewUserService(c),
	}
}
