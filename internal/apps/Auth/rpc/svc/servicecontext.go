package svc

import (
	"IM2/internal/apps/Auth/rpc/config"
	"IM2/internal/apps/Auth/rpc/internal/service"
)

type ServiceContext struct {
	Config      config.Config
	AuthService service.AuthService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:      c,
		AuthService: service.NewAuthServiceImpl(c),
	}
}
