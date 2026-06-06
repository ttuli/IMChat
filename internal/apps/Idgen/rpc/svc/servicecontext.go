package svc

import (
	"IM2/internal/apps/Idgen/rpc/config"
	"IM2/internal/apps/Idgen/rpc/internal/service"
)

type ServiceContext struct {
	Config    config.Config
	IDService *service.IDService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:    c,
		IDService: service.NewIDService(c),
	}
}
