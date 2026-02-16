package svc

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/internal/service/defaultimpl"
)

type ServiceContext struct {
	Config         config.Config
	MessageService service.MessageService
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:         c,
		MessageService: defaultimpl.NewMessageService(c),
	}
}
