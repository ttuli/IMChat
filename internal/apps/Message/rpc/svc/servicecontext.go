package svc

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/listener"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/internal/service/defaultimpl"
)

type ServiceContext struct {
	Config         config.Config
	MessageService service.MessageService
	ListenService  *listener.NatsListener
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:         c,
		MessageService: defaultimpl.NewMessageService(c),
		ListenService:  listener.NewNatsListener(c.Listener),
	}
}
