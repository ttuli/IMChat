package svc

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/listener"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/internal/service/defaultimpl"

	"github.com/nats-io/nats.go"
)

type ServiceContext struct {
	Config         config.Config
	MessageService service.MessageService
	ListenService  *listener.NatsListener
	NatsConn       *nats.Conn
	Js             nats.JetStreamContext
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn, err := nats.Connect(c.Listener.Url)
	if err != nil {
		panic(err)
	}
	js, err := conn.JetStream()
	if err != nil {
		panic(err)
	}

	return &ServiceContext{
		Config:         c,
		NatsConn:       conn,
		Js:             js,
		MessageService: defaultimpl.NewMessageService(c, js),
		ListenService:  listener.NewNatsListener(c, conn, js),
	}
}
