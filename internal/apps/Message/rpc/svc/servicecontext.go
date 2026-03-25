package svc

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/listener"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/internal/service/defaultimpl"

	"github.com/nats-io/nats.go"
)

type ServiceContext struct {
	Config          config.Config
	MessageService  service.MessageService
	ListenService   *listener.NatsListener
	NatsConn        *nats.Conn
	Js              nats.JetStreamContext
	MessageDAO      *dao.MessageDAO
	ConversationDAO *dao.ConversationDAO
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

	msgDao := dao.NewMessageDAO(c.DAO.MessageDAO.Dbsource)
	convDao := dao.NewConversationDAO(c.DAO.ConversationDAO.Dbsource, c.DAO.ConversationDAO.Redisx)

	return &ServiceContext{
		Config:          c,
		NatsConn:        conn,
		Js:              js,
		MessageService:  defaultimpl.NewMessageService(c, js, msgDao, convDao),
		ListenService:   listener.NewNatsListener(c, conn, js, msgDao, convDao),
		MessageDAO:      msgDao,
		ConversationDAO: convDao,
	}
}
