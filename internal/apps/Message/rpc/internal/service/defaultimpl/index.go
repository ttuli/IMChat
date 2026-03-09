package defaultimpl

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/service"

	"github.com/nats-io/nats.go"
)

// messageService 消息服务实现
type messageService struct {
	config.Config
	messageDAO      *dao.MessageDAO
	conversationDAO *dao.ConversationDAO
	js              nats.JetStreamContext
}

// NewMessageService 创建消息服务
func NewMessageService(c config.Config, js nats.JetStreamContext) service.MessageService {
	return &messageService{
		Config:          c,
		messageDAO:      dao.NewMessageDAO(c.DAO.MessageDAO.Dbsource),
		conversationDAO: dao.NewConversationDAO(c.DAO.ConversationDAO.Dbsource, c.DAO.ConversationDAO.Redisx),
		js:              js,
	}
}
