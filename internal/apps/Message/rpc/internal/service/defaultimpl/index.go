package defaultimpl

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/service"
)

// messageService 消息服务实现
type messageService struct {
	config.Config
	messageDAO      *dao.MessageDAO
	conversationDAO *dao.ConversationDAO
}

// NewMessageService 创建消息服务
func NewMessageService(c config.Config) service.MessageService {
	return &messageService{
		Config:          c,
		messageDAO:      dao.NewMessageDAO(c.Listener.DBAddress),
		conversationDAO: dao.NewConversationDAO(c.DAO.ConversationTable, c.Redisx),
	}
}
