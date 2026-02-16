package defaultimpl

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/service"
)

// messageService 消息服务实现
type messageService struct {
	config.Config
	messageDAO *dao.MessageDAO
}

// NewMessageService 创建消息服务
func NewMessageService(c config.Config) service.MessageService {
	return &messageService{
		Config:     c,
		messageDAO: dao.NewMessageDAO(c.DAO.ConversationTable, c.Redis, c.Mongo.Uri),
	}
}
