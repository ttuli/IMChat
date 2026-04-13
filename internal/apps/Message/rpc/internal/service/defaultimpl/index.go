package defaultimpl

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/pkg/redisx"

	"github.com/nats-io/nats.go"
)

// messageService 消息服务实现
type messageService struct {
	config.Config
	messageDAO      *dao.MessageDAO
	conversationDAO *dao.ConversationDAO
	js              nats.JetStreamContext
	redis           *redisx.Client
}

// NewMessageService 创建消息服务
func NewMessageService(c config.Config, js nats.JetStreamContext, msgDao *dao.MessageDAO, convDao *dao.ConversationDAO, redisClient *redisx.Client) service.MessageService {
	return &messageService{
		Config:          c,
		messageDAO:      msgDao,
		conversationDAO: convDao,
		js:              js,
		redis:           redisClient,
	}
}
