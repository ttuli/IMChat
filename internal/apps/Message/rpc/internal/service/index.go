package service

import (
	"IM2/internal/apps/Message/rpc/svc"
)

// MessageService 消息服务实现
type MessageService struct {
	svcCtx *svc.ServiceContext
}

// NewMessageService 创建消息服务
func NewMessageService(svcCtx *svc.ServiceContext) *MessageService {
	return &MessageService{
		svcCtx: svcCtx,
	}
}

// GenerateMsgId 使用本地 SnowflakeNode 生成全局唯一消息 ID
func (s *MessageService) GenerateMsgId() string {
	return s.svcCtx.SnowflakeNode.Generate().String()
}
