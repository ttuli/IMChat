package handler

import (
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"context"
)

// MessageHandler 消息处理器
type MessageHandler struct {
	svcCtx *svc.ServiceContext
	conn   *connection.Connection
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(svcCtx *svc.ServiceContext, conn *connection.Connection) *MessageHandler {
	return &MessageHandler{
		svcCtx: svcCtx,
		conn:   conn,
	}
}

// Handle 处理消息
func (h *MessageHandler) Handle(ctx context.Context, msg *transport.WSMessage) error {
	switch {
	case util.IsChatMessage(msg.Type):
		return h.processMessage(msg)
	default:
		logger.Infof("[MessageHandler] unknown message type: %v", msg.Type)
		return nil
	}
}
