package handler

import (
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/common"
	"IM2/pkg/logger"
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
func (h *MessageHandler) Handle(ctx context.Context, msg *common.WSMessage) error {
	switch {
	case common.IsChatMessage(msg.Type):
		return h.handleChatMessage(ctx, msg)
	default:
		logger.Infof("[MessageHandler] unknown message type: %v", msg.Type)
		return nil
	}
}

// handleChatMessage 处理单聊消息 (100-199)
func (h *MessageHandler) handleChatMessage(ctx context.Context, msg *common.WSMessage) error {
	base, err := h.processMessage(ctx, msg)
	if err != nil {
		return err
	}
	switch msg.RouteTargetType {
	case common.TargetType_USER:
		if err := h.svcCtx.ConnectionManager.SendToUser(ctx, msg.RouteTarget, msg); err != nil {
			h.svcCtx.TelemetryBus.Publish(err)
		}
	case common.TargetType_GROUP:
		if err := h.svcCtx.ConnectionManager.SendToGroup(ctx, msg.RouteTarget, msg); err != nil {
			h.svcCtx.TelemetryBus.Publish(err)
		}
	default:
		return nil
	}

	// 发送 ACK
	return h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_SUCCESS))
}
