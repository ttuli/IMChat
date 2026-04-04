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

func (h *MessageHandler) handleChatMessage(ctx context.Context, msg *common.WSMessage) error {
	base, err := h.processMessage(ctx, msg)
	if err != nil {
		return err
	}
	switch msg.RouteTargetType {
	case common.TargetType_USER:
		for _, target := range msg.RouteTarget {
			if err := h.svcCtx.ConnectionManager.SendToUser(ctx, target, msg); err != nil {
				h.svcCtx.TelemetryBus.Publish(err)
			}
		}
	case common.TargetType_GROUP:
		for _, target := range msg.RouteTarget {
			if err := h.svcCtx.ConnectionManager.SendToGroup(ctx, target, msg); err != nil {
				h.svcCtx.TelemetryBus.Publish(err)
			}
		}
	default:
		return nil
	}

	// 发送 ACK
	return h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_SUCCESS))
}
