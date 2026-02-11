package handler

import (
	"context"

	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"

	"github.com/zeromicro/go-zero/core/logx"
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
func (h *MessageHandler) Handle(ctx context.Context, msg *protocol.Message) error {
	switch msg.Type {
	case protocol.MessageTypeHeartbeat:
		return h.handleHeartbeat(ctx, msg)
	case protocol.MessageTypeChat:
		return h.handleChat(ctx, msg)
	default:
		logx.Slowf("[MessageHandler] unknown message type: %s", msg.Type)
		return nil
	}
}

// handleHeartbeat 处理心跳消息
func (h *MessageHandler) handleHeartbeat(ctx context.Context, msg *protocol.Message) error {
	ack := protocol.NewHeartbeatAck()
	return h.conn.Send(ack)
}

// handleChat 处理聊天消息
func (h *MessageHandler) handleChat(ctx context.Context, msg *protocol.Message) error {
	// 设置发送者
	msg.From = h.conn.UserID

	// 验证消息
	if msg.To == 0 && len(msg.ToList) == 0 {
		return nil // 忽略无效消息
	}

	// 发送给单个用户
	if msg.To != 0 {
		if err := h.svcCtx.ConnectionManager.SendToUser(ctx, msg.To, msg); err != nil {
			logx.Errorf("[MessageHandler] send to user %d failed: %v", msg.To, err)
			// 发送失败不返回错误，可以通过消息确认机制处理
		}

		// 发送确认
		ack := &protocol.Message{
			Type: protocol.MessageTypeChatAck,
			ID:   msg.ID,
		}
		return h.conn.Send(ack)
	}

	// 广播给多个用户
	if len(msg.ToList) > 0 {
		if err := h.svcCtx.ConnectionManager.Broadcast(ctx, msg.ToList, msg); err != nil {
			logx.Errorf("[MessageHandler] broadcast failed: %v", err)
		}

		// 发送确认
		ack := &protocol.Message{
			Type: protocol.MessageTypeChatAck,
			ID:   msg.ID,
		}
		return h.conn.Send(ack)
	}

	return nil
}
