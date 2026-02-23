package handler

import (
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/common"
	"IM2/pkg/logger"
	"context"

	"google.golang.org/protobuf/proto"
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
	case common.IsGroupMessage(msg.Type):
		return h.handleGroupMessage(ctx, msg)
	case common.IsNotifyMessage(msg.Type):
		return h.handleNotifyMessage(ctx, msg)
	default:
		logger.Infof("[MessageHandler] unknown message type: %v", msg.Type)
		return nil
	}
}

// handleChatMessage 处理单聊消息 (100-199)
func (h *MessageHandler) handleChatMessage(ctx context.Context, msg *common.WSMessage) error {
	base, err := h.processMessage(ctx, msg)
	if err != nil {
		return nil
	}
	// 发送给目标用户
	if err := h.svcCtx.ConnectionManager.SendToUser(ctx, base.Target, msg); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
	}

	// 发送 ACK
	return h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_SUCCESS))
}

// handleGroupMessage 处理群聊消息 (200-299)
func (h *MessageHandler) handleGroupMessage(ctx context.Context, msg *common.WSMessage) error {
	base, err := h.processMessage(ctx, msg)
	if err != nil {
		return nil
	}

	// TODO: 通过群成员列表广播
	logger.Infof("[MessageHandler] group message to group %d from user %d", base.Target, base.FromUserId)

	// 发送 ACK
	return h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_SUCCESS))
}

func (h *MessageHandler) handleNotifyMessage(ctx context.Context, msg *common.WSMessage) error {
	switch msg.Type {
	case common.MessageType_FRIEND_REQUEST:
		var content common.FriendRequest
		err := proto.Unmarshal(msg.Payload, &content)
		if err != nil {
			return err
		}
		h.svcCtx.ConnectionManager.SendToUser(ctx, content.ToUserId, msg)
	case common.MessageType_FRIEND_ADD:
		var content common.Friend
		err := proto.Unmarshal(msg.Payload, &content)
		if err != nil {
			return err
		}
		h.svcCtx.ConnectionManager.SendToUser(ctx, content.FriendId, msg)
	case common.MessageType_GROUP_REQUEST:
		var content common.GroupApply
		err := proto.Unmarshal(msg.Payload, &content)
		if err != nil {
			return err
		}
		h.svcCtx.ConnectionManager.SendToGroup(ctx, content.GroupId, msg)
	default:
		return nil
	}
	return nil
}

func (h *MessageHandler) sendError(err *common.ErrorMessage) {
	if err == nil {
		return
	}
	resp, _ := protocol.NewWSMessage(common.MessageType_ERROR, err)
	h.conn.Send(resp)
}
