package handler

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/common"
	"IM2/pkg/logger"

	"github.com/google/uuid"
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
	default:
		logger.Infof("[MessageHandler] unknown message type: %v", msg.Type)
		return nil
	}
}

// handleChatMessage 处理单聊消息 (100-199)
func (h *MessageHandler) handleChatMessage(ctx context.Context, msg *common.WSMessage) error {
	// 根据具体类型解析 payload 中的 BaseMessage
	base, err := h.extractBaseMessage(msg)
	if err != nil {
		logger.Errorf("[MessageHandler] extract base message failed: %v", err)
		return nil
	}

	base.MsgId = uuid.New().String()
	seq, err := h.svcCtx.MessageDao.GetConversationMaxSeq(ctx, base.SessionId)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		// 返回带 ClientId 的错误消息，方便前端更新状态
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil
	}
	base.MsgSeq = int32(seq)
	base.Status = common.MessageStatus_MESSAGE_STATUS_SENT

	// 验证目标
	if base.Target == 0 {
		return nil
	}

	// 重新序列化 payload (带上服务端填充的字段)
	if err := h.repackPayload(base, msg); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil
	}

	if err := h.svcCtx.Router.RouteMsgToDB(ctx, msg); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil
	}
	msg.Payload, _ = proto.Marshal(base)

	// 发送给目标用户
	if err := h.svcCtx.ConnectionManager.SendToUser(ctx, base.Target, msg); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
	}

	ack := protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_SUCCESS)
	// 发送 ACK
	return h.conn.Send(ack)
}

// handleGroupMessage 处理群聊消息 (200-299)
func (h *MessageHandler) handleGroupMessage(ctx context.Context, msg *common.WSMessage) error {
	// 解析 BaseMessage 获取 group_id
	base, err := h.extractBaseMessage(msg)
	if err != nil {
		logger.Errorf("[MessageHandler] extract base message failed: %v", err)
		return nil
	}

	base.FromUserId = h.conn.UserID

	if base.Target == 0 {
		return nil
	}

	// TODO: 通过群成员列表广播
	logger.Infof("[MessageHandler] group message to group %d from user %d", base.Target, base.FromUserId)

	// 发送 ACK
	ack, _ := protocol.NewWSMessage(common.MessageType_MSG_ACK, nil)
	return h.conn.Send(ack)
}

// extractBaseMessage 从各类消息 payload 中提取 BaseMessage
// 所有聊天/群聊消息的第一个字段都是 BaseMessage
func (h *MessageHandler) extractBaseMessage(msg *common.WSMessage) (*common.BaseMessage, error) {
	switch msg.Type {
	case common.MessageType_CHAT_TEXT, common.MessageType_GROUP_TEXT:
		var textMsg common.TextMessage
		if err := proto.Unmarshal(msg.Payload, &textMsg); err != nil {
			return nil, err
		}
		return textMsg.Base, nil
	case common.MessageType_CHAT_IMAGE, common.MessageType_GROUP_IMAGE:
		var imgMsg common.ImageMessage
		if err := proto.Unmarshal(msg.Payload, &imgMsg); err != nil {
			return nil, err
		}
		return imgMsg.Base, nil
	case common.MessageType_CHAT_VIDEO, common.MessageType_GROUP_VIDEO:
		var videoMsg common.VideoMessage
		if err := proto.Unmarshal(msg.Payload, &videoMsg); err != nil {
			return nil, err
		}
		return videoMsg.Base, nil
	case common.MessageType_CHAT_AUDIO, common.MessageType_GROUP_AUDIO:
		var audioMsg common.AudioMessage
		if err := proto.Unmarshal(msg.Payload, &audioMsg); err != nil {
			return nil, err
		}
		return audioMsg.Base, nil
	case common.MessageType_CHAT_FILE, common.MessageType_GROUP_FILE:
		var fileMsg common.FileMessage
		if err := proto.Unmarshal(msg.Payload, &fileMsg); err != nil {
			return nil, err
		}
		return fileMsg.Base, nil
	case common.MessageType_CHAT_LOCATION:
		var locMsg common.LocationMessage
		if err := proto.Unmarshal(msg.Payload, &locMsg); err != nil {
			return nil, err
		}
		return locMsg.Base, nil
	case common.MessageType_CHAT_CUSTOM:
		var customMsg common.CustomMessage
		if err := proto.Unmarshal(msg.Payload, &customMsg); err != nil {
			return nil, err
		}
		return customMsg.Base, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %v", msg.Type)
	}
}

// repackPayload 重新序列化 payload (服务端填充字段后)
func (h *MessageHandler) repackPayload(base *common.BaseMessage, msg *common.WSMessage) error {
	// 反序列化 → 修改 → 重新序列化
	// 由于 extractBaseMessage 已经修改了 base 的指针内容，
	// 我们需要重新 marshal 完整的消息结构
	switch msg.Type {
	case common.MessageType_CHAT_TEXT, common.MessageType_GROUP_TEXT:
		var textMsg common.TextMessage
		if err := proto.Unmarshal(msg.Payload, &textMsg); err != nil {
			return err
		}
		textMsg.Base = base
		data, err := proto.Marshal(&textMsg)
		if err != nil {
			return err
		}
		msg.Payload = data
	// 其他类型类推，暂时只处理文本消息
	default:
		// 对于未处理的类型，payload 保持不变
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
