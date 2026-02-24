package handler

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/common"
	"IM2/pkg/logger"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// processMessage 通用消息处理流程：提取 base、填充服务端字段、重新打包、路由到 DB
// 返回 base 供调用方使用（如发送 ACK、转发等），error 非 nil 时已发送失败 ACK
func (h *MessageHandler) processMessage(ctx context.Context, msg *common.WSMessage) (*common.BaseMessage, error) {
	timeStamp := time.Now().UnixMilli()
	msg.Timestamp = timeStamp
	base, repack, err := h.prepareMessage(msg)
	if err != nil {
		logger.Errorf("[MessageHandler] prepare message failed: %v", err)
		return nil, err
	}

	// 填充服务端字段
	base.MsgId = uuid.New().String()
	base.SendTime = timeStamp
	base.FromUserId = h.conn.UserID
	seq, err := h.svcCtx.MessageDao.IncrConversationSeq(ctx, base.SessionId)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, err
	}
	base.MsgSeq = int32(seq)
	base.Status = common.MessageStatus_MESSAGE_STATUS_SENT

	// 验证目标
	if base.Target == 0 {
		return nil, fmt.Errorf("target is empty")
	}

	// 重新序列化（base 已被原地修改，只需一次 Marshal）
	if err := repack(); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, err
	}

	// 路由到 DB（通过 NATS）
	if err := h.svcCtx.Router.RouteMsgToDB(ctx, msg); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, err
	}

	return base, nil
}

// prepareMessage 反序列化消息并返回 BaseMessage 指针和重新打包闭包
// 修改 base 字段后调用 repack() 即可将修改后的消息重新序列化到 msg.Payload
func (h *MessageHandler) prepareMessage(msg *common.WSMessage) (base *common.BaseMessage, repack func() error, err error) {
	switch msg.Type {
	case common.MessageType_CHAT_TEXT, common.MessageType_GROUP_TEXT:
		var m common.TextMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_IMAGE, common.MessageType_GROUP_IMAGE:
		var m common.ImageMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_VIDEO, common.MessageType_GROUP_VIDEO:
		var m common.VideoMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_AUDIO, common.MessageType_GROUP_AUDIO:
		var m common.AudioMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_FILE, common.MessageType_GROUP_FILE:
		var m common.FileMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_LOCATION:
		var m common.LocationMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	case common.MessageType_CHAT_CUSTOM:
		var m common.CustomMessage
		if err := proto.Unmarshal(msg.Payload, &m); err != nil {
			return nil, nil, err
		}
		return m.Base, func() error {
			data, err := proto.Marshal(&m)
			if err != nil {
				return err
			}
			msg.Payload = data
			return nil
		}, nil

	default:
		return nil, nil, fmt.Errorf("unsupported message type: %v", msg.Type)
	}
}
