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

// ─────────────────────────────────────────────────────
// 消息类型注册表：用泛型消除 prepareMessage 的重复 switch-case
// ─────────────────────────────────────────────────────

// messageSpec 描述一种消息类型的反序列化规则
type messageSpec struct {
	// newMsg 创建该类型消息的空实例
	newMsg func() proto.Message
	// getBase 从反序列化后的消息中提取 BaseMessage 指针
	getBase func(proto.Message) *common.BaseMessage
	// getContentPreview 返回该消息类型的内容预览（用于会话列表展示）
	getContentPreview func(proto.Message) string
}

// msgSpecRegistry 消息类型 → 反序列化规则 的映射表
var msgSpecRegistry = map[common.MessageType]messageSpec{
	// ─── 文本消息 ───
	common.MessageType_CHAT_TEXT:  textSpec(),
	common.MessageType_GROUP_TEXT: textSpec(),
	// ─── 图片消息 ───
	common.MessageType_CHAT_IMAGE:  staticSpec(func() proto.Message { return &common.ImageMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.ImageMessage).Base }, "[图片]"),
	common.MessageType_GROUP_IMAGE: staticSpec(func() proto.Message { return &common.ImageMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.ImageMessage).Base }, "[图片]"),
	// ─── 视频消息 ───
	common.MessageType_CHAT_VIDEO:  staticSpec(func() proto.Message { return &common.VideoMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.VideoMessage).Base }, "[视频]"),
	common.MessageType_GROUP_VIDEO: staticSpec(func() proto.Message { return &common.VideoMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.VideoMessage).Base }, "[视频]"),
	// ─── 语音消息 ───
	common.MessageType_CHAT_AUDIO:  staticSpec(func() proto.Message { return &common.AudioMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.AudioMessage).Base }, "[语音]"),
	common.MessageType_GROUP_AUDIO: staticSpec(func() proto.Message { return &common.AudioMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.AudioMessage).Base }, "[语音]"),
	// ─── 文件消息 ───
	common.MessageType_CHAT_FILE:  staticSpec(func() proto.Message { return &common.FileMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.FileMessage).Base }, "[文件]"),
	common.MessageType_GROUP_FILE: staticSpec(func() proto.Message { return &common.FileMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.FileMessage).Base }, "[文件]"),
	// ─── 位置消息 ───
	common.MessageType_CHAT_LOCATION: staticSpec(func() proto.Message { return &common.LocationMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.LocationMessage).Base }, "[位置]"),
	// ─── 自定义消息 ───
	common.MessageType_CHAT_CUSTOM: staticSpec(func() proto.Message { return &common.CustomMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.CustomMessage).Base }, "[自定义]"),
}

// textSpec 文本消息的特殊处理：内容预览取实际文本
func textSpec() messageSpec {
	return messageSpec{
		newMsg:  func() proto.Message { return &common.TextMessage{} },
		getBase: func(m proto.Message) *common.BaseMessage { return m.(*common.TextMessage).Base },
		getContentPreview: func(m proto.Message) string {
			return m.(*common.TextMessage).Content
		},
	}
}

// staticSpec 构造固定预览内容的 messageSpec
func staticSpec(newMsg func() proto.Message, getBase func(proto.Message) *common.BaseMessage, preview string) messageSpec {
	return messageSpec{
		newMsg:            newMsg,
		getBase:           getBase,
		getContentPreview: func(proto.Message) string { return preview },
	}
}

// ─────────────────────────────────────────────────────
// processMessage 通用消息处理流程
// ─────────────────────────────────────────────────────

// processMessage 提取 base、填充服务端字段、重新打包、路由到 DB
// 返回 base 供调用方使用（如发送 ACK、转发等），error 非 nil 时已发送失败 ACK
func (h *MessageHandler) processMessage(ctx context.Context, msg *common.WSMessage) (*common.BaseMessage, error) {
	timeStamp := time.Now().UnixMilli()
	msg.Timestamp = timeStamp
	msg.SenderId = h.conn.UserID
	base, contentStr, repack, err := h.prepareMessage(ctx, msg)
	if err != nil {
		logger.Errorf("[MessageHandler] prepare message failed: %v", err)
		return nil, err
	}

	base.MsgId = uuid.New().String()
	base.SendTime = timeStamp
	base.FromUserId = h.conn.UserID

	// 验证目标
	if base.Target == 0 {
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, fmt.Errorf("target is empty")
	}

	// 仅递增 seq (整个 conversation update 交由 MQ 异步处理)
	seq, err := h.svcCtx.ConversationDao.IncrSeq(ctx, base.SessionId)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, err
	}
	base.MsgSeq = int32(seq)
	base.Status = common.MessageStatus_MESSAGE_STATUS_SENT

	updateSessionMsg := buildUpdateSession(base, contentStr, seq)

	if err := h.svcCtx.Router.BroadcastToAllNodes(ctx, updateSessionMsg); err != nil {
		logger.Errorf("[MessageHandler] failed to broadcast UpdateSession: %v", err)
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

func buildUpdateSession(base *common.BaseMessage, contentStr string, seq uint64) *common.WSMessage {
	session := base.SessionId
	var targetType common.TargetType
	if common.IsGroupSession(session) {
		targetType = common.TargetType_GROUP
	} else {
		targetType = common.TargetType_USER
	}
	
	u := &common.UpdateSession{
		TargetType:  targetType,
		TargetId:    base.Target,
		SessionId:   session,
		MaxSeq:      int64(seq),
		UpdateTime:  base.SendTime,
		Sender:      base.FromUserId,
		LastContent: contentStr,
	}
	payload, _ := proto.Marshal(u)
	return &common.WSMessage{
		Timestamp: base.SendTime,
		Type:      common.MessageType_UPDATE_SESSION,
		Payload:   payload,
	}
}

// ─────────────────────────────────────────────────────
// prepareMessage 通过注册表反序列化消息
// ─────────────────────────────────────────────────────

// prepareMessage 反序列化消息并返回 BaseMessage 指针和重新打包闭包
// 修改 base 字段后调用 repack() 即可将修改后的消息重新序列化到 msg.Payload
func (h *MessageHandler) prepareMessage(ctx context.Context, msg *common.WSMessage) (base *common.BaseMessage, contentStr string, repack func() error, err error) {
	spec, ok := msgSpecRegistry[msg.Type]
	if !ok {
		return nil, "", nil, fmt.Errorf("unsupported message type: %v", msg.Type)
	}

	// 反序列化
	m := spec.newMsg()
	if err := proto.Unmarshal(msg.Payload, m); err != nil {
		return nil, "", nil, err
	}

	return spec.getBase(m), spec.getContentPreview(m), func() error {
		data, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		msg.Payload = data
		return nil
	}, nil
}
