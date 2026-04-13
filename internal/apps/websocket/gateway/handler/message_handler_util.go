package handler

import (
	"fmt"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// ─────────────────────────────────────────────────────
// 消息类型注册表：消除 prepareMessage 的 switch-case
// ─────────────────────────────────────────────────────

// messageSpec 描述一种消息类型的反序列化规则
type messageSpec struct {
	// newMsg 创建该类型消息的空实例
	newMsg func() proto.Message
	// getBase 从反序列化后的消息中提取 BaseMessage 指针
	getBase func(proto.Message) *message.BaseMessage
	// getPreview 生成消息的概括内容/预览（用于会话列表显示）
	getPreview func(proto.Message) string
	// getMediaUrl 提取多媒体链接（非多媒体消息返回空）
	getMediaUrl func(proto.Message) string
}

// msgSpecRegistry 消息类型 → 反序列化规则 的映射表
var msgSpecRegistry = map[transport.MessageType]messageSpec{
	// ─── 文本消息 ───
	transport.MessageType_CHAT_TEXT:  {func() proto.Message { return &message.TextMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.TextMessage).Base }, func(m proto.Message) string { return m.(*message.TextMessage).Content }, func(proto.Message) string { return "" }},
	transport.MessageType_GROUP_TEXT: {func() proto.Message { return &message.TextMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.TextMessage).Base }, func(m proto.Message) string { return m.(*message.TextMessage).Content }, func(proto.Message) string { return "" }},
	// ─── 图片消息 ───
	transport.MessageType_CHAT_IMAGE:  {func() proto.Message { return &message.ImageMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.ImageMessage).Base }, func(proto.Message) string { return "[图片]" }, func(m proto.Message) string { return m.(*message.ImageMessage).Url }},
	transport.MessageType_GROUP_IMAGE: {func() proto.Message { return &message.ImageMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.ImageMessage).Base }, func(proto.Message) string { return "[图片]" }, func(m proto.Message) string { return m.(*message.ImageMessage).Url }},
	// ─── 视频消息 ───
	transport.MessageType_CHAT_VIDEO:  {func() proto.Message { return &message.VideoMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.VideoMessage).Base }, func(proto.Message) string { return "[视频]" }, func(m proto.Message) string { return m.(*message.VideoMessage).Url }},
	transport.MessageType_GROUP_VIDEO: {func() proto.Message { return &message.VideoMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.VideoMessage).Base }, func(proto.Message) string { return "[视频]" }, func(m proto.Message) string { return m.(*message.VideoMessage).Url }},
	// ─── 语音消息 ───
	transport.MessageType_CHAT_AUDIO:  {func() proto.Message { return &message.AudioMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.AudioMessage).Base }, func(proto.Message) string { return "[语音]" }, func(m proto.Message) string { return m.(*message.AudioMessage).Url }},
	transport.MessageType_GROUP_AUDIO: {func() proto.Message { return &message.AudioMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.AudioMessage).Base }, func(proto.Message) string { return "[语音]" }, func(m proto.Message) string { return m.(*message.AudioMessage).Url }},
	// ─── 文件消息 ───
	transport.MessageType_CHAT_FILE:  {func() proto.Message { return &message.FileMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.FileMessage).Base }, func(proto.Message) string { return "[文件]" }, func(m proto.Message) string { return m.(*message.FileMessage).Url }},
	transport.MessageType_GROUP_FILE: {func() proto.Message { return &message.FileMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.FileMessage).Base }, func(proto.Message) string { return "[文件]" }, func(m proto.Message) string { return m.(*message.FileMessage).Url }},
	// ─── 位置消息 ───
	transport.MessageType_CHAT_LOCATION: {func() proto.Message { return &message.LocationMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.LocationMessage).Base }, func(proto.Message) string { return "[位置]" }, func(proto.Message) string { return "" }},
	// ─── 自定义消息 ───
	transport.MessageType_CHAT_CUSTOM: {func() proto.Message { return &message.CustomMessage{} }, func(m proto.Message) *message.BaseMessage { return m.(*message.CustomMessage).Base }, func(proto.Message) string { return "[自定义消息]" }, func(proto.Message) string { return "" }},
}

// processMessage 提取 base、填充服务端字段、重新打包
// 返回 base、消息概览(preview)、多媒体URL(mediaUrl)供调用方使用（如发送到 MQ 入库），error 非 nil 时已发送失败 ACK
func (h *MessageHandler) processMessage(msg *transport.WSMessage) error {
	msg.SenderId = h.conn.UserID
	base, preview, mediaUrl, err := h.prepareMessage(msg)
	if err != nil {
		logger.Errorf("[MessageHandler] prepare message failed: %v", err)
		return err
	}

	// 验证目标
	if base.Target == 0 {
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return fmt.Errorf("target is empty")
	}

	// 网关层只负责生成 MsgId，seq 分配和 DB 落盘由后续 MQ 队列消费者异步完成
	base.FromUserId = h.conn.UserID
	base.MsgId = h.svcCtx.SnowflakeNode.Generate().String()
	base.SendTime = time.Now().UnixMilli()
	msg.Timestamp = base.SendTime
	base.Status = message.MessageStatus_MESSAGE_STATUS_SENDING

	msgSend := svc.MessageSend{
		MsgId:          base.MsgId,
		ClientId:       base.ClientId,
		ConversationId: base.SessionId,
		Sender:         base.FromUserId,
		Target:         base.Target,
		CreateTime:     base.SendTime,
		Content:        preview,
		MediaUrl:       mediaUrl,
		MsgType:        int32(msg.Type),
	}

	msgSendBytes, err := proto.Marshal(&msgSend)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return err
	}

	dedupKey := fmt.Sprintf("%d:%s", base.FromUserId, base.ClientId)
	if _, err := h.svcCtx.JetStream.Publish(
		h.svcCtx.Config.Nats.DBSubject,
		msgSendBytes,
		nats.MsgId(dedupKey),
	); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return err
	}

	h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_SUCCESS))
	return nil
}

// ─────────────────────────────────────────────────────
// prepareMessage 通过注册表反序列化消息
// ─────────────────────────────────────────────────────

// prepareMessage 反序列化消息并返回 BaseMessage 指针、预览文本、多媒体URL
func (h *MessageHandler) prepareMessage(msg *transport.WSMessage) (base *message.BaseMessage, preview string, mediaUrl string, err error) {
	spec, ok := msgSpecRegistry[msg.Type]
	if !ok {
		return nil, "", "", fmt.Errorf("unsupported message type: %v", msg.Type)
	}

	// 反序列化
	m := spec.newMsg()
	if err := proto.Unmarshal(msg.Payload, m); err != nil {
		return nil, "", "", err
	}

	return spec.getBase(m), spec.getPreview(m), spec.getMediaUrl(m), nil
}
