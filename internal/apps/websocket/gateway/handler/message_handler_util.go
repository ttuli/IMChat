package handler

import (
	"context"
	"fmt"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/common"
	"IM2/pkg/logger"

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
	// getContent 提取文本内容（非文本消息返回空）
	getContent func(proto.Message) string
	// getMediaUrl 提取多媒体链接（非媒体消息返回空）
	getMediaUrl func(proto.Message) string
}

// msgSpecRegistry 消息类型 → 反序列化规则 的映射表
var msgSpecRegistry = map[common.MessageType]messageSpec{
	// ─── 文本消息 ───
	common.MessageType_CHAT_TEXT:  textSpec(),
	common.MessageType_GROUP_TEXT: textSpec(),
	// ─── 图片消息 ───
	common.MessageType_CHAT_IMAGE:  staticSpec(func() proto.Message { return &common.ImageMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.ImageMessage).Base }, func(m proto.Message) string { return m.(*common.ImageMessage).Url }),
	common.MessageType_GROUP_IMAGE: staticSpec(func() proto.Message { return &common.ImageMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.ImageMessage).Base }, func(m proto.Message) string { return m.(*common.ImageMessage).Url }),
	// ─── 视频消息 ───
	common.MessageType_CHAT_VIDEO:  staticSpec(func() proto.Message { return &common.VideoMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.VideoMessage).Base }, func(m proto.Message) string { return m.(*common.VideoMessage).Url }),
	common.MessageType_GROUP_VIDEO: staticSpec(func() proto.Message { return &common.VideoMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.VideoMessage).Base }, func(m proto.Message) string { return m.(*common.VideoMessage).Url }),
	// ─── 语音消息 ───
	common.MessageType_CHAT_AUDIO:  staticSpec(func() proto.Message { return &common.AudioMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.AudioMessage).Base }, func(m proto.Message) string { return m.(*common.AudioMessage).Url }),
	common.MessageType_GROUP_AUDIO: staticSpec(func() proto.Message { return &common.AudioMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.AudioMessage).Base }, func(m proto.Message) string { return m.(*common.AudioMessage).Url }),
	// ─── 文件消息 ───
	common.MessageType_CHAT_FILE:  staticSpec(func() proto.Message { return &common.FileMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.FileMessage).Base }, func(m proto.Message) string { return m.(*common.FileMessage).Url }),
	common.MessageType_GROUP_FILE: staticSpec(func() proto.Message { return &common.FileMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.FileMessage).Base }, func(m proto.Message) string { return m.(*common.FileMessage).Url }),
	// ─── 位置消息 ───
	common.MessageType_CHAT_LOCATION: staticSpec(func() proto.Message { return &common.LocationMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.LocationMessage).Base }, func(m proto.Message) string { return "" }),
	// ─── 自定义消息 ───
	common.MessageType_CHAT_CUSTOM: staticSpec(func() proto.Message { return &common.CustomMessage{} }, func(m proto.Message) *common.BaseMessage { return m.(*common.CustomMessage).Base }, func(m proto.Message) string { return "" }),
}

// textSpec 文本消息的特殊处理：提取实际文本
func textSpec() messageSpec {
	return messageSpec{
		newMsg:  func() proto.Message { return &common.TextMessage{} },
		getBase: func(m proto.Message) *common.BaseMessage { return m.(*common.TextMessage).Base },
		getContent: func(m proto.Message) string {
			return m.(*common.TextMessage).Content
		},
		getMediaUrl: func(m proto.Message) string { return "" },
	}
}

// staticSpec 构造仅包含媒体等非文本信息的 messageSpec
func staticSpec(newMsg func() proto.Message, getBase func(proto.Message) *common.BaseMessage, getUrl func(proto.Message) string) messageSpec {
	return messageSpec{
		newMsg:      newMsg,
		getBase:     getBase,
		getContent:  func(proto.Message) string { return "" }, // 非文本消息 Content 为空
		getMediaUrl: getUrl,
	}
}

// processMessage 提取 base、填充服务端字段、重新打包、路由到 DB
// 返回 base 供调用方使用（如发送 ACK、转发等），error 非 nil 时已发送失败 ACK
func (h *MessageHandler) processMessage(ctx context.Context, msg *common.WSMessage) (*common.BaseMessage, error) {
	msg.SenderId = h.conn.UserID
	base, contentStr, mediaUrl, repack, err := h.prepareMessage(msg)
	if err != nil {
		logger.Errorf("[MessageHandler] prepare message failed: %v", err)
		return nil, err
	}

	base.FromUserId = h.conn.UserID

	// 验证目标
	if base.Target == 0 {
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, fmt.Errorf("target is empty")
	}

	// 调用 RPC 处理：seq递增、MsgId生成、DB落盘、广播UpdateSession
	rpcReq := &message.SendMessageReq{
		Message: &message.Message{
			ClientId:       base.ClientId,
			ConversationId: base.SessionId,
			FromUserId:     base.FromUserId,
			MsgType:        int32(msg.Type),
			Content:        contentStr,
			MediaUrl:       mediaUrl,
		},
	}

	rpcResp, err := h.svcCtx.MessageRpc.SendMessage(ctx, rpcReq)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, fmt.Errorf("rpc send message failed: %w", err)
	}

	// RPC 处理成功，将处理后的 Seq、MsgId、SendTime 写回本地 base，重新打包以便推给对端
	respMsg := rpcResp.Message
	base.MsgSeq = int32(respMsg.Seq)
	base.MsgId = respMsg.MsgId
	base.SendTime = respMsg.CreateTime
	base.Status = common.MessageStatus_MESSAGE_STATUS_SENT
	msg.Timestamp = respMsg.CreateTime

	// 重新序列化（base 已被原地修改，只需一次 Marshal）
	if err := repack(); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, common.AckStatus_ACK_STATUS_FAILED))
		return nil, err
	}

	return base, nil
}

// ─────────────────────────────────────────────────────
// prepareMessage 通过注册表反序列化消息
// ─────────────────────────────────────────────────────

// prepareMessage 反序列化消息并返回 BaseMessage 指针、文案预览、多媒体URL和重新打包闭包
// 修改 base 字段后调用 repack() 即可将修改后的消息重新序列化到 msg.Payload
func (h *MessageHandler) prepareMessage(msg *common.WSMessage) (base *common.BaseMessage, contentStr string, mediaUrl string, repack func() error, err error) {
	spec, ok := msgSpecRegistry[msg.Type]
	if !ok {
		return nil, "", "", nil, fmt.Errorf("unsupported message type: %v", msg.Type)
	}

	// 反序列化
	m := spec.newMsg()
	if err := proto.Unmarshal(msg.Payload, m); err != nil {
		return nil, "", "", nil, err
	}

	return spec.getBase(m), spec.getContent(m), spec.getMediaUrl(m), func() error {
		data, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		msg.Payload = data
		return nil
	}, nil
}
