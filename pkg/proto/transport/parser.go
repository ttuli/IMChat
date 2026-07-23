package transport

import (
	"IM2/pkg/proto/message"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// messageSpec 描述一种消息类型的反序列化规则
type messageSpec struct {
	// newMsg 创建该类型消息的空实例
	newMsg func() proto.Message
	// getBase 从反序列化后的消息中提取 BaseMessage 指针
	getBase func(proto.Message) *message.BaseMessage
	// getPreview 生成消息的概括内容/预览（用于会话列表显示）
	getPreview func(proto.Message) string
}

// msgSpecRegistry 消息类型 → 反序列化规则 的映射表
var msgSpecRegistry = map[MessageType]messageSpec{
	// ─── 文本消息 ───
	MessageType_CHAT_TEXT: {
		func() proto.Message { return &message.TextMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.TextMessage).Base },
		func(m proto.Message) string { return m.(*message.TextMessage).Content },
	},
	MessageType_GROUP_TEXT: {
		func() proto.Message { return &message.TextMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.TextMessage).Base },
		func(m proto.Message) string { return m.(*message.TextMessage).Content },
	},
	// ─── 图片消息 ───
	MessageType_CHAT_IMAGE: {
		func() proto.Message { return &message.ImageMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.ImageMessage).Base },
		func(proto.Message) string { return "[图片]" },
	},
	MessageType_GROUP_IMAGE: {
		func() proto.Message { return &message.ImageMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.ImageMessage).Base },
		func(proto.Message) string { return "[图片]" },
	},
	// ─── 视频消息 ───
	MessageType_CHAT_VIDEO: {
		func() proto.Message { return &message.VideoMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.VideoMessage).Base },
		func(proto.Message) string { return "[视频]" },
	},
	MessageType_GROUP_VIDEO: {
		func() proto.Message { return &message.VideoMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.VideoMessage).Base },
		func(proto.Message) string { return "[视频]" },
	},
	// ─── 语音消息 ───
	MessageType_CHAT_AUDIO: {
		func() proto.Message { return &message.AudioMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.AudioMessage).Base },
		func(proto.Message) string { return "[语音]" },
	},
	MessageType_GROUP_AUDIO: {
		func() proto.Message { return &message.AudioMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.AudioMessage).Base },
		func(proto.Message) string { return "[语音]" },
	},
	// ─── 文件消息 ───
	MessageType_CHAT_FILE: {
		func() proto.Message { return &message.FileMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.FileMessage).Base },
		func(proto.Message) string { return "[文件]" },
	},
	MessageType_GROUP_FILE: {
		func() proto.Message { return &message.FileMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.FileMessage).Base },
		func(proto.Message) string { return "[文件]" },
	},
	// ─── 位置消息 ───
	MessageType_CHAT_LOCATION: {
		func() proto.Message { return &message.LocationMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.LocationMessage).Base },
		func(proto.Message) string { return "[位置]" },
	},
	// ─── 自定义消息 ───
	MessageType_CHAT_CUSTOM: {
		func() proto.Message { return &message.CustomMessage{} },
		func(m proto.Message) *message.BaseMessage { return m.(*message.CustomMessage).Base },
		func(proto.Message) string { return "[自定义消息]" },
	},
}

func ParseMessage(msg *WSMessage) (
	base *message.BaseMessage,
	preview string,
	repack func() (*WSMessage, error),
	err error,
) {
	spec, ok := msgSpecRegistry[msg.Type]
	if !ok {
		return nil, "", nil, fmt.Errorf("unsupported message type: %v", msg.Type)
	}

	// 反序列化内层消息；m 持有 base 的指针，后续对 base 的修改会直接反映在 m 中
	m := spec.newMsg()
	if err := proto.Unmarshal(msg.Payload, m); err != nil {
		return nil, "", nil, err
	}

	// repack 闭包：捕获 m 与原始 msg，调用时将当前 m（base 已被外部修改）
	// 重新序列化为 Payload，并返回携带完整字段的新 WSMessage
	repackFn := func() (*WSMessage, error) {
		payload, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("repack marshal inner message: %w", err)
		}
		return &WSMessage{
			RouteTarget:     msg.RouteTarget,
			RouteTargetType: msg.RouteTargetType,
			Timestamp:       msg.Timestamp,
			Type:            msg.Type,
			Payload:         payload,
			SenderId:        msg.SenderId,
			Version:         msg.Version,
		}, nil
	}
	return spec.getBase(m), spec.getPreview(m), repackFn, nil
}