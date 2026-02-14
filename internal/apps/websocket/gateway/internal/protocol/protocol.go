package protocol

import (
	"encoding/json"
	"fmt"
)

// MessageType 消息类型
type MessageType int32

const (
	// 系统消息
	MessageTypeHeartbeat    MessageType = 0  // 心跳
	MessageTypeHeartbeatAck MessageType = 1  // 心跳响应

	// 聊天消息
	MessageTypeChat    MessageType = 100 // 聊天消息
	MessageTypeChatAck MessageType = 101 // 聊天消息确认
	MessageTypeRead    MessageType = 102 // 已读回执

	// 通知消息
	MessageTypeNotify MessageType = 200 // 通知

	// 错误消息
	MessageTypeError MessageType = 999 // 错误
)

// String 返回消息类型的字符串表示
func (m MessageType) String() string {
	switch m {
	case MessageTypeHeartbeat:
		return "Heartbeat"
	case MessageTypeHeartbeatAck:
		return "HeartbeatAck"
	case MessageTypeChat:
		return "Chat"
	case MessageTypeChatAck:
		return "ChatAck"
	case MessageTypeRead:
		return "Read"
	case MessageTypeNotify:
		return "Notify"
	case MessageTypeError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown(%d)", m)
	}
}

// Message WebSocket 消息结构
type Message struct {
	Type           MessageType     `json:"type"`                      // 消息类型
	ID             string          `json:"id,omitempty"`              // 消息ID(用于确认)
	Seq            int64           `json:"seq,omitempty"`             // 消息序列号(用于排序和断线重传)
	ConversationID string          `json:"conversation_id,omitempty"` // 会话ID
	From           uint64          `json:"from,omitempty"`            // 发送者ID
	To             uint64          `json:"to,omitempty"`              // 接收者ID
	ToList         []uint64        `json:"to_list,omitempty"`         // 接收者ID列表(用于广播)
	Timestamp      int64           `json:"timestamp,omitempty"`       // 时间戳
	Data           json.RawMessage `json:"data,omitempty"`            // 消息数据
	Extra          json.RawMessage `json:"extra,omitempty"`           // 扩展数据
}

// ChatData 聊天消息数据
type ChatData struct {
	ContentType int32  `json:"content_type"`
	Content     string `json:"content"`
}

// ReadData 已读回执数据
type ReadData struct {
	LastReadSeq int64 `json:"last_read_seq"` // 最后已读的消息序列号
}

// ErrorData 错误数据
type ErrorData struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// InternalMessage 内部消息(用于跨节点通信)
type InternalMessage struct {
	TargetUserID uint64  `json:"target_user_id"`
	Message      Message `json:"message"`
}
