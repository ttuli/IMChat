package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Message 消息表 (MongoDB)
type Message struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id"`                // MongoDB 默认主键
	MsgID          string             `bson:"msg_id" json:"msg_id"`                   // 客户端消息ID(幂等去重)
	ClientID       string             `bson:"client_id" json:"client_id"`             // 客户端ID
	ConversationID string             `bson:"conversation_id" json:"conversation_id"` // 会话ID
	FromUserID     uint64             `bson:"from_user_id" json:"from_user_id"`       // 发送者ID
	MsgType        int16              `bson:"msg_type" json:"msg_type"`               // 消息类型
	Seq            uint64             `bson:"seq" json:"seq"`                         // 消息序号(会话内递增)
	Content        string             `bson:"content" json:"content"`                 // 文本内容
	MediaURL       string             `bson:"media_url" json:"media_url"`             // 媒体文件URL
	Extra          map[string]any     `bson:"extra,omitempty" json:"extra,omitempty"` // 扩展字段
	Status         int8               `bson:"status" json:"status"`                   // 状态: 0-正常 1-撤回 2-删除
	CreateTime     time.Time          `bson:"create_time" json:"create_time"`         // 创建时间
}

// 消息状态常量
const (
	MsgStatusNormal   int8 = 0 // 正常
	MsgStatusRecalled int8 = 1 // 撤回
	MsgStatusDeleted  int8 = 2 // 删除
)

// ==================== 领域方法 ====================

// NewMessage 创建新消息
func NewMessage(msgID, clientID, conversationID string, fromUserID uint64, msgType int16, seq uint64, content string) *Message {
	return &Message{
		ID:             primitive.NewObjectID(),
		MsgID:          msgID,
		ClientID:       clientID,
		ConversationID: conversationID,
		FromUserID:     fromUserID,
		MsgType:        msgType,
		Seq:            seq,
		Content:        content,
		Status:         MsgStatusNormal,
		CreateTime:     time.Now(),
	}
}

// Recall 撤回消息
func (m *Message) Recall() {
	m.Status = MsgStatusRecalled
}

// Delete 删除消息
func (m *Message) Delete() {
	m.Status = MsgStatusDeleted
}

// IsRecalled 是否已撤回
func (m *Message) IsRecalled() bool {
	return m.Status == MsgStatusRecalled
}
