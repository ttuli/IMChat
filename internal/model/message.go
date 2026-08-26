package model

import (
	"time"
	"IM2/pkg/proto/message"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Message 消息表 (MongoDB)
type Message struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`                // MongoDB 默认主键
	MsgID      string             `bson:"msg_id" json:"msg_id"`                   // 客户端消息ID(幂等去重)
	ClientID   string             `bson:"client_id" json:"client_id"`             // 客户端ID
	SessionID  string             `bson:"session_id" json:"session_id"`           // 会话ID
	SessionKey string             `bson:"session_key" json:"session_key"`         // 会话Key
	FromUserID uint64             `bson:"from_user_id" json:"from_user_id"`       // 发送者ID
	MsgType    int16              `bson:"msg_type" json:"msg_type"`               // 消息类型
	Seq        uint64             `bson:"seq" json:"seq"`                         // 消息序号(会话内递增)
	Content    string             `bson:"content" json:"content"`                 // 文本内容
	MediaURL   string             `bson:"media_url" json:"media_url"`             // 媒体文件URL
	Extra      map[string]any     `bson:"extra,omitempty" json:"extra,omitempty"` // 扩展字段
	Status     int8               `bson:"status" json:"status"`                   // 状态: 0-正常 1-撤回 2-删除
	CreateTime time.Time          `bson:"create_time" json:"create_time"`         // 创建时间
}

// MessageBucket 消息桶 (MongoDB)：同一会话的消息合并存放，一个文档承载 N 条（默认 100）。
//
// 相比「一条消息一个文档」，桶把文档数与 {session_id, seq} 索引项数都压缩 N 倍，
// 一次历史拉取通常只需读 1-2 个文档即可取满一页。
//
// 桶身份是 {session_id, min_seq}，不设自增桶号：桶号需要「先读最大号再加一」，
// 多实例并发写同一会话时只能靠唯一索引冲突重试来防重号；而 min_seq 由 $min 在
// upsert 时天然产生，无需预读。
//
// MinSeq/MaxSeq 如实反映桶内实际区间（用 $min/$max 维护，不假定写入顺序等于 seq
// 顺序）——跨实例并发消费同一会话时写入可能乱序，只要区间如实，读路径的区间相交
// 查询就不会漏桶。桶之间区间允许重叠，读路径按 seq 排序后合并即可。
type MessageBucket struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	SessionID  string             `bson:"session_id" json:"session_id"`   // 会话ID
	MinSeq     uint64             `bson:"min_seq" json:"min_seq"`         // 桶内最小 seq
	MaxSeq     uint64             `bson:"max_seq" json:"max_seq"`         // 桶内最大 seq
	Count      int32              `bson:"count" json:"count"`             // 桶内消息数，未满桶判定依据
	Messages   []*Message         `bson:"messages" json:"messages"`       // 内嵌消息，按 seq 升序
	CreateTime time.Time          `bson:"create_time" json:"create_time"` // 建桶时间
	UpdateTime time.Time          `bson:"update_time" json:"update_time"` // 最后追加时间
}

// 消息状态常量
// const (
// 	MsgStatusNormal   int8 = 0 // 正常
// 	MsgStatusRecalled int8 = 1 // 撤回
// 	MsgStatusDeleted  int8 = 2 // 删除
// )

// ==================== 领域方法 ====================

// NewMessage 创建新消息
func NewMessage(msgID, clientID, sessionID string, fromUserID uint64, msgType int16, seq uint64, content string) *Message {
	return &Message{
		ID:         primitive.NewObjectID(),
		MsgID:      msgID,
		ClientID:   clientID,
		SessionID:  sessionID,
		FromUserID: fromUserID,
		MsgType:    msgType,
		Seq:        seq,
		Content:    content,
		Status:     int8(message.MessageStatus_MESSAGE_STATUS_UNSPECIFIED),
		CreateTime: time.Now(),
	}
}

// Recall 撤回消息
func (m *Message) Recall() {
	m.Status = int8(message.MessageStatus_MESSAGE_STATUS_RECALLED)
}
// IsRecalled 是否已撤回
func (m *Message) IsRecalled() bool {
	return m.Status == int8(message.MessageStatus_MESSAGE_STATUS_RECALLED)
}
