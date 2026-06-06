package model

import "time"

// Conversation 会话表
type Conversation struct {
	ConversationID string    `gorm:"column:conversation_id;type:varchar(128);not null;uniqueIndex:uni_conv_id;comment:唯一会话标识" json:"conversation_id"`
	Type           int8      `gorm:"column:type;type:tinyint;not null;comment:会话类型: 1-单聊 2-群聊" json:"type"`
	LastContent    string    `gorm:"column:last_content;type:varchar(1024);not null;default:'';comment:最后一条消息内容" json:"last_content"`
	LastSender     uint64    `gorm:"column:last_sender;type:bigint unsigned;not null;default:0;comment:最后发送者ID" json:"last_sender"`
	MaxSeq         uint64    `gorm:"column:max_seq;type:bigint unsigned;not null;default:0;comment:最大消息序号" json:"max_seq"`
	CreateTime     time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime     time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

func (Conversation) TableName() string {
	return "conversation"
}

// 会话类型常量
const (
	ConvTypeSingle int8 = 1 // 单聊
	ConvTypeGroup  int8 = 2 // 群聊
)

// UserConversation 用户会话关系表
type UserConversation struct {
	UserID         uint64    `gorm:"column:user_id;type:bigint unsigned;not null;uniqueIndex:uni_user_conv;comment:用户ID" json:"user_id"`
	ConversationID string    `gorm:"column:conversation_id;type:varchar(128);not null;uniqueIndex:uni_user_conv;index:idx_conv_id;comment:会话标识" json:"conversation_id"`
	IsTop          int8      `gorm:"column:is_top;type:tinyint(1);not null;default:1;comment:是否置顶" json:"is_top"`
	IsDisturb      int8      `gorm:"column:is_disturb;type:tinyint(1);not null;default:1;comment:是否免打扰" json:"is_disturb"`
	LastReadSeq    uint64    `gorm:"column:last_read_seq;type:bigint unsigned;not null;default:0;comment:最后已读消息序号" json:"last_read_seq"`
	CreateTime     time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime     time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

const (
	Active   int8 = 2 // 活跃
	InActive int8 = 1 // 不活跃
)

func (UserConversation) TableName() string {
	return "user_conversation"
}

// ==================== Conversation 领域方法 ====================

// NewConversation 创建新的会话
func NewConversation(conversationID string, convType int8) *Conversation {
	now := time.Now()
	return &Conversation{
		ConversationID: conversationID,
		Type:           convType,
		CreateTime:     now,
		UpdateTime:     now,
	}
}

// IsSingle 判断是否为单聊
func (c *Conversation) IsSingle() bool {
	return c.Type == ConvTypeSingle
}

// IsGroup 判断是否为群聊
func (c *Conversation) IsGroup() bool {
	return c.Type == ConvTypeGroup
}

// UpdateLastMessage 更新最后一条消息状态
func (c *Conversation) UpdateLastMessage(content string, sender uint64, seq uint64, updateTime time.Time) {
	c.LastContent = content
	c.LastSender = sender
	c.MaxSeq = seq
	c.UpdateTime = updateTime
}

// ==================== UserConversation 领域方法 ====================

// NewUserConversation 创建用户会话关系
func NewUserConversation(userID uint64, conversationID string, maxSeq uint64) *UserConversation {
	now := time.Now()
	return &UserConversation{
		UserID:         userID,
		ConversationID: conversationID,
		IsTop:          1, // 默认 1
		IsDisturb:      1, // 默认 1
		LastReadSeq:    maxSeq,
		CreateTime:     now,
		UpdateTime:     now,
	}
}

// SetTop 设置置顶状态
func (u *UserConversation) SetTop(isTop int8) {
	u.IsTop = isTop
	u.UpdateTime = time.Now()
}

// SetDisturb 设置免打扰状态
func (u *UserConversation) SetDisturb(isDisturb int8) {
	u.IsDisturb = isDisturb
	u.UpdateTime = time.Now()
}

// ClearUnread 清除未读并更新已读序号
func (u *UserConversation) ClearUnread(lastReadSeq uint64) {
	u.LastReadSeq = lastReadSeq
	u.UpdateTime = time.Now()
}
