package model

import "time"

// Conversation 会话表
type Conversation struct {
	ConversationID string    `gorm:"column:conversation_id;type:varchar(128);not null;uniqueIndex:uni_conv_id;comment:唯一会话标识" json:"conversation_id"`
	Type           int8      `gorm:"column:type;type:tinyint;not null;comment:会话类型: 1-单聊 2-群聊" json:"type"`
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
	IsTop          bool      `gorm:"column:is_top;type:tinyint(1);not null;default:0;comment:是否置顶" json:"is_top"`
	IsDisturb      bool      `gorm:"column:is_disturb;type:tinyint(1);not null;default:0;comment:是否免打扰" json:"is_disturb"`
	IsMute         bool      `gorm:"column:is_mute;type:tinyint(1);not null;default:0;comment:是否静音" json:"is_mute"`
	LastReadSeq    uint64    `gorm:"column:last_read_seq;type:bigint unsigned;not null;default:0;comment:最后已读消息序号" json:"last_read_seq"`
	CreateTime     time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime     time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

func (UserConversation) TableName() string {
	return "user_conversation"
}
