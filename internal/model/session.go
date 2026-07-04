package model

import "time"

// Session 会话表
type Session struct {
	SessionID   string    `gorm:"column:session_id;type:varchar(128);not null;uniqueIndex:uni_session_id;comment:唯一会话标识" json:"session_id"`
	Type        int8      `gorm:"column:type;type:tinyint;not null;comment:会话类型" json:"type"`
	SessionKey  string    `gorm:"column:session_key;type:varchar(128);not null;default:'';comment:业务主键(群号或单聊双方ID拼接)" json:"session_key"`
	LastContent string    `gorm:"column:last_content;type:varchar(1024);not null;default:'';comment:最后一条消息内容" json:"last_content"`
	LastSender  uint64    `gorm:"column:last_sender;type:bigint unsigned;not null;default:0;comment:最后发送者ID" json:"last_sender"`
	MaxSeq      uint64    `gorm:"column:max_seq;type:bigint unsigned;not null;default:0;comment:最大消息序号（号段上限）" json:"max_seq"`
	ActualSeq   uint64    `gorm:"column:actual_seq;type:bigint unsigned;not null;default:0;comment:实际已分配的最大消息序号" json:"actual_seq"`
	CreateTime  time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime  time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

func (Session) TableName() string {
	return "session"
}

// 会话类型常量
const (
	SessionTypeSingle int8 = 1 // 单聊
	SessionTypeGroup  int8 = 2 // 群聊
)

// UserSession 用户会话关系表
type UserSession struct {
	UserID      uint64    `gorm:"column:user_id;type:bigint unsigned;not null;uniqueIndex:uni_user_session;comment:用户ID" json:"user_id"`
	SessionID   string    `gorm:"column:session_id;type:varchar(128);not null;uniqueIndex:uni_user_session;index:idx_session_id;comment:会话标识" json:"session_id"`
	IsTop       int8      `gorm:"column:is_top;type:tinyint(1);not null;default:1;comment:是否置顶" json:"is_top"`
	IsDisturb   int8      `gorm:"column:is_disturb;type:tinyint(1);not null;default:1;comment:是否免打扰" json:"is_disturb"`
	LastReadSeq uint64    `gorm:"column:last_read_seq;type:bigint unsigned;not null;default:0;comment:最后已读消息序号" json:"last_read_seq"`
	CreateTime  time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime  time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

const (
	Active   int8 = 2 // 活跃
	InActive int8 = 1 // 不活跃
)

func (UserSession) TableName() string {
	return "user_session"
}

// ==================== Session 领域方法 ====================

// NewSession 创建新的会话
func NewSession(sessionID string, sessionType int8) *Session {
	now := time.Now()
	return &Session{
		SessionID:  sessionID,
		Type:       sessionType,
		CreateTime: now,
		UpdateTime: now,
	}
}

// IsSingle 判断是否为单聊
func (c *Session) IsSingle() bool {
	return c.Type == SessionTypeSingle
}

// IsGroup 判断是否为群聊
func (c *Session) IsGroup() bool {
	return c.Type == SessionTypeGroup
}

// UpdateLastMessage 更新最后一条消息状态
func (c *Session) UpdateLastMessage(content string, sender uint64, seq uint64, updateTime time.Time) {
	c.LastContent = content
	c.LastSender = sender
	c.MaxSeq = seq
	c.UpdateTime = updateTime
}

// ==================== UserSession 领域方法 ====================

// NewUserSession 创建用户会话关系
func NewUserSession(userID uint64, sessionID string, maxSeq uint64) *UserSession {
	now := time.Now()
	return &UserSession{
		UserID:      userID,
		SessionID:   sessionID,
		IsTop:       1, // 默认 1
		IsDisturb:   1, // 默认 1
		LastReadSeq: maxSeq,
		CreateTime:  now,
		UpdateTime:  now,
	}
}

// SetTop 设置置顶状态
func (u *UserSession) SetTop(isTop int8) {
	u.IsTop = isTop
	u.UpdateTime = time.Now()
}

// SetDisturb 设置免打扰状态
func (u *UserSession) SetDisturb(isDisturb int8) {
	u.IsDisturb = isDisturb
	u.UpdateTime = time.Now()
}

// ClearUnread 清除未读并更新已读序号
func (u *UserSession) ClearUnread(lastReadSeq uint64) {
	u.LastReadSeq = lastReadSeq
	u.UpdateTime = time.Now()
}
