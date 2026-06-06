package model

import "time"

// FriendApply 好友申请记录表
type FriendApply struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	FromUserID   uint64    `gorm:"column:from_user_id;type:bigint unsigned;not null;index:idx_from_user;comment:申请发起人ID" json:"from_user_id"`
	ToUserID     uint64    `gorm:"column:to_user_id;type:bigint unsigned;not null;index:idx_to_user_status;comment:申请接收人ID" json:"to_user_id"`
	ApplyMsg     string    `gorm:"column:apply_msg;type:varchar(255);not null;default:'';comment:申请理由/验证消息" json:"apply_msg"`
	Status       uint8     `gorm:"column:status;type:tinyint unsigned;not null;default:1;index:idx_to_user_status;comment:状态: 1-待处理, 2-已同意, 3-已拒绝, 4-已忽略" json:"status"`
	Source       uint8     `gorm:"column:source;type:tinyint unsigned;not null;default:1;comment:来源" json:"source"`
	CreateTime   time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:申请时间" json:"create_time"`
	HandleTime   time.Time `gorm:"column:handle_time;type:datetime(3);not null;autoUpdateTime;comment:处理时间" json:"handle_time"`
	RejectReason string    `gorm:"column:reject_reason;type:varchar(255);not null;default:'';comment:拒绝原因" json:"reject_reason"`
}

// TableName 指定表名
func (FriendApply) TableName() string {
	return "friend_apply"
}

// 好友申请状态常量
const (
	ApplyStatusPending  uint8 = 1 // 待处理
	ApplyStatusAccepted uint8 = 2 // 已同意
	ApplyStatusRejected uint8 = 3 // 已拒绝
	ApplyStatusIgnored  uint8 = 4 // 已忽略
)

// 好友申请来源常量
const (
	ApplySourceSearchAccount uint8 = 1 // 搜索账号
	ApplySourceSearchPhone   uint8 = 2 // 搜索手机号
	ApplySourceSearchName    uint8 = 3 // 搜索名字
	ApplySourceGroup         uint8 = 4 // 群聊
	ApplySourceRecommend     uint8 = 5 // 推荐
)

// ==================== 领域方法 ====================

// NewFriendApply 创建新的好友申请
func NewFriendApply(fromUserID, toUserID uint64, msg string, source uint8) *FriendApply {
	now := time.Now()
	return &FriendApply{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		ApplyMsg:   msg,
		Status:     ApplyStatusPending,
		Source:     source,
		CreateTime: now,
		HandleTime: now,
	}
}

// Accept 同意好友申请
func (f *FriendApply) Accept() {
	f.Status = ApplyStatusAccepted
	f.HandleTime = time.Now()
}

// Reject 拒绝好友申请
func (f *FriendApply) Reject(reason string) {
	f.Status = ApplyStatusRejected
	f.RejectReason = reason
	f.HandleTime = time.Now()
}

// Ignore 忽略好友申请
func (f *FriendApply) Ignore() {
	f.Status = ApplyStatusIgnored
	f.HandleTime = time.Now()
}

// IsPending 判断是否为待处理状态
func (f *FriendApply) IsPending() bool {
	return f.Status == ApplyStatusPending
}
