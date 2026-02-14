package model

import "time"

type GroupInvite struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	GroupID    uint64    `gorm:"column:group_id;type:bigint unsigned;not null;index:idx_group_invite_status;comment:群组ID" json:"group_id"`
	InviterID  uint64    `gorm:"column:inviter_id;type:bigint unsigned;not null;comment:邀请人ID(群成员)" json:"inviter_id"`
	InviteeID  uint64    `gorm:"column:invitee_id;type:bigint unsigned;not null;index:idx_invitee_invite_status;comment:被邀请人ID" json:"invitee_id"`
	Status     uint8     `gorm:"column:status;type:tinyint unsigned;not null;default:1;index:idx_group_invite_status;index:idx_invitee_invite_status;comment:状态(1:待处理 2:已接受 3:已拒绝)" json:"status"`
	InviteMsg  string    `gorm:"column:invite_msg;type:varchar(255);not null;default:'';comment:邀请语" json:"invite_msg"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

func (GroupInvite) TableName() string {
	return "group_invite"
}

// 群组邀请状态常量
const (
	GroupInviteStatusPending  uint8 = 1 // 待处理
	GroupInviteStatusAccepted uint8 = 2 // 已接受
	GroupInviteStatusRejected uint8 = 3 // 已拒绝
)
