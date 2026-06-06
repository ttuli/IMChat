package model

import "time"

type GroupApply struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	FromUserID uint64    `gorm:"column:from_user_id;type:bigint unsigned;not null;index:idx_from_user;comment:申请发起人ID" json:"from_user_id"`
	GroupID    uint64    `gorm:"column:group_id;type:bigint unsigned;not null;index:idx_group_status;comment:群组ID" json:"group_id"`
	ApplyMsg   string    `gorm:"column:apply_msg;type:varchar(255);not null;default:'';comment:申请理由/验证消息" json:"apply_msg"`
	Status     uint8     `gorm:"column:status;type:tinyint unsigned;not null;default:1;index:idx_group_status;comment:状态" json:"status"`
	HandlerID  uint64    `gorm:"column:handler_id;type:bigint unsigned;default:null;comment:处理人ID（群主或管理员）" json:"handler_id"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:申请时间" json:"create_time"`
	UpdateTime time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:处理时间" json:"update_time"`
}

func (GroupApply) TableName() string {
	return "group_apply"
}

// 群组申请状态常量
const (
	GroupApplyStatusPending  uint8 = 1 // 待处理
	GroupApplyStatusAccepted uint8 = 2 // 已同意
	GroupApplyStatusRejected uint8 = 3 // 已拒绝
	GroupApplyStatusIgnored  uint8 = 4 // 已忽略
)

// ==================== 领域方法 ====================

// NewGroupApply 创建群组申请
func NewGroupApply(fromUserID, groupID uint64, msg string) *GroupApply {
	now := time.Now()
	return &GroupApply{
		FromUserID: fromUserID,
		GroupID:    groupID,
		ApplyMsg:   msg,
		Status:     GroupApplyStatusPending,
		CreateTime: now,
		UpdateTime: now,
	}
}

// Accept 接受群申请
func (g *GroupApply) Accept(handlerID uint64) {
	g.Status = GroupApplyStatusAccepted
	g.HandlerID = handlerID
	g.UpdateTime = time.Now()
}

// Reject 拒绝群申请
func (g *GroupApply) Reject(handlerID uint64) {
	g.Status = GroupApplyStatusRejected
	g.HandlerID = handlerID
	g.UpdateTime = time.Now()
}

// Ignore 忽略群申请
func (g *GroupApply) Ignore(handlerID uint64) {
	g.Status = GroupApplyStatusIgnored
	g.HandlerID = handlerID
	g.UpdateTime = time.Now()
}

// IsPending 是否为待处理状态
func (g *GroupApply) IsPending() bool {
	return g.Status == GroupApplyStatusPending
}
