package model

import "time"

// GroupMember 群成员表
type GroupMember struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	GroupID   uint64    `gorm:"column:group_id;type:bigint unsigned;not null;index:idx_group_user,priority:1;index;comment:群组ID" json:"group_id"`
	UserID    uint64    `gorm:"column:user_id;type:bigint unsigned;not null;index:idx_group_user,priority:2;index;comment:用户ID" json:"user_id"`
	Role      int8      `gorm:"column:role;type:tinyint;not null;default:3;comment:角色: 1-群主, 2-管理员, 3-普通成员" json:"role"`
	Nickname  string    `gorm:"column:nickname;type:varchar(50);not null;default:'';comment:群内昵称" json:"nickname"`
	MuteUntil int64     `gorm:"column:mute_until;type:bigint;not null;default:0;comment:禁言截止时间（Unix时间戳，0表示未禁言）" json:"mute_until"`
	JoinedAt  time.Time `gorm:"column:joined_at;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:加入时间" json:"joined_at"`
	Extra     string    `gorm:"column:extra;type:json;comment:扩展字段(背景图、备注等)" json:"extra"`
}

func (GroupMember) TableName() string {
	return "group_member"
}

// 群成员角色常量
const (
	GroupRoleOwner  int8 = 1 // 群主
	GroupRoleAdmin  int8 = 2 // 管理员
	GroupRoleMember int8 = 3 // 普通成员
)
