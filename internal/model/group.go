package model

import "time"

type Group struct {
	ID          uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	OwnerID     uint64    `gorm:"column:owner_id;type:bigint unsigned;not null;comment:群主ID" json:"owner_id"`
	Name        string    `gorm:"column:name;type:varchar(255);not null;comment:群名称" json:"name"`
	Avatar      string    `gorm:"column:avatar;type:varchar(255);not null;comment:群头像" json:"avatar"`
	Notice      string    `gorm:"column:notice;type:varchar(1000);comment:群公告" json:"notice"`
	MemberCount int       `gorm:"column:member_count;type:int;not null;default:1;comment:群成员数" json:"member_count"`
	JoinType    int       `gorm:"column:join_type;type:int;not null;default:1;comment:加群方式" json:"join_type"`
	CreateTime  time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime  time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

func (Group) TableName() string {
	return "group"
}

// ==================== 领域方法 ====================

// NewGroup 创建新群组
func NewGroup(ownerID uint64, name, avatar, notice string, joinType int) *Group {
	now := time.Now()
	return &Group{
		OwnerID:     ownerID,
		Name:        name,
		Avatar:      avatar,
		Notice:      notice,
		MemberCount: 1, // 默认包含群主自己
		JoinType:    joinType,
		CreateTime:  now,
		UpdateTime:  now,
	}
}

// UpdateInfo 更新群基本信息
func (g *Group) UpdateInfo(name, avatar, notice string) {
	g.Name = name
	g.Avatar = avatar
	g.Notice = notice
	g.UpdateTime = time.Now()
}

// IncreMember 增加群成员数
func (g *Group) IncreMember(delta int) {
	g.MemberCount += delta
	g.UpdateTime = time.Now()
}
