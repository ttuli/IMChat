package dao

import (
	"context"

	model "IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type InviteDAO struct {
	*gorm.DB
}

func NewInviteDAO(dbSource string) *InviteDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	if err := db.AutoMigrate(&model.GroupInvite{}); err != nil {
		panic(err)
	}

	return &InviteDAO{DB: db}
}

// InsertInvites 批量创建群邀请（单次 SQL，避免逐条事务累积超时）
func (m *InviteDAO) InsertInvites(ctx context.Context, invites []*model.GroupInvite) error {
	if len(invites) == 0 {
		return nil
	}
	return m.DB.WithContext(ctx).Create(&invites).Error
}

// FindInviteByID 根据ID查询邀请
func (m *InviteDAO) FindInviteByID(ctx context.Context, id uint64) (*model.GroupInvite, error) {
	var invite model.GroupInvite
	if err := m.DB.WithContext(ctx).Where("id = ?", id).First(&invite).Error; err != nil {
		return nil, err
	}
	return &invite, nil
}

// FindExistingPendingInvite 检查是否已存在针对同一 (group, invitee) 的待处理邀请
func (m *InviteDAO) FindExistingPendingInvite(ctx context.Context, groupID, inviteeID uint64) (*model.GroupInvite, error) {
	var invite model.GroupInvite
	if err := m.DB.WithContext(ctx).
		Where("group_id = ? AND invitee_id = ? AND status = ?", groupID, inviteeID, model.GroupInviteStatusPending).
		First(&invite).Error; err != nil {
		return nil, err
	}
	return &invite, nil
}

// FindPendingInvitesByInviteeID 查询被邀请人所有待处理邀请（收件箱）
func (m *InviteDAO) FindPendingInvitesByInviteeID(ctx context.Context, inviteeID uint64) ([]*model.GroupInvite, error) {
	var invites []*model.GroupInvite
	if err := m.DB.WithContext(ctx).
		Where("invitee_id = ? AND status = ?", inviteeID, model.GroupInviteStatusPending).
		Order("create_time DESC").
		Find(&invites).Error; err != nil {
		return nil, err
	}
	return invites, nil
}

// UpdateInviteStatus 更新邀请状态（接受/拒绝）
func (m *InviteDAO) UpdateInviteStatus(ctx context.Context, id uint64, status uint8) error {
	return m.DB.WithContext(ctx).Model(&model.GroupInvite{}).
		Where("id = ?", id).
		Update("status", status).Error
}
