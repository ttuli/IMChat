package dao

import (
	"context"

	"IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type ApplyDAO struct {
	*gorm.DB
}

func NewApplyDAO(dbSource string) *ApplyDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return &ApplyDAO{DB: db}
}

// InsertApply 创建群申请
func (m *ApplyDAO) InsertApply(ctx context.Context, apply *model.GroupApply) error {
	return m.DB.WithContext(ctx).Create(apply).Error
}

// FindApplyByID 根据ID查询申请
func (m *ApplyDAO) FindApplyByID(ctx context.Context, id uint64) (*model.GroupApply, error) {
	var apply model.GroupApply
	if err := m.DB.WithContext(ctx).Where("id = ?", id).First(&apply).Error; err != nil {
		return nil, err
	}
	return &apply, nil
}

// FindPendingAppliesByGroupIDs 查询指定群的待处理申请（分页）
func (m *ApplyDAO) FindPendingAppliesByGroupIDs(ctx context.Context, groupIDs []uint64, limit, offset int) ([]*model.GroupApply, int64, error) {
	if len(groupIDs) == 0 {
		return []*model.GroupApply{}, 0, nil
	}

	var applies []*model.GroupApply
	var total int64

	query := m.DB.WithContext(ctx).Model(&model.GroupApply{}).
		Where("group_id IN ? AND status = ?", groupIDs, model.GroupApplyStatusPending)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit > 0 {
		query = query.Limit(limit).Offset(offset)
	}

	if err := query.Order("create_time DESC").Find(&applies).Error; err != nil {
		return nil, 0, err
	}

	return applies, total, nil
}

// FindExistingPendingApply 检查是否存在重复的待处理申请
func (m *ApplyDAO) FindExistingPendingApply(ctx context.Context, fromUserID, groupID uint64) (*model.GroupApply, error) {
	var apply model.GroupApply
	if err := m.DB.WithContext(ctx).
		Where("from_user_id = ? AND group_id = ? AND status = ?", fromUserID, groupID, model.GroupApplyStatusPending).
		First(&apply).Error; err != nil {
		return nil, err
	}
	return &apply, nil
}

// UpdateApplyStatus 更新申请状态
func (m *ApplyDAO) UpdateApplyStatus(ctx context.Context, id uint64, status uint8, rejectReason string) error {
	updates := map[string]any{
		"status": status,
	}
	if rejectReason != "" {
		updates["remark"] = rejectReason
	}
	return m.DB.WithContext(ctx).Model(&model.GroupApply{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// UpdateApplyStatusWithHandler 更新申请状态并记录处理人
func (m *ApplyDAO) UpdateApplyStatusWithHandler(ctx context.Context, id uint64, status uint8, handlerID uint64, rejectReason string) error {
	updates := map[string]any{
		"status":     status,
		"handler_id": handlerID,
	}
	if rejectReason != "" {
		updates["remark"] = rejectReason
	}
	return m.DB.WithContext(ctx).Model(&model.GroupApply{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// FindPendingAppliesByFromUserID 查询用户发出的待处理申请
func (m *ApplyDAO) FindPendingAppliesByFromUserID(ctx context.Context, userID uint64, limit, offset int) ([]*model.GroupApply, int64, error) {
	var applies []*model.GroupApply
	var total int64

	query := m.DB.WithContext(ctx).Model(&model.GroupApply{}).
		Where("from_user_id = ? AND status = ?", userID, model.GroupApplyStatusPending)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit > 0 {
		query = query.Limit(limit).Offset(offset)
	}

	if err := query.Order("create_time DESC").Find(&applies).Error; err != nil {
		return nil, 0, err
	}

	return applies, total, nil
}
