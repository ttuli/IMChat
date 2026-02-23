package dao

import (
	"context"
	"time"

	"IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// FriendApplyDAO 好友申请数据访问层
type FriendApplyDAO struct {
	db *gorm.DB
}

// NewFriendApplyDAO 创建好友申请 DAO
func NewFriendApplyDAO(DataSource string) *FriendApplyDAO {
	db, err := gorm.Open(mysql.Open(DataSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return &FriendApplyDAO{db: db}
}

// InsertFriendApply 创建好友申请
func (d *FriendApplyDAO) InsertFriendApply(ctx context.Context, apply *model.FriendApply) error {
	return d.db.WithContext(ctx).Create(apply).Error
}

// FindFriendApplyByID 根据 ID 查询申请
func (d *FriendApplyDAO) FindFriendApplyByID(ctx context.Context, id uint64) (*model.FriendApply, error) {
	var apply model.FriendApply
	err := d.db.WithContext(ctx).Where("id = ?", id).First(&apply).Error
	if err != nil {
		return nil, err
	}
	return &apply, nil
}

// FindPendingAppliesByToUserID 分页查询用户收到的待处理申请
func (d *FriendApplyDAO) FindPendingAppliesByToUserID(ctx context.Context, toUserID uint64, limit, offset int32) ([]*model.FriendApply, int64, error) {
	var applies []*model.FriendApply
	var total int64

	db := d.db.WithContext(ctx).Model(&model.FriendApply{}).
		Where("to_user_id = ? OR from_user_id = ?", toUserID, toUserID)

	// 查询总数
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 查询分页数据，按创建时间倒序
	query := db.Order("create_time DESC").Offset(int(offset))
	if limit != -1 {
		query = query.Limit(int(limit))
	}
	if err := query.Find(&applies).Error; err != nil {
		return nil, 0, err
	}

	return applies, total, nil
}

// FindExistingPendingApply 检查是否存在重复的待处理申请
func (d *FriendApplyDAO) FindExistingPendingApply(ctx context.Context, fromUserID, toUserID uint64) (*model.FriendApply, error) {
	var apply model.FriendApply
	err := d.db.WithContext(ctx).
		Where("from_user_id = ? AND to_user_id = ? AND status = ?", fromUserID, toUserID, model.ApplyStatusPending).
		First(&apply).Error
	if err != nil {
		return nil, err
	}
	return &apply, nil
}

// UpdateFriendApplyStatus 更新申请状态
func (d *FriendApplyDAO) UpdateFriendApplyStatus(ctx context.Context, id uint64, status uint8, rejectReason string) error {
	updates := map[string]any{
		"status":      status,
		"handle_time": time.Now(),
	}
	if rejectReason != "" {
		updates["reject_reason"] = rejectReason
	}

	return d.db.WithContext(ctx).
		Model(&model.FriendApply{}).
		Where("id = ?", id).
		Updates(updates).Error
}
