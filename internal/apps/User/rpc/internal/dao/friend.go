package dao

import (
	"context"

	"IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// FriendDAO 好友关系数据访问层
type FriendDAO struct {
	db *gorm.DB
}

// NewFriendDAO 创建好友关系 DAO
func NewFriendDAO(DataSource string) *FriendDAO {
	db, err := gorm.Open(mysql.Open(DataSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return &FriendDAO{db: db}
}

// InsertFriend 创建双向好友关系
func (d *FriendDAO) InsertFriend(ctx context.Context, userID, friendID uint64, source uint8) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 插入 userID -> friendID
		f1 := &model.UserFriend{
			UserID:   userID,
			FriendID: friendID,
			Source:   source,
		}
		if err := tx.Create(f1).Error; err != nil {
			return err
		}

		// 插入 friendID -> userID (反向关系)
		f2 := &model.UserFriend{
			UserID:   friendID,
			FriendID: userID,
			Source:   source,
			Remark:   "", // 反向关系备注为空，由对方自己设置
		}
		if err := tx.Create(f2).Error; err != nil {
			return err
		}

		return nil
	})
}

// FindFriendsByUserID 分页获取用户的好友列表
func (d *FriendDAO) FindFriendsByUserID(ctx context.Context, userID uint64, limit, offset int32) ([]*model.UserFriend, int64, error) {
	var friends []*model.UserFriend
	var total int64

	db := d.db.WithContext(ctx).Model(&model.UserFriend{}).Where("user_id = ?", userID)

	// 查询总数
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 查询分页数据
	query := db.Offset(int(offset))
	if limit != -1 {
		query = query.Limit(int(limit))
	}
	if err := query.Find(&friends).Error; err != nil {
		return nil, 0, err
	}

	return friends, total, nil
}

// FindFriendRelation 查询两人是否已是好友
func (d *FriendDAO) FindFriendRelation(ctx context.Context, userID, friendID uint64) (*model.UserFriend, error) {
	var friend model.UserFriend
	err := d.db.WithContext(ctx).
		Where("user_id = ? AND friend_id = ?", userID, friendID).
		First(&friend).Error
	if err != nil {
		return nil, err
	}
	return &friend, nil
}

// UpdateFriend 更新好友信息（备注、拉黑、星标）
func (d *FriendDAO) UpdateFriend(ctx context.Context, userID, friendID uint64, updates map[string]any) error {
	return d.db.WithContext(ctx).
		Model(&model.UserFriend{}).
		Where("user_id = ? AND friend_id = ?", userID, friendID).
		Updates(updates).Error
}

// DeleteFriend 双向删除好友关系
func (d *FriendDAO) DeleteFriend(ctx context.Context, userID, friendID uint64) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 删除 userID -> friendID
		if err := tx.Where("user_id = ? AND friend_id = ?", userID, friendID).
			Delete(&model.UserFriend{}).Error; err != nil {
			return err
		}

		// 删除 friendID -> userID
		if err := tx.Where("user_id = ? AND friend_id = ?", friendID, userID).
			Delete(&model.UserFriend{}).Error; err != nil {
			return err
		}

		return nil
	})
}
