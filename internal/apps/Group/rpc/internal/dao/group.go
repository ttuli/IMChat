package dao

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/redisc"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	redisGroupIdPrefix = "group:id:"
)

// 缓存过期时间
const (
	cacheExpireSeconds = 3600 // 1小时
)

type GroupDAO struct {
	*gorm.DB
	*redisc.RedisModel
}

func NewGroupDAO(dbSource string, redisSource redis.RedisConf) *GroupDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	return &GroupDAO{
		DB:         db,
		RedisModel: redisc.MustNewRedis(redisSource),
	}
}

func (m *GroupDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return m.DB.WithContext(ctx).Transaction(fc)
}

// InsertGroup 创建群组
func (m *GroupDAO) InsertGroup(ctx context.Context, data *model.Group) error {
	if err := m.DB.WithContext(ctx).Create(data).Error; err != nil {
		return err
	}
	m.setGroupCache(ctx, data)
	return nil
}

// FindByID 根据ID查找群组
func (m *GroupDAO) FindByID(ctx context.Context, id uint64) (*model.Group, error) {
	// 1. 先查缓存
	cacheKey := fmt.Sprintf("%s%d", redisGroupIdPrefix, id)
	res, _ := m.Redis.Get(cacheKey)
	if res != "" {
		var g model.Group
		if err := json.Unmarshal([]byte(res), &g); err == nil {
			return &g, nil
		}
		m.Redis.Del(cacheKey)
	}

	// 2. 查数据库
	var g model.Group
	if err := m.DB.WithContext(ctx).Where("id = ?", id).First(&g).Error; err != nil {
		return nil, err
	}

	// 3. 写缓存
	m.setGroupCache(ctx, &g)
	return &g, nil
}

// FindByIDs 批量查询群组
func (m *GroupDAO) FindByIDs(ctx context.Context, ids []uint64) ([]*model.Group, error) {
	if len(ids) == 0 {
		return []*model.Group{}, nil
	}

	// 1. 构建缓存 key 列表
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("%s%d", redisGroupIdPrefix, id))
	}

	// 2. 批量查缓存
	res, err := m.MgetCtx(ctx, keys...)
	if err != nil {
		logger.Errorf("批量查询群组缓存失败: %v", err)
		return m.findByIDsFromDB(ctx, ids)
	}

	// 3. 解析缓存结果，找出缺失的 ID
	var missingIDs []uint64
	groupMap := make(map[uint64]*model.Group, len(ids))

	for i, v := range res {
		if v == "" {
			missingIDs = append(missingIDs, ids[i])
			continue
		}
		var g model.Group
		if err := json.Unmarshal([]byte(v), &g); err != nil {
			missingIDs = append(missingIDs, ids[i])
			continue
		}
		groupMap[g.ID] = &g
	}

	// 4. 查询缺失的群组
	if len(missingIDs) > 0 {
		dbGroups, err := m.findByIDsFromDB(ctx, missingIDs)
		if err != nil {
			return nil, err
		}
		for _, g := range dbGroups {
			groupMap[g.ID] = g
			go m.setGroupCache(context.Background(), g)
		}
	}

	// 5. 按原始顺序返回结果
	result := make([]*model.Group, 0, len(ids))
	for _, id := range ids {
		if g, ok := groupMap[id]; ok {
			result = append(result, g)
		}
	}

	return result, nil
}

func (m *GroupDAO) findByIDsFromDB(ctx context.Context, ids []uint64) ([]*model.Group, error) {
	var groups []*model.Group
	if err := m.DB.WithContext(ctx).Where("id IN ?", ids).Find(&groups).Error; err != nil {
		return nil, err
	}
	return groups, nil
}

// UpdateGroup 更新群组信息（延迟双删策略）
func (m *GroupDAO) UpdateGroup(ctx context.Context, data *model.Group) error {
	// 1. 删除缓存
	m.deleteGroupCache(ctx, data.ID)

	// 2. 更新数据库
	if err := m.DB.WithContext(ctx).Save(data).Error; err != nil {
		return err
	}

	// 3. 延迟双删
	m.delayDeleteCache(data.ID)
	return nil
}

// DeleteGroup 删除群组
func (m *GroupDAO) DeleteGroup(ctx context.Context, id uint64) error {
	m.deleteGroupCache(ctx, id)
	if err := m.DB.WithContext(ctx).Delete(&model.Group{}, id).Error; err != nil {
		return err
	}
	m.delayDeleteCache(id)
	return nil
}

// FindGroupsByUserID 查询用户所在的群组ID列表
func (m *GroupDAO) FindGroupIDsByUserID(ctx context.Context, userID uint64, limit, offset int) ([]uint64, int64, error) {
	var groupIDs []uint64
	var total int64

	query := m.DB.WithContext(ctx).Table("group_member").
		Select("group_id").
		Where("user_id = ?", userID)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit > 0 {
		query = query.Limit(limit).Offset(offset)
	}

	if err := query.Pluck("group_id", &groupIDs).Error; err != nil {
		return nil, 0, err
	}

	return groupIDs, total, nil
}

// SearchByName 通过名称模糊搜索群组
func (m *GroupDAO) SearchByName(ctx context.Context, keyword string, limit, offset int) ([]*model.Group, int64, error) {
	var groups []*model.Group
	var total int64

	query := m.DB.WithContext(ctx).Model(&model.Group{}).
		Where("name LIKE ?", "%"+keyword+"%")

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit > 0 {
		query = query.Limit(limit).Offset(offset)
	}

	if err := query.Find(&groups).Error; err != nil {
		return nil, 0, err
	}

	return groups, total, nil
}

// ==================== 缓存辅助方法 ====================

func (m *GroupDAO) setGroupCache(ctx context.Context, group *model.Group) {
	data, err := json.Marshal(group)
	if err != nil {
		logger.Errorf("序列化群组数据失败: %v", err)
		return
	}

	idKey := fmt.Sprintf("%s%d", redisGroupIdPrefix, group.ID)
	if err := m.Redis.SetexCtx(ctx, idKey, string(data), cacheExpireSeconds); err != nil {
		logger.Errorf("设置群组缓存失败: %v", err)
	}
}

func (m *GroupDAO) deleteGroupCache(ctx context.Context, groupID uint64) {
	idKey := fmt.Sprintf("%s%d", redisGroupIdPrefix, groupID)
	if _, err := m.Redis.DelCtx(ctx, idKey); err != nil {
		logger.Errorf("删除群组缓存失败: %v", err)
	}
}

func (m *GroupDAO) delayDeleteCache(groupID uint64) {
	go func() {
		time.Sleep(500 * time.Millisecond)
		idKey := fmt.Sprintf("%s%d", redisGroupIdPrefix, groupID)
		if _, err := m.Redis.Del(idKey); err != nil {
			logger.Errorf("延迟删除群组缓存失败: %v", err)
		}
	}()
}

// ==================== 群成员管理 ====================

// InsertMember 添加单个成员
func (m *GroupDAO) InsertMember(ctx context.Context, member *model.GroupMember) error {
	return m.DB.WithContext(ctx).Create(member).Error
}

// InsertMembers 批量添加成员
func (m *GroupDAO) InsertMembers(ctx context.Context, members []*model.GroupMember) error {
	if len(members) == 0 {
		return nil
	}
	return m.DB.WithContext(ctx).Create(&members).Error
}

// FindMembersByGroupID 查询群成员列表
func (m *GroupDAO) FindMembersByGroupID(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	var members []*model.GroupMember
	if err := m.DB.WithContext(ctx).Where("group_id = ?", groupID).Find(&members).Error; err != nil {
		return nil, err
	}
	return members, nil
}

// FindManagersByGroupID 查询群管理员和群主列表
func (m *GroupDAO) FindManagersByGroupID(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	var members []*model.GroupMember
	if err := m.DB.WithContext(ctx).
		Where("group_id = ? AND role IN ?", groupID, []int8{model.GroupRoleOwner, model.GroupRoleAdmin}).
		Find(&members).Error; err != nil {
		return nil, err
	}
	return members, nil
}

// FindMember 查询单个成员
func (m *GroupDAO) FindMember(ctx context.Context, groupID, userID uint64) (*model.GroupMember, error) {
	var member model.GroupMember
	if err := m.DB.WithContext(ctx).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		First(&member).Error; err != nil {
		return nil, err
	}
	return &member, nil
}

// UpdateMember 更新成员信息
func (m *GroupDAO) UpdateMember(ctx context.Context, groupID, userID uint64, updates map[string]any) error {
	return m.DB.WithContext(ctx).Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Updates(updates).Error
}

// DeleteMember 删除成员
func (m *GroupDAO) DeleteMember(ctx context.Context, groupID, userID uint64) error {
	return m.DB.WithContext(ctx).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Delete(&model.GroupMember{}).Error
}

// DeleteMembersByGroupID 删除群所有成员
func (m *GroupDAO) DeleteMembersByGroupID(ctx context.Context, groupID uint64) error {
	return m.DB.WithContext(ctx).
		Where("group_id = ?", groupID).
		Delete(&model.GroupMember{}).Error
}

// CountMembers 统计群成员数
func (m *GroupDAO) CountMembers(ctx context.Context, groupID uint64) (int64, error) {
	var count int64
	if err := m.DB.WithContext(ctx).Model(&model.GroupMember{}).
		Where("group_id = ?", groupID).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// IsMember 检查是否是群成员
func (m *GroupDAO) IsMember(ctx context.Context, groupID, userID uint64) (bool, error) {
	var count int64
	if err := m.DB.WithContext(ctx).Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// FindAdminGroupIDs 查询用户是管理员或群主的群ID列表
func (m *GroupDAO) FindAdminGroupIDs(ctx context.Context, userID uint64) ([]uint64, error) {
	var groupIDs []uint64
	if err := m.DB.WithContext(ctx).Table("group_member").
		Select("group_id").
		Where("user_id = ? AND role IN ?", userID, []int8{model.GroupRoleOwner, model.GroupRoleAdmin}).
		Pluck("group_id", &groupIDs).Error; err != nil {
		return nil, err
	}
	return groupIDs, nil
}

// FindGroupIDsByUserID 查询用户所在的所有群ID列表（无分页版本）
func (m *GroupDAO) FindAllGroupIDsByUserID(ctx context.Context, userID uint64) ([]uint64, error) {
	var groupIDs []uint64
	if err := m.DB.WithContext(ctx).Model(&model.GroupMember{}).
		Where("user_id = ?", userID).
		Pluck("group_id", &groupIDs).Error; err != nil {
		return nil, err
	}
	fmt.Println(groupIDs)
	return groupIDs, nil
}

// CreateGroupWithMembers 创建群聊并添加成员（事务）
func (m *GroupDAO) CreateGroupWithMembers(ctx context.Context, group *model.Group, members []*model.GroupMember) error {
	return m.Transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Create(group).Error; err != nil {
			return err
		}
		if len(members) > 0 {
			if err := tx.Create(&members).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
