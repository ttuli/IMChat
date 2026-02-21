package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// SegmentCache 号段缓存，用于内存中快速分配ID
type SegmentCache struct {
	CurrentID int64 // 当前分配的ID
	MaxID     int64 // 号段的最大ID
	mu        sync.Mutex
}

type IdDAO struct {
	*gorm.DB
	// 每个业务类型的号段缓存
	caches  map[string]*SegmentCache
	cacheMu sync.RWMutex
}

func NewIdDAO(dbSource string) *IdDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	dao := &IdDAO{
		DB:     db,
		caches: make(map[string]*SegmentCache),
	}
	return dao
}

// InitBizTags 初始化所有业务标签的记录（没有就创建，有就跳过）
func (dao *IdDAO) InitBizTags(ctx context.Context) error {
	bizTags := []string{
		model.BizTagUser,
		model.BizTagGroup,
		model.BizTagMessage,
	}

	for _, bizTag := range bizTags {
		// 检查记录是否存在
		var segment model.IdSegment
		err := dao.DB.WithContext(ctx).
			Where("biz_tag = ?", bizTag).
			First(&segment).Error

		if err == nil {
			// 记录已存在，跳过
			continue
		}

		if !errors.Is(err, gorm.ErrRecordNotFound) {
			// 查询出错
			return fmt.Errorf("查询业务标签失败: bizTag=%s, error=%w", bizTag, err)
		}

		// 记录不存在，创建初始记录
		length := model.GetIDLength(bizTag)

		// 如果ID长度未配置（如message），使用默认值
		var initialMaxID int64 = 0
		if length > 0 {
			minID, _ := model.GetIDRange(bizTag)
			initialMaxID = minID - 1 // 初始值设为最小ID-1，这样第一次获取时从minID开始
		}

		newSegment := model.IdSegment{
			BizTag:  bizTag,
			MaxID:   initialMaxID,
			Step:    model.DefaultStep,
			Version: 0,
		}

		if err = dao.DB.WithContext(ctx).Create(&newSegment).Error; err != nil {
			return fmt.Errorf("初始化业务标签失败: bizTag=%s, error=%w", bizTag, err)
		}
	}

	return nil
}

// SaveCacheState 优雅关闭时持久化缓存状态，将已分配的ID位置保存回数据库
// 这样可以减少服务重启时的ID浪费
func (dao *IdDAO) SaveCacheState(ctx context.Context) error {
	dao.cacheMu.RLock()
	defer dao.cacheMu.RUnlock()

	for bizTag, cache := range dao.caches {
		cache.mu.Lock()
		// 只有当缓存中有已分配的ID时才更新
		if cache.CurrentID > 0 {
			// 更新数据库中的 max_id 为当前已分配的位置
			// 这样下次启动时会从 CurrentID+1 开始分配
			result := dao.DB.WithContext(ctx).
				Model(&model.IdSegment{}).
				Where("biz_tag = ?", bizTag).
				Update("max_id", cache.CurrentID)

			if result.Error != nil {
				cache.mu.Unlock()
				return fmt.Errorf("保存缓存状态失败: bizTag=%s, error=%w", bizTag, result.Error)
			}
		}
		cache.mu.Unlock()
	}

	return nil
}

// getCache 获取或创建指定业务类型的缓存
func (dao *IdDAO) getCache(bizTag string) *SegmentCache {
	dao.cacheMu.RLock()
	cache, exists := dao.caches[bizTag]
	dao.cacheMu.RUnlock()

	if exists {
		return cache
	}

	// 双重检查锁定
	dao.cacheMu.Lock()
	defer dao.cacheMu.Unlock()

	cache, exists = dao.caches[bizTag]
	if !exists {
		cache = &SegmentCache{
			CurrentID: 0,
			MaxID:     0,
		}
		dao.caches[bizTag] = cache
	}
	return cache
}

// getNextSegment 从数据库获取下一个号段（使用乐观锁）
func (dao *IdDAO) getNextSegment(ctx context.Context, bizTag string) (startID, endID int64, err error) {
	const maxRetries = 10 // 最大重试次数

	// 获取ID长度和范围
	length := model.GetIDLength(bizTag)
	minID, maxID := model.GetIDRange(bizTag)

	// 如果ID长度未配置，使用默认逻辑
	if length <= 0 {
		return dao.getNextSegmentDefault(ctx, bizTag)
	}

	for i := 0; i < maxRetries; i++ {
		var segment model.IdSegment

		// 查询当前号段记录
		err = dao.DB.WithContext(ctx).
			Where("biz_tag = ?", bizTag).
			First(&segment).Error

		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 记录不存在，初始化
				segment = model.IdSegment{
					BizTag:  bizTag,
					MaxID:   minID - 1, // 初始值设为最小ID-1
					Step:    model.DefaultStep,
					Version: 0,
				}
				if err = dao.DB.WithContext(ctx).Create(&segment).Error; err != nil {
					return 0, 0, fmt.Errorf("初始化号段失败: %w", err)
				}
			} else {
				return 0, 0, fmt.Errorf("查询号段失败: %w", err)
			}
		}

		// 计算新的最大ID，确保不超过范围
		newMaxID := segment.MaxID + segment.Step
		if newMaxID > maxID {
			return 0, 0, fmt.Errorf("ID已达到最大值: bizTag=%s, maxID=%d", bizTag, maxID)
		}

		// 使用乐观锁更新号段
		result := dao.DB.WithContext(ctx).
			Model(&model.IdSegment{}).
			Where("biz_tag = ? AND version = ?", bizTag, segment.Version).
			Updates(map[string]interface{}{
				"max_id":  newMaxID,
				"version": segment.Version + 1,
			})

		if result.Error != nil {
			return 0, 0, fmt.Errorf("更新号段失败: %w", result.Error)
		}

		// 检查是否更新成功（版本匹配）
		if result.RowsAffected > 0 {
			startID = segment.MaxID + 1
			endID = newMaxID
			return startID, endID, nil
		}

		// 版本冲突，重试
	}

	return 0, 0, fmt.Errorf("获取号段失败: 超过最大重试次数")
}

// getNextSegmentDefault 默认的号段获取逻辑（用于未配置长度的业务标签）
func (dao *IdDAO) getNextSegmentDefault(ctx context.Context, bizTag string) (startID, endID int64, err error) {
	const maxRetries = 10

	for i := 0; i < maxRetries; i++ {
		var segment model.IdSegment

		err = dao.DB.WithContext(ctx).
			Where("biz_tag = ?", bizTag).
			First(&segment).Error

		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				segment = model.IdSegment{
					BizTag:  bizTag,
					MaxID:   0,
					Step:    model.DefaultStep,
					Version: 0,
				}
				if err = dao.DB.WithContext(ctx).Create(&segment).Error; err != nil {
					return 0, 0, fmt.Errorf("初始化号段失败: %w", err)
				}
			} else {
				return 0, 0, fmt.Errorf("查询号段失败: %w", err)
			}
		}

		newMaxID := segment.MaxID + segment.Step
		result := dao.DB.WithContext(ctx).
			Model(&model.IdSegment{}).
			Where("biz_tag = ? AND version = ?", bizTag, segment.Version).
			Updates(map[string]interface{}{
				"max_id":  newMaxID,
				"version": segment.Version + 1,
			})

		if result.Error != nil {
			return 0, 0, fmt.Errorf("更新号段失败: %w", result.Error)
		}

		if result.RowsAffected > 0 {
			startID = segment.MaxID + 1
			endID = newMaxID
			return startID, endID, nil
		}
	}

	return 0, 0, fmt.Errorf("获取号段失败: 超过最大重试次数")
}

// GetNextIDs 批量获取ID（根据业务标签生成固定长度的ID）
func (dao *IdDAO) GetNextIDs(ctx context.Context, bizTag string, count int32) ([]int64, error) {
	if count <= 0 {
		count = 1 // 默认获取1个
	}

	// 获取ID长度和范围
	length := model.GetIDLength(bizTag)

	// 如果ID长度未配置，使用默认逻辑
	if length <= 0 {
		return dao.getNextIDsDefault(ctx, bizTag, count)
	}

	_, maxID := model.GetIDRange(bizTag)

	cache := dao.getCache(bizTag)
	cache.mu.Lock()
	defer cache.mu.Unlock()

	var ids []int64
	remaining := int(count)

	for remaining > 0 {
		// 检查当前号段是否还有可用ID（注意：CurrentID是已分配的最后一个ID）
		// 如果 CurrentID >= MaxID，说明号段已用完或未初始化，需要获取新号段
		if cache.CurrentID >= cache.MaxID || cache.MaxID == 0 {
			// 号段用完，从数据库获取新号段
			startID, endID, err := dao.getNextSegment(ctx, bizTag)
			if err != nil {
				return nil, err
			}
			// CurrentID设置为startID-1，这样第一次++后就是startID
			cache.CurrentID = startID - 1
			cache.MaxID = endID
		}

		// 从当前号段中分配ID，确保不超过最大值
		for remaining > 0 && cache.CurrentID < cache.MaxID {
			cache.CurrentID++

			// 检查是否超过最大ID
			if cache.CurrentID > maxID {
				return nil, fmt.Errorf("ID已达到最大值: bizTag=%s, maxID=%d", bizTag, maxID)
			}

			ids = append(ids, cache.CurrentID)
			remaining--
		}
	}

	return ids, nil
}

// getNextIDsDefault 默认的ID获取逻辑（用于未配置长度的业务标签）
func (dao *IdDAO) getNextIDsDefault(ctx context.Context, bizTag string, count int32) ([]int64, error) {
	if count <= 0 {
		count = 1
	}

	cache := dao.getCache(bizTag)
	cache.mu.Lock()
	defer cache.mu.Unlock()

	var ids []int64
	remaining := int(count)

	for remaining > 0 {
		// 检查当前号段是否还有可用ID（注意：CurrentID是已分配的最后一个ID）
		// 如果 CurrentID >= MaxID 或 MaxID == 0，说明号段已用完或未初始化，需要获取新号段
		if cache.CurrentID >= cache.MaxID || cache.MaxID == 0 {
			startID, endID, err := dao.getNextSegmentDefault(ctx, bizTag)
			if err != nil {
				return nil, err
			}
			// CurrentID设置为startID-1，这样第一次++后就是startID
			cache.CurrentID = startID - 1
			cache.MaxID = endID
		}

		// 从当前号段中分配ID
		for remaining > 0 && cache.CurrentID < cache.MaxID {
			cache.CurrentID++
			ids = append(ids, cache.CurrentID)
			remaining--
		}
	}

	return ids, nil
}
