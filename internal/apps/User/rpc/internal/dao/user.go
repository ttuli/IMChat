package dao

import (
	"context"
	"fmt"
	"strconv"
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
	redisInfoIdPrefix    = "user:id:"
	redisInfoPhonePrefix = "user:phone:"
)

// 缓存过期时间
const (
	cacheExpireSeconds = 3600 // 1小时
)

type UserDAO struct {
	*gorm.DB
	*redisc.RedisModel
}

func NewUserDAO(dbSource string, redisSource redis.RedisConf) *UserDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	return &UserDAO{
		DB:         db,
		RedisModel: redisc.MustNewRedis(redisSource),
	}
}

func (m *UserDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return m.DB.WithContext(ctx).Transaction(fc)
}

// InsertUser 插入用户（先写数据库，再写缓存）
func (m *UserDAO) InsertUser(ctx context.Context, data *model.UserInfo) error {
	// 1. 先写数据库
	if err := m.DB.WithContext(ctx).Create(data).Error; err != nil {
		return err
	}

	// 2. 再写缓存（失败只记录日志，不影响主流程）
	m.setUserCache(ctx, data)
	return nil
}

// UpdateUser 更新用户信息（延迟双删策略）
func (m *UserDAO) UpdateUser(ctx context.Context, data *model.UserInfo) error {
	// 1. 删除缓存
	m.deleteUserCache(ctx, data.UserID, data.Phone)

	// 2. 更新数据库
	if err := m.DB.WithContext(ctx).Save(data).Error; err != nil {
		return err
	}

	// 3. 延迟双删
	m.delayDeleteCache(data.UserID, data.Phone)
	return nil
}

// FindOneByID 根据ID查找用户（先查缓存，再查数据库）
func (m *UserDAO) FindOneByID(ctx context.Context, id uint64) (*model.UserInfo, error) {
	// 1. 先查缓存
	cacheKey := fmt.Sprintf("%s%d", redisInfoIdPrefix, id)
	res, _ := m.Redis.Get(cacheKey)
	if res != "" {
		var u model.UserInfo
		if err := json.Unmarshal([]byte(res), &u); err == nil {
			return &u, nil
		}
		// 缓存数据损坏，删除并继续查数据库
		m.Redis.Del(cacheKey)
	}

	// 2. 查数据库
	var u model.UserInfo
	if err := m.DB.WithContext(ctx).Where("user_id = ?", id).First(&u).Error; err != nil {
		return nil, err
	}

	// 3. 写缓存（失败只记录日志，不影响返回结果）
	m.setUserCache(ctx, &u)
	return &u, nil
}

// FindByIDs 根据ID列表批量查找用户
func (m *UserDAO) FindByIDs(ctx context.Context, ids []uint64) ([]*model.UserInfo, error) {
	if len(ids) == 0 {
		return []*model.UserInfo{}, nil
	}

	// 1. 构建缓存 key 列表
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("%s%d", redisInfoIdPrefix, id))
	}

	// 2. 批量查缓存
	res, err := m.MgetCtx(ctx, keys...)
	if err != nil {
		// 缓存查询失败，直接查数据库
		logger.Errorf("批量查询缓存失败: %v", err)
		return m.findByIDsFromDB(ctx, ids)
	}

	// 3. 解析缓存结果，找出缺失的 ID
	var missingIDs []uint64
	userMap := make(map[uint64]*model.UserInfo, len(ids))

	for i, v := range res {
		if v == "" {
			missingIDs = append(missingIDs, ids[i])
			continue
		}
		var u model.UserInfo
		if err := json.Unmarshal([]byte(v), &u); err != nil {
			// 缓存数据损坏，加入缺失列表
			missingIDs = append(missingIDs, ids[i])
			continue
		}
		userMap[u.UserID] = &u
	}

	// 4. 查询缺失的用户
	if len(missingIDs) > 0 {
		dbUsers, err := m.findByIDsFromDB(ctx, missingIDs)
		if err != nil {
			return nil, err
		}
		for _, u := range dbUsers {
			userMap[u.UserID] = u
			// 异步写缓存
			go m.setUserCache(context.Background(), u)
		}
	}

	// 5. 按原始顺序返回结果
	result := make([]*model.UserInfo, 0, len(ids))
	for _, id := range ids {
		if u, ok := userMap[id]; ok {
			result = append(result, u)
		}
	}

	return result, nil
}

// findByIDsFromDB 从数据库批量查询用户
func (m *UserDAO) findByIDsFromDB(ctx context.Context, ids []uint64) ([]*model.UserInfo, error) {
	var users []*model.UserInfo
	if err := m.DB.WithContext(ctx).Where("user_id IN ?", ids).Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

// FindOneByPhone 根据手机号查找用户
func (m *UserDAO) FindOneByPhone(ctx context.Context, phone string) (*model.UserInfo, error) {
	// 1. 先查手机号索引缓存
	phoneKey := redisInfoPhonePrefix + phone
	res, _ := m.Redis.Get(phoneKey)
	if res != "" {
		userID, err := strconv.ParseUint(res, 10, 64)
		if err == nil {
			return m.FindOneByID(ctx, userID)
		}
		// 缓存数据损坏，删除
		m.Redis.Del(phoneKey)
	}

	// 2. 查数据库
	var u model.UserInfo
	if err := m.DB.WithContext(ctx).Where("phone = ?", phone).First(&u).Error; err != nil {
		return nil, err
	}

	// 3. 写缓存
	m.setUserCache(ctx, &u)
	return &u, nil
}

// FindByName 根据名字模糊查询用户（不缓存，因为是模糊查询）
func (m *UserDAO) FindByName(ctx context.Context, name string, limit, offset int32) ([]*model.UserInfo, error) {
	var users []*model.UserInfo
	if err := m.DB.WithContext(ctx).Where("user_name LIKE ?", "%"+name+"%").Limit(int(limit)).Offset(int(offset)).Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

// ==================== 缓存辅助方法 ====================

// setUserCache 设置用户缓存（使用 Pipeline 原子性批量设置）
func (m *UserDAO) setUserCache(ctx context.Context, user *model.UserInfo) {
	data, err := json.Marshal(user)
	if err != nil {
		logger.Errorf("序列化用户数据失败: %v", err)
		return
	}

	idKey := fmt.Sprintf("%s%d", redisInfoIdPrefix, user.UserID)
	phoneKey := redisInfoPhonePrefix + user.Phone
	dataStr := string(data)
	userIDStr := strconv.FormatUint(user.UserID, 10)

	// 使用 Pipeline 原子性批量设置
	err = m.Redis.Pipelined(func(p redis.Pipeliner) error {
		p.SetEx(ctx, idKey, dataStr, time.Duration(cacheExpireSeconds)*time.Second)
		p.SetEx(ctx, phoneKey, userIDStr, time.Duration(cacheExpireSeconds)*time.Second)
		return nil
	})
	if err != nil {
		logger.Errorf("设置用户缓存失败: %v", err)
	}
}

// deleteUserCache 删除用户缓存（使用 Pipeline 原子性批量删除）
func (m *UserDAO) deleteUserCache(ctx context.Context, userID uint64, phone string) {
	idKey := fmt.Sprintf("%s%d", redisInfoIdPrefix, userID)
	phoneKey := redisInfoPhonePrefix + phone

	err := m.Redis.Pipelined(func(p redis.Pipeliner) error {
		p.Del(ctx, idKey)
		p.Del(ctx, phoneKey)
		return nil
	})
	if err != nil {
		logger.Errorf("删除用户缓存失败: %v", err)
	}
}

// delayDeleteCache 延迟双删（使用 Pipeline 原子性删除，防止并发读写导致的缓存不一致）
func (m *UserDAO) delayDeleteCache(userID uint64, phone string) {
	go func() {
		time.Sleep(500 * time.Millisecond)
		idKey := fmt.Sprintf("%s%d", redisInfoIdPrefix, userID)
		phoneKey := redisInfoPhonePrefix + phone

		err := m.Redis.Pipelined(func(p redis.Pipeliner) error {
			p.Del(context.Background(), idKey)
			p.Del(context.Background(), phoneKey)
			return nil
		})
		if err != nil {
			logger.Errorf("延迟删除用户缓存失败: %v", err)
		}
	}()
}
