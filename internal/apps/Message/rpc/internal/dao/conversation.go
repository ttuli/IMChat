package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	convTimelinePrefix = "user:conv:timeline:"
	convInfoPrefix     = "conv:info:"
	convInfoExpire     = 7 * 24 * time.Hour
)

// ConversationDAO 会话数据访问对象 (MySQL + Redis缓存)
type ConversationDAO struct {
	db        *gorm.DB
	cache     *redisx.Client
	seqSyncer *SeqSyncer
}

// NewConversationDAO 创建会话DAO
func NewConversationDAO(dbSource string, redisConf redis.RedisConf) *ConversationDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	client, err := redisx.NewClient(redisConf)
	if err != nil {
		panic(err)
	}
	return &ConversationDAO{
		db:        db,
		cache:     client,
		seqSyncer: newSeqSyncer(db, client),
	}
}

// PushSeqUpdate 将完整的会话状态推送到 SeqSyncer，由后台批量刷 MySQL + 广播 UpdateSession。
// 非阻塞：channel 满时打日志丢弃，不影响主链路。
func (c *ConversationDAO) PushSeqUpdate(conversationID string, seq uint64, lastContent string, lastSender uint64, updateTime int64) {
	c.seqSyncer.Push(seqUpdate{
		conversationID: conversationID,
		seq:            seq,
		lastContent:    lastContent,
		lastSender:     lastSender,
		updateTime:     updateTime,
	})
}

// FindConversationsByIDs 批量查询会话
func (c *ConversationDAO) FindConversationsByIDs(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	var convs []*model.Conversation
	if err := c.db.WithContext(ctx).
		Where("conversation_id IN ?", conversationIDs).
		Find(&convs).Error; err != nil {
		return nil, err
	}
	return convs, nil
}

// FindUserConversations 查询用户的会话列表 (按最后消息时间倒序)
func (c *ConversationDAO) FindUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	var userConvs []*model.UserConversation
	if err := c.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("update_time DESC").
		Find(&userConvs).Error; err != nil {
		return nil, err
	}
	return userConvs, nil
}

// ClearUnread 清零未读并更新已读游标
func (c *ConversationDAO) ClearUnread(ctx context.Context, userID uint64, conversationID string, lastReadMsgID, lastReadSeq uint64) error {
	return c.db.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(map[string]any{
			"unread_count":     0,
			"last_read_msg_id": lastReadMsgID,
			"last_read_seq":    lastReadSeq,
		}).Error
}

// UpdateUserConversation 更新用户会话设置 (置顶/免打扰/静音)
func (c *ConversationDAO) UpdateUserConversation(ctx context.Context, userID uint64, conversationID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}

	// 构造完整的主键和默认值，用于不存在时的 Create
	userConv := model.UserConversation{
		UserID:         userID,
		ConversationID: conversationID,
		IsTop:          1, // 默认 1
		IsDisturb:      1, // 默认 1
	}

	// 将需要更新的字段也覆盖到结构体上，保证初次 Create 时的值是设置后的业务值
	if v, ok := updates["is_top"].(int32); ok {
		userConv.IsTop = int8(v)
	}
	if v, ok := updates["is_disturb"].(int32); ok {
		userConv.IsDisturb = int8(v)
	}

	// 执行 Upsert：冲突时按 updates 里的特定字段执行更新
	return c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.Assignments(updates),
	}).Create(&userConv).Error
}

// InsertUserConversation 插入新的用户会话
func (c *ConversationDAO) InsertUserConversation(ctx context.Context, userId uint64, conversationId string, convType int8) error {
	maxSeqVal, err := c.cache.GetCtx(ctx, "conv:seq:"+conversationId)
	var maxSeq uint64
	if err == nil && maxSeqVal != "" {
		maxSeq, _ = strconv.ParseUint(maxSeqVal, 10, 64)
	}

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Conversation 表 (存在则只更新 update_time)
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "conversation_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Conversation{
			ConversationID: conversationId,
			Type:           convType,
		}).Error
		if err != nil {
			return err
		}

		// 2. 插入 UserConversation 记录，忽略冲突
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.UserConversation{
			UserID:         userId,
			ConversationID: conversationId,
			IsTop:          1,
			IsDisturb:      1,
			LastReadSeq:    maxSeq,
		}).Error
	})
}

// Transaction 提供开启事务的能力
func (c *ConversationDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return c.db.WithContext(ctx).Transaction(fc)
}

// BatchInsertUserConversations 批量插入用户会话记录
func (c *ConversationDAO) BatchInsertUserConversations(ctx context.Context, userConvs []*model.UserConversation, convType int8) error {
	if len(userConvs) == 0 {
		return nil
	}

	conversationId := userConvs[0].ConversationID

	maxSeqVal, err := c.cache.GetCtx(ctx, "conv:seq:"+conversationId)
	var maxSeq uint64
	if err == nil && maxSeqVal != "" {
		maxSeq, _ = strconv.ParseUint(maxSeqVal, 10, 64)
	}

	for _, conv := range userConvs {
		conv.LastReadSeq = maxSeq
	}

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Conversation 表
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "conversation_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Conversation{
			ConversationID: conversationId,
			Type:           convType,
		}).Error
		if err != nil {
			return err
		}

		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&userConvs).Error
	})
}

// GetActiveConversationIDs 获取活跃的会话 ID 列表，按时间戳过滤大于 sinceTimestamp 的记录
func (c *ConversationDAO) GetActiveConversationIDs(ctx context.Context, userID uint64, sinceTimestamp int64) ([]string, error) {

	key := fmt.Sprintf("%s%d", convTimelinePrefix, userID)

	pairs, err := c.cache.ZRangeByScoreWithScoresCtx(ctx, key, sinceTimestamp+1, time.Now().UnixMilli()+100000)
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		logger.Errorf("get updated conversations failed for user %d: %v", userID, err)
		return nil, err
	}

	res := make([]string, 0, len(pairs))
	// ZrangebyscoreWithScoresCtx 返回的是升序排列，为了和一般时间线逻辑一致，客户端可能需要降序，
	// 但协议只是返回列表，这里我们倒序放入结果，保证最新的排最前
	for i := len(pairs) - 1; i >= 0; i-- {
		res = append(res, pairs[i].Key)
	}

	return res, nil
}

// IncrSeq 仅递增会话的序号 (基于 Redis Hash 进行递增，保证整个 conversation 存储在 hset 里以便后续缓存查询)
func (c *ConversationDAO) IncrSeq(ctx context.Context, conversationID string) (uint64, error) {
	cacheKey := convInfoPrefix + conversationID

	// 1. 同步写 Redis (使用 Lua 脚本保证 HINCRBY, HSET 和 EXPIRE 的原子性)
	script := `
		local seq = redis.call("HINCRBY", KEYS[1], "max_seq", 1)
		redis.call("HSET", KEYS[1], "update_time", ARGV[1])
		redis.call("EXPIRE", KEYS[1], ARGV[2])
		return seq
	`
	updateTime := fmt.Sprintf("%d", time.Now().UnixMilli())
	expireSecs := int(convInfoExpire.Seconds())

	val, err := c.cache.EvalCtx(ctx, script, []string{cacheKey}, updateTime, strconv.Itoa(expireSecs))
	if err != nil {
		return 0, fmt.Errorf("redis eval lua script for incr max_seq failed: %w", err)
	}

	var seq int64
	switch v := val.(type) {
	case int64:
		seq = v
	case int:
		seq = int64(v)
	default:
		return 0, fmt.Errorf("unexpected return type from redis lua script: %T", val)
	}

	// 如果 seq == 1说明原来缓存里没有，必须从DB加载正确的seq
	if seq == 1 {
		c.cache.HDelCtx(ctx, cacheKey, "max_seq")
		dbSeq, err := c.incrSeqFromDB(ctx, conversationID, cacheKey)
		if err != nil {
			return 0, err
		}
		return uint64(dbSeq), nil
	}

	return uint64(seq), nil
}

// incrSeqFromDB 从 MySQL 递增 max_seq 并回填 Redis 缓存
func (c *ConversationDAO) incrSeqFromDB(ctx context.Context, conversationID, cacheKey string) (int, error) {
	var conv model.Conversation
	err := c.db.WithContext(ctx).
		Where("conversation_id = ?", conversationID).
		First(&conv).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 会话不存在，自动创建，max_seq 初始化为 1
			now := time.Now()
			newConv := model.Conversation{
				ConversationID: conversationID,
				Type:           int8(common.GetConversationType(conversationID)),
				MaxSeq:         1,
				CreateTime:     now,
				UpdateTime:     now,
			}
			if createErr := c.db.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "conversation_id"}},
				DoUpdates: clause.Assignments(map[string]interface{}{"max_seq": gorm.Expr("max_seq + 1")}),
			}).Create(&newConv).Error; createErr != nil {
				return 0, fmt.Errorf("auto create conversation failed: %w", createErr)
			}
			// 回填 Redis，将该Conversation的每一个字段都设置进去
			fields := map[string]string{
				"conversation_id": newConv.ConversationID,
				"type":            fmt.Sprintf("%d", newConv.Type),
				"max_seq":         fmt.Sprintf("%d", newConv.MaxSeq),
				"create_time":     fmt.Sprintf("%d", newConv.CreateTime.UnixMilli()),
				"update_time":     fmt.Sprintf("%d", newConv.UpdateTime.UnixMilli()),
			}
			err = c.cache.HMSetCtx(ctx, cacheKey, fields)
			if err != nil {
				logger.Errorf("redis hmset failed: %v", err)
			}
			c.cache.ExpireCtx(ctx, cacheKey, int(convInfoExpire.Seconds()))
			return int(newConv.MaxSeq), nil
		}
		return 0, fmt.Errorf("query conversation max_seq failed: %w", err)
	}

	// 递增 DB
	newSeq := conv.MaxSeq + 1
	if updateErr := c.db.WithContext(ctx).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Update("max_seq", newSeq).Error; updateErr != nil {
		return 0, fmt.Errorf("update conversation max_seq failed: %w", updateErr)
	}

	// 回填 Redis，将该Conversation的每一个字段都设置进去
	fields := map[string]string{
		"conversation_id": conv.ConversationID,
		"type":            fmt.Sprintf("%d", conv.Type),
		"max_seq":         fmt.Sprintf("%d", newSeq),
		"create_time":     fmt.Sprintf("%d", conv.CreateTime.UnixMilli()),
		"update_time":     fmt.Sprintf("%d", time.Now().UnixMilli()),
	}
	err = c.cache.HMSetCtx(ctx, cacheKey, fields)
	if err != nil {
		logger.Errorf("redis hmset failed: %v", err)
	}
	c.cache.ExpireCtx(ctx, cacheKey, int(convInfoExpire.Seconds()))
	return int(newSeq), nil
}
