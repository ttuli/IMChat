package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/redisc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (

	convTimelinePrefix = "user:conv:timeline:"
)

// ConversationDAO 会话数据访问对象 (MySQL + Redis缓存)
type ConversationDAO struct {
	db    *gorm.DB
	cache *redisc.RedisModel
}

// NewConversationDAO 创建会话DAO
func NewConversationDAO(dbSource string, redisConf redis.RedisConf) *ConversationDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	return &ConversationDAO{
		db:    db,
		cache: redisc.MustNewRedis(redisConf),
	}
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
	return c.db.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(updates).Error
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

	pairs, err := c.cache.ZrangebyscoreWithScoresCtx(ctx, key, sinceTimestamp+1, time.Now().UnixMilli()+100000)
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
