package dao

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/redisc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	redisConvIdPrefix = "conv:id:"

	// 缓存过期时间
	cacheExpireSeconds = 3600 // 1小时
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
func (c *ConversationDAO) InsertUserConversation(ctx context.Context, userId uint64, conversationId string) error {
	return c.db.WithContext(ctx).Create(&model.UserConversation{
		UserID:         userId,
		ConversationID: conversationId,
		IsTop:          false,
		IsDisturb:      false,
		IsMute:         false,
		LastReadSeq:    0,
	}).Error
}

// Transaction 提供开启事务的能力
func (c *ConversationDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return c.db.WithContext(ctx).Transaction(fc)
}

// BatchInsertUserConversations 批量插入用户会话记录
func (c *ConversationDAO) BatchInsertUserConversations(ctx context.Context, userConvs []*model.UserConversation) error {
	if len(userConvs) == 0 {
		return nil
	}
	return c.db.WithContext(ctx).Create(&userConvs).Error
}
