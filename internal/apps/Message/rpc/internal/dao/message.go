package dao

import (
	"context"
	"time"

	"IM2/internal/model"
	"IM2/pkg/redisc"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	redisConvIdPrefix = "conv:id:"

	mongoDbName      = "im2"
	mongoCollMessage = "message"
)

// 缓存过期时间
const (
	cacheExpireSeconds = 3600 // 1小时
)

// MessageDAO 消息数据访问对象
// 采用混合存储：MongoDB (消息) + MySQL (会话)
type MessageDAO struct {
	*gorm.DB
	*redisc.RedisModel
	Mongo *mongo.Database
}

// NewMessageDAO 创建消息DAO
func NewMessageDAO(dbSource string, redisSource redis.RedisConf, mongoUri string) *MessageDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// 初始化 MongoDB 客户端
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		panic(err)
	}

	mongoDb := client.Database(mongoDbName)
	return &MessageDAO{
		DB:         db,
		RedisModel: redisc.MustNewRedis(redisSource),
		Mongo:      mongoDb,
	}
}

// ==================== 消息操作 (MongoDB) ====================

// FindByConversation 按会话查询消息 (基于 Seq 分页)
// cursorSeq: 上一页最后一条消息的 Seq，首次传 0 (或最大值)
// limit: 每页条数
// 返回按 seq DESC 排列的消息列表
func (m *MessageDAO) FindByConversation(ctx context.Context, conversationID string, cursorSeq uint64, limit int) ([]*model.Message, error) {
	var messages []*model.Message

	filter := bson.M{
		"conversation_id": conversationID,
		"status":          model.MsgStatusNormal,
	}

	// 如果 cursorSeq > 0，查询 seq < cursorSeq 的消息
	if cursorSeq > 0 {
		filter["seq"] = bson.M{"$lt": cursorSeq}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "seq", Value: -1}}). // 按 seq 倒序
		SetLimit(int64(limit))

	cursor, err := m.Mongo.Collection(mongoCollMessage).Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

// ==================== 会话操作 (MySQL) ====================

// FindConversationsByIDs 批量查询会话
func (m *MessageDAO) FindConversationsByIDs(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	var convs []*model.Conversation
	if err := m.DB.WithContext(ctx).
		Where("conversation_id IN ?", conversationIDs).
		Find(&convs).Error; err != nil {
		return nil, err
	}
	return convs, nil
}

// ==================== 用户会话操作 (MySQL) ====================

// FindUserConversations 查询用户的会话列表 (按最后消息时间倒序)
func (m *MessageDAO) FindUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	var userConvs []*model.UserConversation
	if err := m.DB.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("update_time DESC").
		Find(&userConvs).Error; err != nil {
		return nil, err
	}
	return userConvs, nil
}

// ClearUnread 清零未读并更新已读游标
func (m *MessageDAO) ClearUnread(ctx context.Context, userID uint64, conversationID string, lastReadMsgID, lastReadSeq uint64) error {
	return m.DB.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(map[string]any{
			"unread_count":     0,
			"last_read_msg_id": lastReadMsgID,
			"last_read_seq":    lastReadSeq,
		}).Error
}

// UpdateUserConversation 更新用户会话设置 (置顶/免打扰/静音)
func (m *MessageDAO) UpdateUserConversation(ctx context.Context, userID uint64, conversationID string, updates map[string]any) error {
	return m.DB.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(updates).Error
}
