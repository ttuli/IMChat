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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	redisMsgIdPrefix  = "msg:id:"
	redisConvIdPrefix = "conv:id:"
	redisSeqPrefix    = "conv:seq:"

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

func (m *MessageDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return m.DB.WithContext(ctx).Transaction(fc)
}

// ==================== 消息操作 (MongoDB) ====================

// InsertMessage 插入消息
func (m *MessageDAO) InsertMessage(ctx context.Context, msg *model.Message) error {
	msg.CreateTime = time.Now()
	// 如果 ID 为空，生成新的 ObjectID
	if msg.ID.IsZero() {
		msg.ID = primitive.NewObjectID()
	}

	_, err := m.Mongo.Collection(mongoCollMessage).InsertOne(ctx, msg)
	if err != nil {
		return err
	}
	m.setMsgCache(ctx, msg)
	return nil
}

// FindByID 根据主键查找消息
func (m *MessageDAO) FindByID(ctx context.Context, id primitive.ObjectID) (*model.Message, error) {
	// 1. 查缓存
	cacheKey := fmt.Sprintf("%s%s", redisMsgIdPrefix, id.Hex())
	res, _ := m.Redis.Get(cacheKey)
	if res != "" {
		var msg model.Message
		if err := json.Unmarshal([]byte(res), &msg); err == nil {
			return &msg, nil
		}
		m.Redis.Del(cacheKey)
	}

	// 2. 查 MongoDB
	var msg model.Message
	err := m.Mongo.Collection(mongoCollMessage).FindOne(ctx, bson.M{"_id": id}).Decode(&msg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, gorm.ErrRecordNotFound // 保持错误类型一致性
		}
		return nil, err
	}

	// 3. 写缓存
	m.setMsgCache(ctx, &msg)
	return &msg, nil
}

// FindByMsgID 根据客户端消息ID查找 (幂等校验)
func (m *MessageDAO) FindByMsgID(ctx context.Context, msgID string) (*model.Message, error) {
	var msg model.Message
	err := m.Mongo.Collection(mongoCollMessage).FindOne(ctx, bson.M{"msg_id": msgID}).Decode(&msg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}
	return &msg, nil
}

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

// UpdateStatus 更新消息状态 (撤回/删除)
func (m *MessageDAO) UpdateStatus(ctx context.Context, id primitive.ObjectID, status int8) error {
	// 1. 删缓存
	m.deleteMsgCache(ctx, id.Hex())

	// 2. 更新 MongoDB
	_, err := m.Mongo.Collection(mongoCollMessage).UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": status}},
	)
	if err != nil {
		return err
	}

	// 3. 延迟双删
	m.delayDeleteMsgCache(id.Hex())
	return nil
}

// GetNextSeq 获取下一个消息序号
func (m *MessageDAO) GetNextSeq(ctx context.Context, conversationID string) (uint64, error) {
	key := fmt.Sprintf("%s%s", redisSeqPrefix, conversationID)
	seq, err := m.Redis.IncrCtx(ctx, key)
	if err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

// ==================== 会话操作 (MySQL) ====================

// FindOrCreateConversation 查找或创建会话
func (m *MessageDAO) FindOrCreateConversation(ctx context.Context, conversationID string, convType int8) (*model.Conversation, error) {
	var conv model.Conversation
	err := m.DB.WithContext(ctx).
		Where("conversation_id = ?", conversationID).
		First(&conv).Error

	if err == nil {
		return &conv, nil
	}

	if err != gorm.ErrRecordNotFound {
		return nil, err
	}

	// 创建新会话
	conv = model.Conversation{
		ConversationID: conversationID,
		Type:           convType,
	}
	if err := m.DB.WithContext(ctx).Create(&conv).Error; err != nil {
		// 并发创建时可能冲突，再查一次
		var existing model.Conversation
		if e := m.DB.WithContext(ctx).Where("conversation_id = ?", conversationID).First(&existing).Error; e == nil {
			return &existing, nil
		}
		return nil, err
	}
	return &conv, nil
}

// UpdateConversationLastMsg 更新会话最后消息
func (m *MessageDAO) UpdateConversationLastMsg(ctx context.Context, conversationID string, msgID uint64, msgTime time.Time, seq uint64) error {
	return m.DB.WithContext(ctx).Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Updates(map[string]any{
			"last_msg_id":   msgID,
			"last_msg_time": msgTime,
			"max_seq":       seq,
		}).Error
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

// FindOrCreateUserConversation 查找或创建用户会话关系
func (m *MessageDAO) FindOrCreateUserConversation(ctx context.Context, userID uint64, conversationID string) (*model.UserConversation, error) {
	var uc model.UserConversation
	err := m.DB.WithContext(ctx).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		First(&uc).Error

	if err == nil {
		return &uc, nil
	}

	if err != gorm.ErrRecordNotFound {
		return nil, err
	}

	uc = model.UserConversation{
		UserID:         userID,
		ConversationID: conversationID,
	}
	if err := m.DB.WithContext(ctx).Create(&uc).Error; err != nil {
		var existing model.UserConversation
		if e := m.DB.WithContext(ctx).Where("user_id = ? AND conversation_id = ?", userID, conversationID).First(&existing).Error; e == nil {
			return &existing, nil
		}
		return nil, err
	}
	return &uc, nil
}

// IncrUnreadCount 增加未读计数
func (m *MessageDAO) IncrUnreadCount(ctx context.Context, userID uint64, conversationID string, delta int32) error {
	return m.DB.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Update("unread_count", gorm.Expr("unread_count + ?", delta)).Error
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

// ==================== 缓存辅助方法 ====================

func (m *MessageDAO) setMsgCache(ctx context.Context, msg *model.Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Errorf("序列化消息数据失败: %v", err)
		return
	}

	cacheKey := fmt.Sprintf("%s%s", redisMsgIdPrefix, msg.ID.Hex())
	if err := m.Redis.SetexCtx(ctx, cacheKey, string(data), cacheExpireSeconds); err != nil {
		logger.Errorf("设置消息缓存失败: %v", err)
	}
}

func (m *MessageDAO) deleteMsgCache(ctx context.Context, idStr string) {
	cacheKey := fmt.Sprintf("%s%s", redisMsgIdPrefix, idStr)
	if _, err := m.Redis.DelCtx(ctx, cacheKey); err != nil {
		logger.Errorf("删除消息缓存失败: %v", err)
	}
}

func (m *MessageDAO) delayDeleteMsgCache(idStr string) {
	go func() {
		time.Sleep(500 * time.Millisecond)
		cacheKey := fmt.Sprintf("%s%s", redisMsgIdPrefix, idStr)
		if _, err := m.Redis.Del(cacheKey); err != nil {
			logger.Errorf("延迟删除消息缓存失败: %v", err)
		}
	}()
}
