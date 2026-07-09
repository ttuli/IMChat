package dao

import (
	"context"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	model "IM2/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoDbName      = "im2"
	mongoCollMessage = "message"
)

// MessageDAO 消息数据访问对象 (MongoDB)
type MessageDAO struct {
	c  config.MessageDAOConfig
	db *mongo.Database
}

// NewMessageDAO 创建消息DAO
func NewMessageDAO(c config.MessageDAOConfig) *MessageDAO {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(c.Dbsource))
	if err != nil {
		panic(err)
	}

	dao := &MessageDAO{c: c, db: client.Database(mongoDbName)}
	_ = dao.EnsureIndexes(context.Background())
	return dao
}

// EnsureIndexes 创建索引：
//  1. {client_id, msg_id} 联合唯一索引（幂等去重）
//  2. {session_id, seq} 复合索引（历史消息范围查询 / 未读计数 / seq 播种）
func (m *MessageDAO) EnsureIndexes(ctx context.Context) error {
	collection := m.db.Collection(mongoCollMessage)
	_, err := collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "client_id", Value: 1},
				{Key: "msg_id", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "session_id", Value: 1},
				{Key: "seq", Value: 1},
			},
		},
	})
	return err
}

// InsertMessage 写入消息
func (m *MessageDAO) InsertMessage(ctx context.Context, msg *model.Message) error {
	_, err := m.db.Collection(mongoCollMessage).InsertOne(ctx, msg)
	return err
}

// InsertMessages 批量写入消息
func (m *MessageDAO) InsertMessages(ctx context.Context, msgs []*model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	docs := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		docs[i] = msg
	}
	_, err := m.db.Collection(mongoCollMessage).InsertMany(ctx, docs)
	return err
}

// FindByConversation 按会话做范围查询（基于 Seq 区间分页）。
// startSeq: 区间起始（含），负数表示无下界。
// endSeq:   区间终止（含），负数表示无上界。
// 排序规则：startSeq≥0 且 endSeq<0（向新消息拉取）→ ASC；其余 → DESC（向旧消息拉取）。
func (m *MessageDAO) FindByConversation(ctx context.Context, conversationID string, startSeq, endSeq int64, limit int) ([]*model.Message, error) {
	seqFilter := bson.M{}
	if startSeq >= 0 {
		seqFilter["$gte"] = startSeq
	}
	if endSeq >= 0 {
		seqFilter["$lte"] = endSeq
	}

	filter := bson.M{"session_id": conversationID}
	if len(seqFilter) > 0 {
		filter["seq"] = seqFilter
	}

	// startSeq 有下界、endSeq 无上界 → 向新消息方向拉取，升序
	sortOrder := -1
	if startSeq >= 0 && endSeq < 0 {
		sortOrder = 1
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "seq", Value: sortOrder}}).
		SetLimit(int64(limit))

	cursor, err := m.db.Collection(mongoCollMessage).Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var messages []*model.Message
	if err = cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

// MaxSeq 返回会话当前已持久化的最大 seq，会话无消息时返回 0。
// 用于 Lamport 分配器进程启动后的播种，防止重启/时钟回拨导致 seq 回退。
func (m *MessageDAO) MaxSeq(ctx context.Context, sessionID string) (uint64, error) {
	opts := options.FindOne().
		SetSort(bson.D{{Key: "seq", Value: -1}}).
		SetProjection(bson.M{"seq": 1})

	var msg model.Message
	err := m.db.Collection(mongoCollMessage).
		FindOne(ctx, bson.M{"session_id": sessionID}, opts).
		Decode(&msg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return msg.Seq, nil
}

// CountUnread 统计会话内 seq 大于游标且非本人发送的消息数。
// Lamport seq 不连续后未读数不能再用减法计算，改为服务端点查。
// limit 限制扫描上限（超过按 limit 返回），防止长期未读会话拖垮查询。
func (m *MessageDAO) CountUnread(ctx context.Context, sessionID string, afterSeq uint64, excludeUser uint64) (uint64, error) {
	filter := bson.M{
		"session_id":   sessionID,
		"seq":          bson.M{"$gt": afterSeq},
		"from_user_id": bson.M{"$ne": excludeUser},
	}
	opts := options.Count()
	if m.c.UnreadCountLimit > 0 {
		opts.SetLimit(m.c.UnreadCountLimit)
	}
	n, err := m.db.Collection(mongoCollMessage).CountDocuments(ctx, filter, opts)
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
}

// FindByMsgID 根据 msg_id 查询单条消息
func (m *MessageDAO) FindByMsgID(ctx context.Context, msgID string) (*model.Message, error) {
	var msg model.Message
	err := m.db.Collection(mongoCollMessage).FindOne(ctx, bson.M{"msg_id": msgID}).Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// UpdateMessageStatus 更新消息状态（0-正常 1-撤回 2-删除）
func (m *MessageDAO) UpdateMessageStatus(ctx context.Context, msgID string, status int8) error {
	_, err := m.db.Collection(mongoCollMessage).UpdateOne(
		ctx,
		bson.M{"msg_id": msgID},
		bson.M{"$set": bson.M{"status": status}},
	)
	return err
}

// FindBySenderAndClient 根据发送者ID和客户端ID查询消息 (用于幂等判断)
func (m *MessageDAO) FindBySenderAndClient(ctx context.Context, fromUserID uint64, clientID string) (*model.Message, error) {
	if clientID == "" {
		return nil, mongo.ErrNoDocuments
	}
	var msg model.Message
	err := m.db.Collection(mongoCollMessage).FindOne(ctx, bson.M{
		"from_user_id": fromUserID,
		"client_id":    clientID,
	}).Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
