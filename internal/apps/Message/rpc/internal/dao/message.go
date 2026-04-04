package dao

import (
	"context"
	"time"

	"IM2/internal/model"

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
	db *mongo.Database
}

// NewMessageDAO 创建消息DAO
func NewMessageDAO(mongoUri string) *MessageDAO {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		panic(err)
	}

	dao := &MessageDAO{db: client.Database(mongoDbName)}
	_ = dao.EnsureIndexes(context.Background())
	return dao
}

// EnsureIndexes 创建联合唯一索引
func (m *MessageDAO) EnsureIndexes(ctx context.Context) error {
	collection := m.db.Collection(mongoCollMessage)
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "client_id", Value: 1},
			{Key: "msg_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
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

	filter := bson.M{"conversation_id": conversationID}
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
