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

	return &MessageDAO{db: client.Database(mongoDbName)}
}

// InsertMessage 写入消息
func (m *MessageDAO) InsertMessage(ctx context.Context, msg *model.Message) error {
	_, err := m.db.Collection(mongoCollMessage).InsertOne(ctx, msg)
	return err
}

// FindByConversation 按会话查询消息 (基于 Seq 游标分页)
// cursorSeq: 上一页最后一条消息的 Seq，首次传 0 表示从最新开始
// 返回按 seq DESC 排列的消息列表
func (m *MessageDAO) FindByConversation(ctx context.Context, conversationID string, cursorSeq uint64, limit int) ([]*model.Message, error) {
	filter := bson.M{
		"conversation_id": conversationID,
	}

	if cursorSeq > 0 {
		filter["seq"] = bson.M{"$lt": cursorSeq}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "seq", Value: -1}}).
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
