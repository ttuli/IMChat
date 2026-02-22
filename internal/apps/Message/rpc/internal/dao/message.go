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
