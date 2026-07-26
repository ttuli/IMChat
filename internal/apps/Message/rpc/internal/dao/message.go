package dao

import (
	"context"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	model "IM2/internal/model"
	"IM2/pkg/proto/transport"

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

// CountUnread 统计会话内 seq 大于游标且非本人发送的「聊天」消息数。
// Lamport seq 不连续后未读数不能再用减法计算，改为服务端点查。
// seq 现为事件流：通知类消息（群操作 606 / 撤回 605）与聊天消息同库、同样占用 seq，
// 但不是用户需要"读"的消息，必须按 msg_type 排除，否则他人的群操作/撤回会虚增未读。
//
// **通话记录 CHAT_CALL(106) 有意不排除**：未接来电必须有未读红点，这是该记录的主要价值。
// 记录的 from_user_id 恒为主叫，故只有被叫侧可能计入未读，主叫不会为自己拨出的电话产生未读。
// 由此带来的副作用——被叫在「正常通话结束」「自己拒接」后同样会 +1——
// 由客户端在这两种 end_reason 下改调 reportSessionRead 推进游标消解，
// 不在本查询里按 end_reason 细分（reason 埋在 payload 内，查询侧不可见，
// 提上来需要给通用消息模型加通话专属列，代价与收益不成比例）。
//
// limit 限制扫描上限（超过按 limit 返回），防止长期未读会话拖垮查询。
func (m *MessageDAO) CountUnread(ctx context.Context, sessionID string, afterSeq uint64, excludeUser uint64) (uint64, error) {
	filter := bson.M{
		"session_id":   sessionID,
		"seq":          bson.M{"$gt": afterSeq},
		"from_user_id": bson.M{"$ne": excludeUser},
		"msg_type": bson.M{"$nin": bson.A{
			int32(transport.MessageType_MSG_OP_RECALL),
			int32(transport.MessageType_GROUP_OP_NOTIFICATION),
		}},
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

// UpdateMessageStatusCAS 原子地将消息状态置为 status（仅当前状态不同才更新，CAS 语义）。
// 返回是否由本次调用完成变更：并发重复操作（如双击撤回、多端同时撤回）时
// 只有一个调用者拿到 true，由其独占后续副作用（如发布撤回通知）。
func (m *MessageDAO) UpdateMessageStatusCAS(ctx context.Context, msgID string, status int8) (bool, error) {
	res, err := m.db.Collection(mongoCollMessage).UpdateOne(
		ctx,
		bson.M{"msg_id": msgID, "status": bson.M{"$ne": status}},
		bson.M{"$set": bson.M{"status": status}},
	)
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
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
