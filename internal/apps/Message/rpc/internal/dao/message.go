package dao

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoDbName = "im2"
	// mongoCollBucket 消息桶集合：一个文档承载同一会话的 BucketSize 条消息
	mongoCollBucket = "message_bucket"

	// DefaultBucketSize 单桶默认容量
	DefaultBucketSize = 100

	// bucketScanPad 历史查询的桶扫描冗余量。
	// 正常情况一个会话只有最后一个桶未满，limit/BucketSize+1 个桶即可取满一页；
	// 多留几个是为了覆盖并发建桶造成的碎片桶（见 AppendMessages 注释）。
	bucketScanPad = 4
)

// MessageDAO 消息数据访问对象 (MongoDB，桶存储)
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
	// mongo.Connect 是惰性的：它只解析 URI、不建立连接，Mongo 完全不可达时也返回 nil error。
	// 而服务的健康检查 /healthz 是静态 200 —— 它的语义「初始化已完成」成立的前提，
	// 是所有依赖都在 HTTP 监听之前 fail-fast（MySQL 走 gorm.Open 会 ping、Redis 同理）。
	// 不显式探活的话，Mongo 挂掉时 Pod 仍会被判定 Ready 并接入流量，
	// 直到第一个查历史消息的请求才失败。
	if err := client.Ping(ctx, nil); err != nil {
		panic(fmt.Errorf("connect mongo failed: %w", err))
	}

	dao := &MessageDAO{c: c, db: client.Database(mongoDbName)}
	// 建索引失败不阻塞启动（可能是并发实例已在建、或权限不足），但必须留痕：
	// 桶存储的读写路径全部依赖这几个索引，静默失败会退化成全集合扫描
	if err := dao.EnsureIndexes(context.Background()); err != nil {
		logger.Errorf("[MessageDAO] ensure indexes on %s failed: %v", mongoCollBucket, err)
	}
	return dao
}

// bucketSize 单桶容量，未配置时取默认值
func (m *MessageDAO) bucketSize() int {
	if m.c.BucketSize > 0 {
		return m.c.BucketSize
	}
	return DefaultBucketSize
}

func (m *MessageDAO) coll() *mongo.Collection {
	return m.db.Collection(mongoCollBucket)
}

// EnsureIndexes 创建桶集合索引：
//  1. {session_id, min_seq} 唯一索引——桶身份；同时服务「找最新未满桶」的写路径
//     与历史消息的区间查询排序
//  2. {session_id, max_seq} ——seq 播种（取会话最大 seq）与向新方向拉取时的区间下界
//  3. {messages.msg_id} multikey——按 msg_id 点查/撤回 CAS
//  4. {messages.client_id, messages.from_user_id} multikey——消费重投时的幂等判重。
//     两个字段同属 messages 数组（非并行数组），可以建复合多键索引。
//
// 注意 3、4 都不能加 unique：多键唯一索引只约束「文档之间」不重复，
// 同一文档的数组内部仍可出现重复值——而重投产生的重复消息恰恰大概率落在同一个桶里。
// 去重因此由写路径显式判重完成（见 FindBySenderAndClient 的调用方）。
func (m *MessageDAO) EnsureIndexes(ctx context.Context) error {
	_, err := m.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "session_id", Value: 1},
				{Key: "min_seq", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "session_id", Value: 1},
				{Key: "max_seq", Value: -1},
			},
		},
		{
			Keys: bson.D{{Key: "messages.msg_id", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "messages.client_id", Value: 1},
				{Key: "messages.from_user_id", Value: 1},
			},
		},
	})
	return err
}

// AppendMessages 将同一会话的一批消息追加到该会话的桶中。
// 调用方必须保证 msgs 全部属于 sessionID。
//
// 单次 $push 的条数不能超过桶容量（否则「追加后不超容量」的条件无解），超出则拆批。
func (m *MessageDAO) AppendMessages(ctx context.Context, sessionID string, msgs []*model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	size := m.bucketSize()
	for start := 0; start < len(msgs); {
		end := start + size
		if end > len(msgs) {
			end = len(msgs)
		}
		if err := m.appendChunk(ctx, sessionID, msgs[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// appendChunk 一次原子操作完成「找最新未满桶 → 追加 → 无可用桶则建新桶」。
//
// 用 FindOneAndUpdate 而非 UpdateOne，是因为只有前者支持 sort：
// 没有 sort 时会命中「最旧的未满桶」，新消息被灌进老桶，桶的 seq 区间将大面积重叠，
// 历史查询每次都要扫更多桶。按 min_seq 倒序取，命中的就是会话最新的那个桶。
//
// 并发：同一桶的并发追加由 MongoDB 的单文档原子性串行化，count 条件保证不会超容量。
// 「所有桶都满」的瞬间多个实例可能各 upsert 出一个新桶——这是良性的：两个半满桶区间
// 不重叠，读路径合并后结果完全正确，只是碎片化一点，不值得为它引入分布式锁。
func (m *MessageDAO) appendChunk(ctx context.Context, sessionID string, chunk []*model.Message) error {
	minSeq, maxSeq := chunk[0].Seq, chunk[0].Seq
	docs := make([]any, 0, len(chunk))
	for _, msg := range chunk {
		if msg.Seq < minSeq {
			minSeq = msg.Seq
		}
		if msg.Seq > maxSeq {
			maxSeq = msg.Seq
		}
		docs = append(docs, msg)
	}
	now := time.Now()

	// count <= size-len(chunk)：保证追加后不超过桶容量（单条追加时即 count < size）
	filter := bson.M{
		"session_id": sessionID,
		"count":      bson.M{"$lte": m.bucketSize() - len(chunk)},
	}
	// session_id 由 filter 的等值条件带入新文档，不能再出现在 $setOnInsert（会报路径冲突）；
	// $min/$max 在字段不存在时直接赋值，新桶自动得到 min_seq/max_seq，无需特判。
	update := bson.M{
		// $sort 让桶内数组始终按 seq 有序（乱序写入时也成立）。它只是可读性/局部性上的
		// 优化——读路径 FindByConversation 展开后还会再排一次，去掉不影响正确性。
		"$push": bson.M{"messages": bson.D{
			{Key: "$each", Value: docs},
			{Key: "$sort", Value: bson.D{{Key: "seq", Value: 1}}},
		}},
		"$inc":         bson.M{"count": len(chunk)},
		"$min":         bson.M{"min_seq": minSeq},
		"$max":         bson.M{"max_seq": maxSeq},
		"$set":         bson.M{"update_time": now},
		"$setOnInsert": bson.M{"create_time": now},
	}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetSort(bson.D{{Key: "min_seq", Value: -1}}).
		SetProjection(bson.M{"_id": 1}) // 只回 _id，避免把整桶消息拉回来

	// upsert 建新桶时按默认的 ReturnDocument=Before 语义返回 ErrNoDocuments，非错误
	err := m.coll().FindOneAndUpdate(ctx, filter, update, opts).Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	return nil
}

// FindByConversation 按会话做范围查询（基于 Seq 区间分页）。
// startSeq: 区间起始（含），负数表示无下界。
// endSeq:   区间终止（含），负数表示无上界。
// 排序规则：startSeq≥0 且 endSeq<0（向新消息拉取）→ ASC；其余 → DESC（向旧消息拉取）。
//
// 桶存储下分两级过滤：先按桶区间与查询区间「相交」筛出候选桶（走 {session_id, min_seq}
// 索引），再在服务端 $unwind 展开、按 seq 精确过滤排序，最后 $limit 截断——
// 只有最终的 limit 条消息会回传，不会把整桶拉过来。
func (m *MessageDAO) FindByConversation(ctx context.Context, conversationID string, startSeq, endSeq int64, limit int) ([]*model.Message, error) {
	// $limit 不接受非正数（旧实现的 Find().SetLimit(0) 是「不限制」，语义不同），
	// 调用方（GetHistory）已把 limit 收敛到 1..100，这里只做兜底
	if limit <= 0 {
		limit = DefaultBucketSize
	}

	bucketFilter := bson.M{"session_id": conversationID}
	if startSeq >= 0 {
		bucketFilter["max_seq"] = bson.M{"$gte": startSeq}
	}
	if endSeq >= 0 {
		bucketFilter["min_seq"] = bson.M{"$lte": endSeq}
	}

	// startSeq 有下界、endSeq 无上界 → 向新消息方向拉取，升序
	sortOrder := -1
	if startSeq >= 0 && endSeq < 0 {
		sortOrder = 1
	}

	// 扫描桶数上限：正常只需 limit/size+1 个桶，冗余量覆盖并发建桶造成的碎片桶
	bucketLimit := int64(limit/m.bucketSize() + 1 + bucketScanPad)

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bucketFilter}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "min_seq", Value: sortOrder}}}},
		bson.D{{Key: "$limit", Value: bucketLimit}},
		bson.D{{Key: "$unwind", Value: "$messages"}},
		bson.D{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$messages"}}},
	}

	seqFilter := bson.M{}
	if startSeq >= 0 {
		seqFilter["$gte"] = startSeq
	}
	if endSeq >= 0 {
		seqFilter["$lte"] = endSeq
	}
	if len(seqFilter) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.M{"seq": seqFilter}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "seq", Value: sortOrder}}}},
		bson.D{{Key: "$limit", Value: int64(limit)}},
	)

	cursor, err := m.coll().Aggregate(ctx, pipeline)
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
// 桶存储下无需展开消息：取 max_seq 最大的那个桶即可（走 {session_id, max_seq} 索引）。
func (m *MessageDAO) MaxSeq(ctx context.Context, sessionID string) (uint64, error) {
	opts := options.FindOne().
		SetSort(bson.D{{Key: "max_seq", Value: -1}}).
		SetProjection(bson.M{"max_seq": 1})

	var bucket model.MessageBucket
	err := m.coll().FindOne(ctx, bson.M{"session_id": sessionID}, opts).Decode(&bucket)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return bucket.MaxSeq, nil
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
// UnreadCountLimit 限制扫描上限（超过按 limit 返回），防止长期未读会话拖垮查询。
// 管道里 $match/$unwind/$match 全是流式阶段，$limit 能让游标提前收敛，
// 扫描上限的语义与旧的 CountDocuments(SetLimit) 一致。
func (m *MessageDAO) CountUnread(ctx context.Context, sessionID string, afterSeq uint64, excludeUser uint64) (uint64, error) {
	pipeline := mongo.Pipeline{
		// 桶级预过滤：max_seq <= 游标的桶整桶跳过
		bson.D{{Key: "$match", Value: bson.M{
			"session_id": sessionID,
			"max_seq":    bson.M{"$gt": afterSeq},
		}}},
		bson.D{{Key: "$unwind", Value: "$messages"}},
		bson.D{{Key: "$match", Value: bson.M{
			"messages.seq":          bson.M{"$gt": afterSeq},
			"messages.from_user_id": bson.M{"$ne": excludeUser},
			"messages.msg_type": bson.M{"$nin": bson.A{
				int32(transport.MessageType_MSG_OP_RECALL),
				int32(transport.MessageType_GROUP_OP_NOTIFICATION),
			}},
		}}},
	}
	if m.c.UnreadCountLimit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: m.c.UnreadCountLimit}})
	}
	pipeline = append(pipeline, bson.D{{Key: "$count", Value: "n"}})

	cursor, err := m.coll().Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var result []struct {
		N int64 `bson:"n"`
	}
	if err = cursor.All(ctx, &result); err != nil {
		return 0, err
	}
	if len(result) == 0 {
		return 0, nil
	}
	return uint64(result[0].N), nil
}

// FindByMsgID 根据 msg_id 查询单条消息。
// $elemMatch 投影让 MongoDB 只回传数组里命中的那一个元素，不回传整桶。
func (m *MessageDAO) FindByMsgID(ctx context.Context, msgID string) (*model.Message, error) {
	return m.findEmbedded(ctx,
		bson.M{"messages.msg_id": msgID},
		bson.M{"msg_id": msgID},
	)
}

// FindBySenderAndClient 根据发送者ID和客户端ID查询消息 (用于幂等判断)
func (m *MessageDAO) FindBySenderAndClient(ctx context.Context, fromUserID uint64, clientID string) (*model.Message, error) {
	if clientID == "" {
		return nil, mongo.ErrNoDocuments
	}
	elem := bson.M{"client_id": clientID, "from_user_id": fromUserID}
	return m.findEmbedded(ctx, bson.M{"messages": bson.M{"$elemMatch": elem}}, elem)
}

// findEmbedded 按 filter 定位桶，再用 $elemMatch 投影取出命中的那条内嵌消息。
// 桶存在但无命中元素时统一返回 mongo.ErrNoDocuments，保持与单文档时代一致的语义。
func (m *MessageDAO) findEmbedded(ctx context.Context, filter bson.M, elemMatch bson.M) (*model.Message, error) {
	opts := options.FindOne().SetProjection(bson.M{
		"messages": bson.M{"$elemMatch": elemMatch},
	})

	var bucket model.MessageBucket
	if err := m.coll().FindOne(ctx, filter, opts).Decode(&bucket); err != nil {
		return nil, err
	}
	if len(bucket.Messages) == 0 {
		return nil, mongo.ErrNoDocuments
	}
	return bucket.Messages[0], nil
}

// UpdateMessageStatus 更新消息状态（0-正常 1-撤回 2-删除）
func (m *MessageDAO) UpdateMessageStatus(ctx context.Context, msgID string, status int8) error {
	_, err := m.coll().UpdateOne(
		ctx,
		bson.M{"messages.msg_id": msgID},
		bson.M{"$set": bson.M{"messages.$.status": status}},
	)
	return err
}

// UpdateMessageStatusCAS 原子地将消息状态置为 status（仅当前状态不同才更新，CAS 语义）。
// 返回是否由本次调用完成变更：并发重复操作（如双击撤回、多端同时撤回）时
// 只有一个调用者拿到 true，由其独占后续副作用（如发布撤回通知）。
//
// filter 必须用 $elemMatch 把两个条件约束到同一个数组元素上：
// 写成 {"messages.msg_id": id, "messages.status": {$ne: status}} 是错的——
// 两个条件可以由桶内**不同的**消息分别满足，CAS 语义就破了。
// 位置操作符 messages.$ 定位的正是满足 $elemMatch 的那个元素。
func (m *MessageDAO) UpdateMessageStatusCAS(ctx context.Context, msgID string, status int8) (bool, error) {
	res, err := m.coll().UpdateOne(
		ctx,
		bson.M{"messages": bson.M{"$elemMatch": bson.M{
			"msg_id": msgID,
			"status": bson.M{"$ne": status},
		}}},
		bson.M{"$set": bson.M{"messages.$.status": status}},
	)
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}
