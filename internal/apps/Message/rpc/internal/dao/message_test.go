package dao

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	model "IM2/internal/model"
	"IM2/pkg/proto/transport"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// 消息桶依赖的是一组 MongoDB 服务端行为（upsert+sort 选桶、$elemMatch 投影、
// messages.$ 位置操作符、复合多键索引等），这些无法用纯内存 fake 验证，
// 因此本文件是集成测试：设置 MESSAGE_DAO_TEST_URI 后才运行。
//
//	MESSAGE_DAO_TEST_URI=mongodb://localhost:27017 go test ./internal/apps/Message/rpc/internal/dao/...
//
// 测试会在 im2 库的 message_bucket 集合里写入 session_id 前缀为 "test-" 的数据，
// 每个用例开始前清理自己的会话。
func newTestDAO(t *testing.T, bucketSize int) *MessageDAO {
	t.Helper()
	uri := os.Getenv("MESSAGE_DAO_TEST_URI")
	if uri == "" {
		t.Skip("MESSAGE_DAO_TEST_URI not set, skipping MongoDB integration test")
	}
	d := NewMessageDAO(config.MessageDAOConfig{
		Dbsource:         uri,
		UnreadCountLimit: 100,
		BucketSize:       bucketSize,
	})
	if err := d.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes failed (复合多键索引/唯一索引是否被服务端接受): %v", err)
	}
	return d
}

func resetSession(t *testing.T, d *MessageDAO, sessionID string) {
	t.Helper()
	if _, err := d.coll().DeleteMany(context.Background(), bson.M{"session_id": sessionID}); err != nil {
		t.Fatalf("cleanup session %s: %v", sessionID, err)
	}
}

func testMsg(sessionID string, seq uint64, from uint64, msgType int16) *model.Message {
	return &model.Message{
		MsgID:      fmt.Sprintf("m-%d", seq),
		ClientID:   fmt.Sprintf("c-%d", seq),
		SessionID:  sessionID,
		SessionKey: sessionID,
		FromUserID: from,
		MsgType:    msgType,
		Seq:        seq,
		Content:    fmt.Sprintf("content-%d", seq),
		CreateTime: time.UnixMilli(1700000000000 + int64(seq)),
	}
}

// 追加超过一桶容量的消息，应滚出多个桶，且每个满桶恰好 bucketSize 条。
func TestAppendRollsOverBuckets(t *testing.T) {
	const size = 10
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-rollover"
	resetSession(t, d, sessionID)

	for seq := uint64(1); seq <= 25; seq++ {
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, 1, 101)}); err != nil {
			t.Fatalf("append seq %d: %v", seq, err)
		}
	}

	cursor, err := d.coll().Find(ctx, bson.M{"session_id": sessionID})
	if err != nil {
		t.Fatal(err)
	}
	var buckets []model.MessageBucket
	if err := cursor.All(ctx, &buckets); err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets for 25 messages at size %d, got %d", size, len(buckets))
	}
	total := 0
	for _, b := range buckets {
		if int(b.Count) != len(b.Messages) {
			t.Errorf("bucket count=%d but len(messages)=%d", b.Count, len(b.Messages))
		}
		if b.Count > size {
			t.Errorf("bucket overflowed capacity: count=%d size=%d", b.Count, size)
		}
		// $min/$max 必须如实覆盖桶内区间，否则区间相交查询会漏桶
		for _, msg := range b.Messages {
			if msg.Seq < b.MinSeq || msg.Seq > b.MaxSeq {
				t.Errorf("message seq %d outside bucket range [%d,%d]", msg.Seq, b.MinSeq, b.MaxSeq)
			}
		}
		total += len(b.Messages)
	}
	if total != 25 {
		t.Fatalf("expected 25 messages persisted, got %d", total)
	}
}

// 一次追加多条（BulkPersistMessages 路径）跨越桶边界时同样不能超容量。
func TestAppendChunkSplitsAcrossBuckets(t *testing.T) {
	const size = 10
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-chunk"
	resetSession(t, d, sessionID)

	batch := make([]*model.Message, 0, 23)
	for seq := uint64(1); seq <= 23; seq++ {
		batch = append(batch, testMsg(sessionID, seq, 1, 101))
	}
	if err := d.AppendMessages(ctx, sessionID, batch); err != nil {
		t.Fatalf("bulk append: %v", err)
	}

	n, err := d.coll().CountDocuments(ctx, bson.M{"session_id": sessionID, "count": bson.M{"$gt": size}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d buckets exceeded capacity %d", n, size)
	}
	if got, _ := d.MaxSeq(ctx, sessionID); got != 23 {
		t.Fatalf("MaxSeq = %d, want 23", got)
	}
}

// 历史拉取必须跨桶正确合并、按 seq 排序、遵守 limit 与区间边界。
func TestFindByConversationAcrossBuckets(t *testing.T) {
	const size = 10
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-history"
	resetSession(t, d, sessionID)

	for seq := uint64(1); seq <= 35; seq++ {
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, 1, 101)}); err != nil {
			t.Fatal(err)
		}
	}

	// 无界 + DESC：取最新 12 条，应为 35..24
	msgs, err := d.FindByConversation(ctx, sessionID, -1, -1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 12 {
		t.Fatalf("DESC unbounded: got %d messages, want 12", len(msgs))
	}
	for i, msg := range msgs {
		if want := uint64(35 - i); msg.Seq != want {
			t.Fatalf("DESC unbounded[%d]: seq=%d, want %d", i, msg.Seq, want)
		}
	}

	// 有下界无上界 → ASC（向新消息拉取），从 20 开始取 12 条
	msgs, err = d.FindByConversation(ctx, sessionID, 20, -1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 12 {
		t.Fatalf("ASC: got %d messages, want 12", len(msgs))
	}
	for i, msg := range msgs {
		if want := uint64(20 + i); msg.Seq != want {
			t.Fatalf("ASC[%d]: seq=%d, want %d", i, msg.Seq, want)
		}
	}

	// 双边界 → DESC，且严格落在区间内
	msgs, err = d.FindByConversation(ctx, sessionID, 8, 22, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 15 {
		t.Fatalf("bounded: got %d messages, want 15 (seq 8..22)", len(msgs))
	}
	for _, msg := range msgs {
		if msg.Seq < 8 || msg.Seq > 22 {
			t.Fatalf("bounded: seq %d outside [8,22]", msg.Seq)
		}
	}
	// 内容字段要能完整还原（$replaceRoot 后的解码）
	if msgs[0].Content != "content-22" || msgs[0].SessionID != sessionID {
		t.Fatalf("decoded message mismatch: %+v", msgs[0])
	}
}

// 跨实例并发消费同一会话时写入顺序可能与 seq 顺序不一致，
// 桶区间用 $min/$max 如实维护后，乱序写入不能影响读取结果。
func TestFindByConversationWithOutOfOrderWrites(t *testing.T) {
	const size = 5
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-outoforder"
	resetSession(t, d, sessionID)

	for _, seq := range []uint64{3, 1, 5, 2, 4, 8, 6, 7} {
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, 1, 101)}); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := d.FindByConversation(ctx, sessionID, -1, -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 8 {
		t.Fatalf("got %d messages, want 8", len(msgs))
	}
	for i, msg := range msgs {
		if want := uint64(8 - i); msg.Seq != want {
			t.Fatalf("[%d]: seq=%d, want %d (乱序写入后读取仍须按 seq 有序)", i, msg.Seq, want)
		}
	}
}

// 未读计数须跨桶累加，并排除通知类消息与本人发送的消息。
func TestCountUnreadAcrossBuckets(t *testing.T) {
	const size = 5
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-unread"
	resetSession(t, d, sessionID)

	const me, peer = uint64(1), uint64(2)
	for seq := uint64(1); seq <= 20; seq++ {
		from, msgType := peer, int16(101)
		switch {
		case seq%5 == 0: // 本人发送，不计未读
			from = me
		case seq%7 == 0: // 撤回通知，占 seq 但不计未读
			msgType = int16(transport.MessageType_MSG_OP_RECALL)
		}
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, from, msgType)}); err != nil {
			t.Fatal(err)
		}
	}

	// 游标 8 之后：seq 9..20 共 12 条，扣掉本人发的 10/15/20 与撤回通知 14 → 8 条
	got, err := d.CountUnread(ctx, sessionID, 8, me)
	if err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Fatalf("CountUnread = %d, want 8", got)
	}

	// 游标已到最新：0 条
	if got, err = d.CountUnread(ctx, sessionID, 20, me); err != nil || got != 0 {
		t.Fatalf("CountUnread after latest = %d (err=%v), want 0", got, err)
	}
}

// 撤回链路：点查 + CAS 置状态，且 CAS 只能成功一次。
func TestFindByMsgIDAndRecallCAS(t *testing.T) {
	const size = 5
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-recall"
	resetSession(t, d, sessionID)

	for seq := uint64(1); seq <= 12; seq++ {
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, 1, 101)}); err != nil {
			t.Fatal(err)
		}
	}

	msg, err := d.FindByMsgID(ctx, "m-7")
	if err != nil {
		t.Fatalf("FindByMsgID: %v", err)
	}
	// $elemMatch 投影必须只返回命中的那一条，而不是桶内第一条
	if msg.MsgID != "m-7" || msg.Seq != 7 {
		t.Fatalf("FindByMsgID returned wrong message: %+v", msg)
	}

	if _, err = d.FindByMsgID(ctx, "m-nonexistent"); err != mongo.ErrNoDocuments {
		t.Fatalf("FindByMsgID(missing) err = %v, want ErrNoDocuments", err)
	}

	const recalled = int8(1)
	ok, err := d.UpdateMessageStatusCAS(ctx, "m-7", recalled)
	if err != nil || !ok {
		t.Fatalf("first CAS: ok=%v err=%v, want true/nil", ok, err)
	}
	// 重复撤回必须返回 false，否则会重复发布撤回通知
	if ok, err = d.UpdateMessageStatusCAS(ctx, "m-7", recalled); err != nil || ok {
		t.Fatalf("second CAS: ok=%v err=%v, want false/nil", ok, err)
	}

	msg, err = d.FindByMsgID(ctx, "m-7")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Status != recalled {
		t.Fatalf("status = %d, want %d", msg.Status, recalled)
	}
	// 位置操作符只能改中标的那一条，同桶的邻居不受影响
	for _, id := range []string{"m-6", "m-8"} {
		neighbor, err := d.FindByMsgID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if neighbor.Status != 0 {
			t.Fatalf("%s status = %d, want 0 (位置操作符改错了数组元素)", id, neighbor.Status)
		}
	}
}

// 幂等判重：按 (from_user_id, client_id) 能命中桶内已落库的消息。
func TestFindBySenderAndClient(t *testing.T) {
	const size = 5
	d := newTestDAO(t, size)
	ctx := context.Background()
	sessionID := "test-idempotency"
	resetSession(t, d, sessionID)

	for seq := uint64(1); seq <= 12; seq++ {
		if err := d.AppendMessages(ctx, sessionID, []*model.Message{testMsg(sessionID, seq, 42, 101)}); err != nil {
			t.Fatal(err)
		}
	}

	msg, err := d.FindBySenderAndClient(ctx, 42, "c-9")
	if err != nil {
		t.Fatalf("FindBySenderAndClient: %v", err)
	}
	if msg.MsgID != "m-9" || msg.Seq != 9 {
		t.Fatalf("returned wrong message: %+v", msg)
	}

	// 发送者不匹配不能命中
	if _, err = d.FindBySenderAndClient(ctx, 43, "c-9"); err != mongo.ErrNoDocuments {
		t.Fatalf("wrong sender err = %v, want ErrNoDocuments", err)
	}
	// 空 client_id 不查库直接返回未找到（服务端铸造的消息走这条路）
	if _, err = d.FindBySenderAndClient(ctx, 42, ""); err != mongo.ErrNoDocuments {
		t.Fatalf("empty clientID err = %v, want ErrNoDocuments", err)
	}
}

// 会话无消息时 MaxSeq 返回 0，用于 seq 分配器播种。
func TestMaxSeqEmptySession(t *testing.T) {
	d := newTestDAO(t, 10)
	ctx := context.Background()
	sessionID := "test-empty"
	resetSession(t, d, sessionID)

	got, err := d.MaxSeq(ctx, sessionID)
	if err != nil {
		t.Fatalf("MaxSeq on empty session: %v", err)
	}
	if got != 0 {
		t.Fatalf("MaxSeq = %d, want 0", got)
	}
}
