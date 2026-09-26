package listener

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"IM2/internal/apps/Message/rpc/internal/service"
	nats_util "IM2/pkg/nats"
	protosvc "IM2/pkg/proto/svc"

	"github.com/nats-io/nats.go"
)

// fakeDLQ 记录 finishMsg 的转存决策
type fakeDLQ struct {
	calls []DeadLetter
	err   error
}

func (f *fakeDLQ) Handle(_ context.Context, dl DeadLetter) error {
	f.calls = append(f.calls, dl)
	return f.err
}

func newTestListener(dlq DLQHandler) *NatsListener {
	l := &NatsListener{dlq: dlq, maxDeliver: 5}
	l.consumerCreated.Store(consumerCreatedUnknown)
	return l
}

// unboundMsg 未绑定连接的消息：Ack/Nak 返回 ErrInvalidConnection 而不会真的发出，
// 足以验证 finishMsg 的路由决策（转不转 DLQ、带什么上下文）
func unboundMsg() *nats.Msg {
	return &nats.Msg{Subject: nats_util.DBSubject, Data: []byte("payload"), Sub: &nats.Subscription{}}
}

var errTransient = errors.New("mongo: server selection timeout")

func TestFinishSuccessNeverDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	newTestListener(dlq).finishMsg(unboundMsg(), nil, service.Delivery{NumDelivered: 5}, nil)
	if len(dlq.calls) != 0 {
		t.Fatalf("successful message must not go to DLQ, got %d calls", len(dlq.calls))
	}
}

// 瞬时错误在阈值之前只退避重投，不转 DLQ
func TestFinishTransientRetriesBelowThreshold(t *testing.T) {
	dlq := &fakeDLQ{}
	l := newTestListener(dlq)
	for n := uint64(1); n < uint64(l.maxDeliver); n++ {
		l.finishMsg(unboundMsg(), nil, service.Delivery{NumDelivered: n}, errTransient)
	}
	if len(dlq.calls) != 0 {
		t.Fatalf("transient failure below maxDeliver must retry, got %d DLQ calls", len(dlq.calls))
	}
}

func TestFinishTransientExhaustedGoesToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	l := newTestListener(dlq)
	send := &protosvc.MessageSend{SessionKey: "s-1", ClientId: "c-1", Sender: 42}
	d := service.Delivery{StreamSeq: 1001, NumDelivered: uint64(l.maxDeliver)}

	l.finishMsg(unboundMsg(), send, d, errTransient)

	if len(dlq.calls) != 1 {
		t.Fatalf("exhausted message must go to DLQ exactly once, got %d", len(dlq.calls))
	}
	got := dlq.calls[0]
	if got.Poison {
		t.Fatal("transient failure must not be classified as poison")
	}
	if got.Send != send || got.Delivery != d || !errors.Is(got.Reason, errTransient) {
		t.Fatalf("dead letter lost context: %+v", got)
	}
}

// 毒消息首投即转 DLQ：重试不会成功，退避只会白白延迟 2.6 分钟。
// 错误经过 service → process 两层 %w 包装，errors.Is 必须仍能识别。
func TestFinishPoisonGoesToDLQImmediately(t *testing.T) {
	dlq := &fakeDLQ{}
	inner := fmt.Errorf("%w: unmarshal image payload: bad wire type", service.ErrPoison)
	wrapped := fmt.Errorf("[NatsListener] PersistMessage error: %w", inner)

	newTestListener(dlq).finishMsg(unboundMsg(), nil, service.Delivery{NumDelivered: 1}, wrapped)

	if len(dlq.calls) != 1 || !dlq.calls[0].Poison {
		t.Fatalf("poison message must go to DLQ on first delivery, got %+v", dlq.calls)
	}
}

// 转存失败时不能 Ack（这里无法观测 Ack/Nak，只验证不 panic 且确实尝试过转存）；
// 真正的保证由 serverMaxDeliver = -1 提供：消息会被重投回来再次尝试转存
func TestFinishDLQFailureDoesNotDropSilently(t *testing.T) {
	dlq := &fakeDLQ{err: errors.New("nats: timeout")}
	l := newTestListener(dlq)
	l.finishMsg(unboundMsg(), nil, service.Delivery{NumDelivered: uint64(l.maxDeliver)}, errTransient)
	if len(dlq.calls) != 1 {
		t.Fatalf("expected one DLQ attempt, got %d", len(dlq.calls))
	}
}

func TestNakDelay(t *testing.T) {
	cases := []struct {
		n    uint64
		want time.Duration
	}{
		{0, time.Second}, // 元数据缺失，按首次失败处理
		{1, time.Second},
		{2, 5 * time.Second},
		{3, 30 * time.Second},
		{4, 2 * time.Minute},
		{5, 2 * time.Minute}, // 超出表长按最后一档
		{100, 2 * time.Minute},
	}
	for _, c := range cases {
		if got := nakDelay(c.n); got != c.want {
			t.Errorf("nakDelay(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

// jsMsgAt 构造一条带 JetStream 元数据的消息，写入时间为 stored
func jsMsgAt(numDelivered, streamSeq uint64, stored time.Time) *nats.Msg {
	m := unboundMsg()
	// $JS.ACK.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>
	m.Reply = fmt.Sprintf("$JS.ACK.%s.%s.%d.%d.%d.%s.0",
		nats_util.StreamName, durableConsumerName, numDelivered, streamSeq, streamSeq,
		strconv.FormatInt(stored.UnixNano(), 10))
	return m
}

func TestDeliveryOfPredatesConsumer(t *testing.T) {
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	l := newTestListener(&fakeDLQ{})
	l.consumerCreated.Store(created.UnixNano())

	before := l.deliveryOf(jsMsgAt(1, 7, created.Add(-time.Minute)))
	if !before.PredatesConsumer || !before.MaybeDuplicate() {
		t.Fatalf("message stored before consumer creation must be dedup-checked: %+v", before)
	}
	if before.StreamSeq != 7 || before.NumDelivered != 1 {
		t.Fatalf("metadata not parsed: %+v", before)
	}

	after := l.deliveryOf(jsMsgAt(1, 8, created.Add(time.Minute)))
	if after.PredatesConsumer || after.MaybeDuplicate() {
		t.Fatalf("first delivery of a message stored after consumer creation should skip dedup: %+v", after)
	}
}

// 取不到 consumer 创建时间时（初始值 / 刷新失败），所有消息都必须走判重
func TestDeliveryOfUnknownConsumerIsConservative(t *testing.T) {
	l := newTestListener(&fakeDLQ{})
	d := l.deliveryOf(jsMsgAt(1, 9, time.Now()))
	if !d.MaybeDuplicate() {
		t.Fatalf("unknown consumer creation time must force dedup: %+v", d)
	}
}
