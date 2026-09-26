package listener

import (
	"context"
	"errors"
	"testing"

	"IM2/internal/apps/Message/rpc/internal/service"
	nats_util "IM2/pkg/nats"
	protosvc "IM2/pkg/proto/svc"

	"github.com/nats-io/nats.go"
)

// fakeJS 只实现 PublishMsg；嵌入接口满足类型约束，调用其他方法会 panic（本测试不会调用）
type fakeJS struct {
	nats.JetStreamContext
	published *nats.Msg
	err       error
}

func (f *fakeJS) PublishMsg(m *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	f.published = m
	if f.err != nil {
		return nil, f.err
	}
	return &nats.PubAck{Stream: nats_util.DLQStreamName}, nil
}

type nopNotifier struct{}

func (nopNotifier) Notify(context.Context, string, string) error { return nil }

// 死信必须带齐定位信息：Data 是 protobuf 二进制，排查与重放只能靠 Header
func TestDLQHandlerWritesLocatorHeaders(t *testing.T) {
	js := &fakeJS{}
	h := NewNatsDLQHandler(js, nats_util.DLQSubject, nopNotifier{})
	orig := &nats.Msg{Subject: nats_util.DBSubject, Data: []byte("raw-proto")}

	err := h.Handle(context.Background(), DeadLetter{
		Msg:      orig,
		Send:     &protosvc.MessageSend{SessionKey: "g-100", ClientId: "01J9X", Sender: 42, MsgType: 201},
		Reason:   errors.New("mongo down"),
		Delivery: service.Delivery{StreamSeq: 1001, NumDelivered: 5},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	m := js.published
	if m == nil || m.Subject != nats_util.DLQSubject || string(m.Data) != "raw-proto" {
		t.Fatalf("dead letter not published verbatim to DLQ subject: %+v", m)
	}
	want := map[string]string{
		HeaderOriginalSubject: nats_util.DBSubject,
		HeaderStreamSeq:       "1001",
		HeaderErrorClass:      ErrorClassExhausted,
		HeaderRetryCount:      "5",
		HeaderSessionKey:      "g-100",
		HeaderSender:          "42",
		HeaderClientID:        "01J9X",
		HeaderMsgType:         "201",
		HeaderDeadReason:      "mongo down",
	}
	for k, v := range want {
		if got := m.Header.Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

// 毒消息反序列化失败时没有业务上下文，仍要能转存，并标记 poison 供重放跳过
func TestDLQHandlerPoisonWithoutParsedMessage(t *testing.T) {
	js := &fakeJS{}
	h := NewNatsDLQHandler(js, nats_util.DLQSubject, nopNotifier{})

	err := h.Handle(context.Background(), DeadLetter{
		Msg:      &nats.Msg{Subject: nats_util.DBSubject, Data: []byte{0xff}},
		Reason:   service.ErrPoison,
		Poison:   true,
		Delivery: service.Delivery{StreamSeq: 7, NumDelivered: 1},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := js.published.Header.Get(HeaderErrorClass); got != ErrorClassPoison {
		t.Fatalf("error class = %q, want %q", got, ErrorClassPoison)
	}
}

// PubAck 失败必须返回错误：调用方据此不 Ack 原消息
func TestDLQHandlerPropagatesPublishFailure(t *testing.T) {
	h := NewNatsDLQHandler(&fakeJS{err: errors.New("no responders")}, nats_util.DLQSubject, nopNotifier{})
	err := h.Handle(context.Background(), DeadLetter{
		Msg:    &nats.Msg{Subject: nats_util.DBSubject},
		Reason: errors.New("x"),
	})
	if err == nil {
		t.Fatal("publish failure must surface, otherwise the original message is acked and lost")
	}
}
