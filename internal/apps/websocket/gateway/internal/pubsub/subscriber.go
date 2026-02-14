package pubsub

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/core/logx"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, msg *protocol.InternalMessage) error

// Subscriber NATS 消息订阅者
type Subscriber struct {
	conn         *nats.Conn
	codec        protocol.Codec
	nodeID       string
	subscription *nats.Subscription
	handler      MessageHandler
	ctx          context.Context
}

// NewSubscriber 创建订阅者
func NewSubscriber(conn *nats.Conn, codec protocol.Codec, nodeID string) *Subscriber {
	return &Subscriber{
		conn:   conn,
		codec:  codec,
		nodeID: nodeID,
	}
}

// Subscribe 订阅本节点消息
func (s *Subscriber) Subscribe(ctx context.Context, handler MessageHandler) error {
	subject := fmt.Sprintf("ws.channel.%s", s.nodeID)
	s.handler = handler
	s.ctx = ctx

	sub, err := s.conn.Subscribe(subject, s.handleNatsMessage)
	if err != nil {
		return fmt.Errorf("subscribe to subject failed: %w", err)
	}

	s.subscription = sub
	logx.Infof("[Subscriber] subscribed to subject %s", subject)
	return nil
}

// handleNatsMessage 处理 NATS 消息
func (s *Subscriber) handleNatsMessage(msg *nats.Msg) {
	internalMsg, err := s.codec.DecodeInternal(msg.Data)
	if err != nil {
		logx.Errorf("[Subscriber] unmarshal message failed: %v", err)
		return
	}

	if s.handler != nil {
		if err := s.handler(s.ctx, internalMsg); err != nil {
			logx.Errorf("[Subscriber] handle message failed: %v", err)
		}
	}
}

// Close 关闭订阅者
func (s *Subscriber) Close() error {
	if s.subscription != nil {
		return s.subscription.Unsubscribe()
	}
	return nil
}
