package pubsub

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/telemetry"
	"IM2/internal/common"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, msg *common.WSMessage) error

// Subscriber NATS 消息订阅者
type Subscriber struct {
	js            nats.JetStreamContext
	codec         protocol.Codec
	nodeID        string
	subscriptions []*nats.Subscription
	telemetryBus  *telemetry.Bus
}

// NewSubscriber 创建订阅者
func NewSubscriber(js nats.JetStreamContext, codec protocol.Codec, nodeID string, bus *telemetry.Bus) *Subscriber {
	return &Subscriber{
		js:           js,
		codec:        codec,
		nodeID:       nodeID,
		telemetryBus: bus,
	}
}

// Subscribe 订阅指定的主题
func (s *Subscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	// 创建闭包处理函数，避免 handler 被覆盖
	msgHandler := func(msg *nats.Msg) {
		internalMsg, err := s.codec.Decode(msg.Data)
		if err != nil {
			s.telemetryBus.Publish(err)
			msg.Nak()
			return
		}

		if handler != nil {
			if err := handler(ctx, internalMsg); err != nil {
				s.telemetryBus.Publish(err)
				msg.Nak()
				return
			}
		}
		msg.Ack()
	}

	// 1. 订阅指定主题
	sub, err := s.js.Subscribe(subject, msgHandler)
	if err != nil {
		s.telemetryBus.Publish(err)
		return fmt.Errorf("subscribe to subject %s failed: %w", subject, err)
	}
	s.subscriptions = append(s.subscriptions, sub)
	logger.Infof("[Subscriber] subscribed to subject %s", subject)

	return nil
}

// QueueSubscribe 队列订阅（负载均衡模式），同一个 queue 组内的节点只有一个会消费消息
func (s *Subscriber) QueueSubscribe(ctx context.Context, subject string, queue string, handler MessageHandler) error {
	msgHandler := func(msg *nats.Msg) {
		internalMsg, err := s.codec.Decode(msg.Data)
		if err != nil {
			s.telemetryBus.Publish(err)
			msg.Nak()
			return
		}

		if handler != nil {
			if err := handler(ctx, internalMsg); err != nil {
				s.telemetryBus.Publish(err)
				msg.Nak()
				return
			}
		}
		msg.Ack()
	}

	sub, err := s.js.QueueSubscribe(subject, queue, msgHandler)
	if err != nil {
		s.telemetryBus.Publish(err)
		return fmt.Errorf("queue subscribe to subject %s failed: %w", subject, err)
	}
	s.subscriptions = append(s.subscriptions, sub)
	logger.Infof("[Subscriber] queue subscribed to subject %s with queue %s", subject, queue)

	return nil
}

// Close 关闭订阅者
func (s *Subscriber) Close() error {
	for _, sub := range s.subscriptions {
		if sub != nil {
			if err := sub.Unsubscribe(); err != nil {
				s.telemetryBus.Publish(err)
			}
		}
	}
	return nil
}
