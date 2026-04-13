package pubsub

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/internal/telemetry"
	"IM2/pkg/proto/transport"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, msg *transport.WSMessage) error

// Subscriber NATS 消息订阅者
type Subscriber struct {
	nc            *nats.Conn
	codec         protocol.Codec
	nodeID        string
	subscriptions []*nats.Subscription
	telemetryBus  *telemetry.Bus
}

// NewSubscriber 创建订阅者
func NewSubscriber(nc *nats.Conn, codec protocol.Codec, nodeID string, bus *telemetry.Bus) *Subscriber {
	return &Subscriber{
		nc:           nc,
		codec:        codec,
		nodeID:       nodeID,
		telemetryBus: bus,
	}
}

// Subscribe 普通的 NATS 订阅（随拿随走，不断网重试，不持久化负载）
func (s *Subscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	msgHandler := func(msg *nats.Msg) {
		internalMsg, err := s.codec.Decode(msg.Data)
		if err != nil {
			s.telemetryBus.Publish(err)
			return
		}

		if handler != nil {
			if err := handler(ctx, internalMsg); err != nil {
				s.telemetryBus.Publish(err)
			}
		}
	}

	sub, err := s.nc.Subscribe(subject, msgHandler)
	if err != nil {
		s.telemetryBus.Publish(err)
		return fmt.Errorf("subscribe to subject %s failed: %w", subject, err)
	}
	s.subscriptions = append(s.subscriptions, sub)
	logger.Infof("[Subscriber] subscribed to subject %s", subject)

	return nil
}

// QueueSubscribe 普通的 NATS 队列订阅（组内单向负载不落盘）
func (s *Subscriber) QueueSubscribe(ctx context.Context, subject string, queue string, handler MessageHandler) error {
	msgHandler := func(msg *nats.Msg) {
		internalMsg, err := s.codec.Decode(msg.Data)
		if err != nil {
			s.telemetryBus.Publish(err)
			return
		}

		if handler != nil {
			if err := handler(ctx, internalMsg); err != nil {
				s.telemetryBus.Publish(err)
			}
		}
	}

	sub, err := s.nc.QueueSubscribe(subject, queue, msgHandler)
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
