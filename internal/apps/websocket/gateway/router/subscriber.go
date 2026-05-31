package router

import (
	"context"
	"fmt"

	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
)

// MessageHandler 消息处理函数，由调用方自行解码
type MessageHandler func(ctx context.Context, data []byte) error

// Subscriber NATS 消息订阅者
type Subscriber struct {
	nc            *nats.Conn
	nodeID        string
	subscriptions []*nats.Subscription
	errHandler    func(error)
}

// NewSubscriber 创建订阅者
func NewSubscriber(nc *nats.Conn, nodeID string, errHandler func(error)) *Subscriber {
	return &Subscriber{
		nc:         nc,
		nodeID:     nodeID,
		errHandler: errHandler,
	}
}

// Subscribe 普通的 NATS 订阅（随拿随走，不断网重试，不持久化负载）
func (s *Subscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	msgHandler := func(msg *nats.Msg) {
		if handler != nil {
			if err := handler(ctx, msg.Data); err != nil && s.errHandler != nil {
				s.errHandler(err)
			}
		}
	}

	sub, err := s.nc.Subscribe(subject, msgHandler)
	if err != nil {
		if s.errHandler != nil {
			s.errHandler(err)
		}
		return fmt.Errorf("subscribe to subject %s failed: %w", subject, err)
	}
	s.subscriptions = append(s.subscriptions, sub)
	logger.Infof("[Subscriber] subscribed to subject %s", subject)

	return nil
}

// QueueSubscribe 普通的 NATS 队列订阅（组内单向负载不落盘）
func (s *Subscriber) QueueSubscribe(ctx context.Context, subject string, queue string, handler MessageHandler) error {
	msgHandler := func(msg *nats.Msg) {
		if handler != nil {
			if err := handler(ctx, msg.Data); err != nil && s.errHandler != nil {
				s.errHandler(err)
			}
		}
	}

	sub, err := s.nc.QueueSubscribe(subject, queue, msgHandler)
	if err != nil {
		if s.errHandler != nil {
			s.errHandler(err)
		}
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
			if err := sub.Unsubscribe(); err != nil && s.errHandler != nil {
				s.errHandler(err)
			}
		}
	}
	return nil
}
