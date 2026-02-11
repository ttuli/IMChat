package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, msg *protocol.InternalMessage) error

// Subscriber Redis Pub/Sub 订阅者
type Subscriber struct {
	client    *redis.Client
	nodeID    string
	pubsub    *redis.PubSub
	handler   MessageHandler
	closeChan chan struct{}
}

// NewSubscriber 创建订阅者
func NewSubscriber(client *redis.Client, nodeID string) *Subscriber {
	return &Subscriber{
		client:    client,
		nodeID:    nodeID,
		closeChan: make(chan struct{}),
	}
}

// Subscribe 订阅本节点消息通道
func (s *Subscriber) Subscribe(ctx context.Context, handler MessageHandler) error {
	channel := fmt.Sprintf("ws:channel:%s", s.nodeID)
	s.pubsub = s.client.Subscribe(ctx, channel)
	s.handler = handler

	// 等待订阅确认
	_, err := s.pubsub.Receive(ctx)
	if err != nil {
		return fmt.Errorf("subscribe to channel failed: %w", err)
	}

	logx.Infof("[Subscriber] subscribed to channel %s", channel)

	// 启动消息处理协程
	go s.readLoop(ctx)

	return nil
}

// readLoop 消息读取循环
func (s *Subscriber) readLoop(ctx context.Context) {
	ch := s.pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeChan:
			return
		case msg, ok := <-ch:
			if !ok {
				logx.Slow("[Subscriber] channel closed")
				return
			}

			var internalMsg protocol.InternalMessage
			if err := json.Unmarshal([]byte(msg.Payload), &internalMsg); err != nil {
				logx.Errorf("[Subscriber] unmarshal message failed: %v", err)
				continue
			}

			if s.handler != nil {
				if err := s.handler(ctx, &internalMsg); err != nil {
					logx.Errorf("[Subscriber] handle message failed: %v", err)
				}
			}
		}
	}
}

// Close 关闭订阅者
func (s *Subscriber) Close() error {
	close(s.closeChan)
	if s.pubsub != nil {
		return s.pubsub.Close()
	}
	return nil
}
