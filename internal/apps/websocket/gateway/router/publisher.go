package router

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/proto/transport"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
)

// BroadcastMode 定义广播消息的消费模式
type BroadcastMode int

const (
	// BroadcastAll 所有节点都收到消息并消费（fan-out 广播）
	BroadcastAll BroadcastMode = iota
	// BroadcastQueue 同一个 Queue Group 内只有一个节点消费（负载均衡）
	BroadcastQueue
)

type SubjectConfig struct {
	NodeSubjectPrefix     string
	DBSubject             string
	BroadcastSubject      string
	QueueBroadcastSubject string
}

// Publisher NATS 消息发布者
type Publisher struct {
	nc            *nats.Conn
	codec         protocol.Codec
	nodeID        string
	subjectConfig SubjectConfig
}

// NewPublisher 创建发布者
func NewPublisher(nc *nats.Conn, codec protocol.Codec, nodeID string, subjectConfig SubjectConfig) *Publisher {
	return &Publisher{
		nc:            nc,
		codec:         codec,
		nodeID:        nodeID,
		subjectConfig: subjectConfig,
	}
}

// PublishToNode 发布消息到指定节点
func (p *Publisher) PublishToNode(ctx context.Context, nodeID string, msg *transport.WSMessage) error {
	subject := p.getNodeSubject(nodeID)

	data, err := p.codec.Encode(msg)
	if err != nil {
		return err
	}

	if err := p.nc.Publish(subject, data); err != nil {
		return err
	}

	logger.Infof("[Publisher] published message to node %s for target %v", nodeID, msg.RouteTarget)
	return nil
}

// BroadcastToAllNodes 广播消息到所有节点（或通过 Queue 仅让一个节点消费）
//   - BroadcastAll:   消息发布到 BroadcastSubject，所有节点都会收到（fan-out）
//   - BroadcastQueue: 消息发布到 QueueBroadcastSubject，仅 Queue Group 内的一个节点消费（负载均衡）
func (p *Publisher) BroadcastToAllNodes(ctx context.Context, msg *transport.WSMessage, mode BroadcastMode) error {
	var subject string
	switch mode {
	case BroadcastQueue:
		subject = p.subjectConfig.QueueBroadcastSubject
	default:
		subject = p.subjectConfig.BroadcastSubject
	}

	data, err := p.codec.Encode(msg)
	if err != nil {
		return err
	}

	if err := p.nc.Publish(subject, data); err != nil {
		return err
	}

	logger.Infof("[Publisher] broadcasted message (mode=%d) to target %v", mode, msg.RouteTarget)
	return nil
}

func (p *Publisher) PublishToDB(ctx context.Context, msg *transport.WSMessage) error {
	subject := p.subjectConfig.DBSubject

	data, err := p.codec.Encode(msg)
	if err != nil {
		return err
	}

	if err := p.nc.Publish(subject, data); err != nil {
		return err
	}

	logger.Infof("[Publisher] published message to db for user")
	return nil
}

// getNodeSubject 获取节点消息 subject
func (p *Publisher) getNodeSubject(nodeID string) string {
	return fmt.Sprintf("%s%s", p.subjectConfig.NodeSubjectPrefix, nodeID)
}
