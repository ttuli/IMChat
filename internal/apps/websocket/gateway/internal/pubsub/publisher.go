package pubsub

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/types"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
	// "github.com/zeromicro/go-zero/core/logx"
)

// Publisher NATS 消息发布者
type Publisher struct {
	conn   *nats.Conn
	codec  protocol.Codec
	nodeID string
}

// NewPublisher 创建发布者
func NewPublisher(conn *nats.Conn, codec protocol.Codec, nodeID string) *Publisher {
	return &Publisher{
		conn:   conn,
		codec:  codec,
		nodeID: nodeID,
	}
}

// PublishToNode 发布消息到指定节点
func (p *Publisher) PublishToNode(ctx context.Context, nodeID string, msg *types.InternalMessage) error {
	subject := getNodeSubject(nodeID)

	data, err := p.codec.EncodeInternal(msg)
	if err != nil {
		return err
	}

	if err := p.conn.Publish(subject, data); err != nil {
		return err
	}

	logger.Infof("[Publisher] published message to node %s for user %d", nodeID, msg.TargetUserId)
	return nil
}

func (p *Publisher) PublishToDB(ctx context.Context, msg *types.WSMessage) error {
	subject := getDBSubject()

	data, err := p.codec.Encode(msg)
	if err != nil {
		return err
	}

	if err := p.conn.Publish(subject, data); err != nil {
		return err
	}

	logger.Infof("[Publisher] published message to db for user")
	return nil
}

// getNodeSubject 获取节点消息 subject
func getNodeSubject(nodeID string) string {
	return fmt.Sprintf("ws.channel.%s", nodeID)
}

func getDBSubject() string {
	return "ws.db"
}
