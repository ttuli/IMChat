package pubsub

import (
	"context"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/common"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
)

type SubjectConfig struct {
	NodeSubjectPrefix string
	DBSubject         string
}

// Publisher NATS 消息发布者
type Publisher struct {
	conn   *nats.Conn
	codec  protocol.Codec
	nodeID string
	subjectConfig SubjectConfig
}

// NewPublisher 创建发布者
func NewPublisher(conn *nats.Conn, codec protocol.Codec, nodeID string, subjectConfig SubjectConfig) *Publisher {
	return &Publisher{
		conn:          conn,
		codec:         codec,
		nodeID:        nodeID,
		subjectConfig: subjectConfig,
	}
}

// PublishToNode 发布消息到指定节点
func (p *Publisher) PublishToNode(ctx context.Context, nodeID string, msg *common.InternalMessage) error {
	subject := p.getNodeSubject(nodeID)

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

func (p *Publisher) PublishToDB(ctx context.Context, msg *common.WSMessage) error {
	subject := p.getDBSubject()

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
func (p *Publisher) getNodeSubject(nodeID string) string {
	return fmt.Sprintf("%s%s", p.subjectConfig.NodeSubjectPrefix, nodeID)
}

func (p *Publisher) getDBSubject() string {
	return p.subjectConfig.DBSubject
}
