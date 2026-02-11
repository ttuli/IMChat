package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/xerr"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// Publisher Redis Pub/Sub 发布者
type Publisher struct {
	client *redis.Client
	nodeID string
}

// NewPublisher 创建发布者
func NewPublisher(client *redis.Client, nodeID string) *Publisher {
	return &Publisher{
		client: client,
		nodeID: nodeID,
	}
}

// PublishToNode 发布消息到指定节点
func (p *Publisher) PublishToNode(ctx context.Context, nodeID string, msg *protocol.InternalMessage) error {
	channel := getNodeChannel(nodeID)

	data, err := json.Marshal(msg)
	if err != nil {
		return xerr.Wrap(err, xerr.ErrEncoding, "marshal internal message failed")
	}

	if err := p.client.Publish(ctx, channel, data).Err(); err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "publish message failed")
	}

	logx.Debugf("[Publisher] published message to node %s for user %d", nodeID, msg.TargetUserID)
	return nil
}

// getNodeChannel 获取节点消息通道名
func getNodeChannel(nodeID string) string {
	return fmt.Sprintf("ws:channel:%s", nodeID)
}
