package nats_util

import (
	"fmt"
	"sync"

	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Client 包装 NATS 连接（类比 pkg/redisx 之于 Redis）：
// 统一连接参数，并把集群内部消息的发布/订阅行为（subject 选择、序列化、
// 订阅句柄追踪、错误日志）收敛到本类型；各服务持有 Client 而非裸 *nats.Conn。
type Client struct {
	conn *nats.Conn
	js   nats.JetStreamContext

	mu   sync.Mutex
	subs []*nats.Subscription
}

// NewClient 建立 NATS 连接并初始化 JetStream 上下文，url 为空时使用默认地址。
// 统一开启无限重连：客户端默认在断线约 2 分钟（60 次 × 2s）后放弃重连，
// 长驻服务会永久失联直到进程重启。
func NewClient(url string) (*Client, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	conn, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{conn: conn, js: js}, nil
}

// Conn 返回底层连接，供订阅等未封装的原生操作使用
func (c *Client) Conn() *nats.Conn { return c.conn }

// JetStream 返回 JetStream 上下文（需要持久化/去重保证的发布与消费）
func (c *Client) JetStream() nats.JetStreamContext { return c.js }

// Close 关闭底层连接
func (c *Client) Close() { c.conn.Close() }

// Broadcast 向全节点广播一条 WSMessage（fan-out，所有网关节点都会收到，
// 各自按 RouteTarget/DeliverTo 过滤本地连接投递）。
// 序列化或发布失败仅记录日志：调用方数据均已落库，允许静默丢弃通知，不阻塞主流程。
func (c *Client) Broadcast(msg *transport.WSMessage) {
	c.publish(BroadcastSubject, msg)
}

// PublishToNode 向指定网关节点的专属 subject 单播一条 WSMessage。
func (c *Client) PublishToNode(nodeID string, msg *transport.WSMessage) {
	c.publish(NodeSubjectPrefix+nodeID, msg)
}

// publish 序列化并发布到指定 subject，失败仅记录日志
func (c *Client) publish(subject string, msg *transport.WSMessage) {
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("[nats] marshal message failed: %v", err)
		return
	}
	if err := c.conn.Publish(subject, data); err != nil {
		logger.Errorf("[nats] publish to %s failed: %v", subject, err)
	}
}

// Subscribe 订阅 subject，解码为 WSMessage 后交给 handler；解码失败的消息记录日志并丢弃。
// 订阅句柄由 Client 追踪，调用方不需要单独持有 *nats.Subscription。
func (c *Client) Subscribe(subject string, handler func(*transport.WSMessage)) error {
	sub, err := c.conn.Subscribe(subject, func(natsMsg *nats.Msg) {
		msg := &transport.WSMessage{}
		if err := proto.Unmarshal(natsMsg.Data, msg); err != nil {
			logger.Errorf("[nats] unmarshal message from %s failed: %v", subject, err)
			return
		}
		handler(msg)
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s failed: %w", subject, err)
	}
	c.mu.Lock()
	c.subs = append(c.subs, sub)
	c.mu.Unlock()
	return nil
}

// Unsubscribe 注销所有通过 Subscribe 建立的订阅，停止消费新消息。
// 与 Close 分离：部分调用方的连接生命周期由外层统一管理，关闭前只需先停止消费。
func (c *Client) Unsubscribe() {
	c.mu.Lock()
	subs := c.subs
	c.subs = nil
	c.mu.Unlock()
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil && err != nats.ErrConnectionClosed {
			logger.Errorf("[nats] unsubscribe failed: %v", err)
		}
	}
}
