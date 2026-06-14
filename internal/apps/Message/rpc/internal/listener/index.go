package listener

import (
	"context"
	"fmt"
	"sync"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	// 单条读取等待超时
	fetchWaitTimeout = 200 * time.Millisecond
	// 持久化消费者名称（多实例共享，NATS 负载均衡分配消息）
	durableConsumerName = "message_db_consumer"
)

// NatsListener 监听 NATS 消息，委托 MessageService 完成业务处理（持久化等）
type NatsListener struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	c      config.Config
	svc    *service.MessageService
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewNatsListener(c config.Config, conn *nats.Conn, js nats.JetStreamContext, svc *service.MessageService) *NatsListener {
	ctx, cancel := context.WithCancel(context.Background())
	return &NatsListener{
		conn:   conn,
		js:     js,
		c:      c,
		svc:    svc,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (l *NatsListener) Listen() error {
	err := nats_util.InitStream(l.js, []string{l.c.Listener.DBSubject, l.c.Listener.BroadcastSubject})
	if err != nil {
		return err
	}

	// Pull Consumer：多个服务实例共享同一个 Durable，NATS 自动负载均衡
	sub, err := l.js.PullSubscribe(l.c.Listener.DBSubject, durableConsumerName)
	if err != nil {
		return err
	}

	l.wg.Add(1)
	go l.runLoop(sub)

	return nil
}

// runLoop 循环拉取并单条处理消息
func (l *NatsListener) runLoop(sub *nats.Subscription) {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(fetchWaitTimeout))
		if err != nil && err != nats.ErrTimeout {
			logger.Errorf("[NatsListener] fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		msg := msgs[0]
		if err := l.handleMessage(msg); err != nil {
			logger.Error(err.Error())
			msg.Nak()
		} else {
			msg.Ack()
		}
	}
}

// handleMessage 反序列化单条 NATS 消息并委托 Service 持久化
func (l *NatsListener) handleMessage(msg *nats.Msg) error {
	var m svc.MessageSend
	if err := proto.Unmarshal(msg.Data, &m); err != nil {
		return fmt.Errorf("[NatsListener] unmarshal error: %v", err)
	}
	ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
	defer cancel()

	dbMsg, err := l.svc.PersistMessage(ctx, &m)
	if err != nil {
		// 持久化失败：直接发裸 PersistAck bytes，由 WS 层的 QueueSubscribeRaw handler 解码
		pack := &message.PersistAck{
			MsgId:     m.MsgId,
			ClientId:  m.ClientId,
			SessionId: m.ConversationId,
			Target:    m.Sender,
			AckStatus: message.AckStatus_ACK_STATUS_FAILED,
			Seq:       dbMsg.Seq,
		}
		if data, err2 := proto.Marshal(pack); err2 == nil {
			if pubErr := l.conn.Publish(l.c.Listener.AckSubject, data); pubErr != nil {
				logger.Error(pubErr.Error())
			}
		}
		return fmt.Errorf("[NatsListener] PersistMessage error: %v", err)
	} else {
		// 1. 发二级 ACK 给发送方：直接发裸 PersistAck bytes（AckSubject 用 QueueSubscribeRaw 接收）
		pack := &message.PersistAck{
			MsgId:     dbMsg.MsgID,
			ClientId:  dbMsg.ClientID,
			SessionId: dbMsg.ConversationID,
			Target:    m.Sender,
			AckStatus: message.AckStatus_ACK_STATUS_SUCCESS,
			Timestamp: time.Now().UnixMilli(),
		}
		if ackData, err2 := proto.Marshal(pack); err2 == nil {
			if pubErr := l.conn.Publish(l.c.Listener.AckSubject, ackData); pubErr != nil {
				logger.Error(pubErr.Error())
			}
		}

		// 2. 将消息投递给接收方：BroadcastSubject 依然用 WSMessage（所有 WS 节点广播转发）
		deliverMsg := &transport.WSMessage{
			Type:            transport.MessageType(m.MsgType),
			Payload:         m.Payload,
			RouteTarget:     []uint64{m.Target},
			Timestamp:       dbMsg.CreateTime.UnixMilli(),
			RouteTargetType: transport.TargetType(util.GetConversationType(m.ConversationId)),
			SenderId:        dbMsg.FromUserID,
		}
		deliverMsgData, _ := proto.Marshal(deliverMsg)
		if pubErr := l.conn.Publish(l.c.Listener.BroadcastSubject, deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
	}
	return nil
}

// Stop 停止监听并释放资源
func (l *NatsListener) Stop() error {
	l.cancel()
	l.wg.Wait()
	if l.conn != nil {
		l.conn.Close()
	}
	return nil
}
