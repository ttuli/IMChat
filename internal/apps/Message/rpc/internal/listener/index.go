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
	svc    service.MessageService
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewNatsListener(c config.Config, conn *nats.Conn, js nats.JetStreamContext, svc service.MessageService) *NatsListener {
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

	dbMsg, err := l.svc.SendMessage(ctx, &m)
	if err != nil {
		pack := &message.PersistAck{
			MsgId:     m.MsgId,
			ClientId:  m.ClientId,
			SessionId: m.ConversationId,
			SendTime:  m.CreateTime,
			AckStatus: message.AckStatus_ACK_STATUS_FAILED,
		}
		data, _ := proto.Marshal(pack)
		rt := transport.TargetType_USER
		if util.IsGroupSession(m.ConversationId) {
			rt = transport.TargetType_GROUP
		}
		wsmsg := &transport.WSMessage{
			Type:            transport.MessageType_MSG_PERSIST_ACK,
			Payload:         data,
			RouteTarget:     []uint64{m.Target},
			Timestamp:       m.CreateTime,
			RouteTargetType: rt,
			SenderId:        m.Sender,
		}
		data, _ = proto.Marshal(wsmsg)
		err = l.conn.Publish(l.c.Listener.AckSubject, data)
		if err != nil {
			logger.Error(err.Error())
		}
		return fmt.Errorf("[NatsListener] SendMessage error: %v", err)
	} else {
		pack := &message.PersistAck{
			MsgId:     dbMsg.MsgID,
			ClientId:  dbMsg.ClientID,
			SessionId: dbMsg.ConversationID,
			Seq:       int64(dbMsg.Seq),
			Content:   dbMsg.Content,
			MediaUrl:  dbMsg.MediaURL,
			MsgType:   int32(dbMsg.MsgType),
			SendTime:  dbMsg.CreateTime.UnixMilli(),
			Status:    int32(dbMsg.Status),
			Sender:    dbMsg.FromUserID,
			Target:    m.Target,
			AckStatus: message.AckStatus_ACK_STATUS_SUCCESS,
		}
		data, _ := proto.Marshal(pack)
		err = l.conn.Publish(l.c.Listener.AckSubject, data)
		if err != nil {
			logger.Error(err.Error())
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
