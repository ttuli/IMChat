package listener

import (
	"context"
	"sync"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/model"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	// 每次最多拉取的消息数量
	fetchBatchSize = 500
	// 拉取等待超时（积压不足 batchSize 时最多等待这么长）
	fetchWaitTimeout = 100 * time.Millisecond
	// 持久化消费者名称（多实例共享，NATS 负载均衡分配消息）
	durableConsumerName = "message_db_consumer"
)

// NatsListener 监听 NATS 消息，将消息写入 MongoDB
type NatsListener struct {
	conn            *nats.Conn
	js              nats.JetStreamContext
	c               config.Config
	messageDAO      *dao.MessageDAO
	conversationDAO *dao.ConversationDAO
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

func NewNatsListener(c config.Config, conn *nats.Conn, js nats.JetStreamContext, msgDao *dao.MessageDAO, convDao *dao.ConversationDAO) *NatsListener {
	ctx, cancel := context.WithCancel(context.Background())

	return &NatsListener{
		conn:            conn,
		js:              js,
		c:               c,
		messageDAO:      msgDao,
		conversationDAO: convDao,
		ctx:             ctx,
		cancel:          cancel,
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
	go l.runBatchLoop(sub)

	return nil
}

// runBatchLoop 持续拉取并批量入库
func (l *NatsListener) runBatchLoop(sub *nats.Subscription) {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		msgs, err := sub.Fetch(fetchBatchSize, nats.MaxWait(fetchWaitTimeout))
		if err != nil && err != nats.ErrTimeout {
			logger.Errorf("[NatsListener] fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		if err := l.handleMessagesBulk(msgs); err != nil {
			for _, msg := range msgs {
				msg.Nak()
			}
		} else {
			for _, msg := range msgs {
				msg.Ack()
			}
		}
	}
}

// handleMessagesBulk 反序列化一批消息并批量写入 MongoDB
func (l *NatsListener) handleMessagesBulk(msgs []*nats.Msg) error {
	dbMsgs := make([]*model.Message, 0, len(msgs))

	for _, msg := range msgs {
		var m message.Message
		if err := proto.Unmarshal(msg.Data, &m); err != nil {
			logger.Errorf("[NatsListener] unmarshal error: %v", err)
			continue
		}
		dbMsgs = append(dbMsgs, &model.Message{
			MsgID:          m.MsgId,
			ClientID:       m.ClientId,
			ConversationID: m.ConversationId,
			FromUserID:     m.FromUserId,
			MsgType:        int16(m.MsgType),
			Seq:            m.Seq,
			Content:        m.Content,
			MediaURL:       m.MediaUrl,
			Status:         int8(m.Status),
			CreateTime:     time.UnixMilli(m.CreateTime),
		})
	}

	if len(dbMsgs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
	defer cancel()

	if err := l.messageDAO.InsertMessages(ctx, dbMsgs); err != nil {
		logger.Errorf("[NatsListener] bulk insert failed (batch=%d): %v", len(dbMsgs), err)
		return err
	}

	logger.Infof("[NatsListener] bulk inserted %d messages", len(dbMsgs))
	return nil
}

// Stop 停止监听并释放资源
func (l *NatsListener) Stop() error {
	l.cancel()
	l.wg.Wait() // 等待当前正在执行的批量入库及 Ack 操作完成
	if l.conn != nil {
		l.conn.Close()
	}
	return nil
}
