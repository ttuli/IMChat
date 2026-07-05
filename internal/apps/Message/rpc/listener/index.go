package listener

import (
	"context"
	"fmt"
	"sync"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/svc"
	"IM2/internal/model"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/message"
	protosvc "IM2/pkg/proto/svc"
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
	svcCtx *svc.ServiceContext
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	dlq    DLQHandler
}

func NewNatsListener(c config.Config, svcCtx *svc.ServiceContext) *NatsListener {
	ctx, cancel := context.WithCancel(context.Background())
	return &NatsListener{
		svcCtx: svcCtx,
		ctx:    ctx,
		cancel: cancel,
		dlq:    NewNatsDLQHandler(svcCtx.NatsConn, svcCtx.Config.Listener.DLQSubject, nil),
	}
}

func (l *NatsListener) Listen() error {
	err := nats_util.InitStream(l.svcCtx.Js, []string{l.svcCtx.Config.Listener.DBSubject, l.svcCtx.Config.Listener.BroadcastSubject})
	if err != nil {
		return err
	}

	maxDeliver := l.svcCtx.Config.Listener.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5 // default
	}

	// 检查并自动更新已存在的 Durable Consumer 配置，防止配置冲突（如 MaxDeliver 校验失败）
	if info, infoErr := l.svcCtx.Js.ConsumerInfo("WS_MESSAGES", durableConsumerName); infoErr == nil {
		if info.Config.MaxDeliver != maxDeliver {
			cfg := info.Config
			cfg.MaxDeliver = maxDeliver
			if _, updateErr := l.svcCtx.Js.UpdateConsumer("WS_MESSAGES", &cfg); updateErr != nil {
				_ = l.svcCtx.Js.DeleteConsumer("WS_MESSAGES", durableConsumerName)
			}
		}
	}

	// Pull Consumer：多个服务实例共享同一个 Durable，NATS 自动负载均衡
	sub, err := l.svcCtx.Js.PullSubscribe(l.svcCtx.Config.Listener.DBSubject, durableConsumerName, nats.MaxDeliver(maxDeliver))
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
		for _, msg := range msgs {
			if err := l.handleMessage(msg); err != nil {
				logger.Error(err.Error())

				// DLQ logic: check if max deliver reached
				meta, metaErr := msg.Metadata()
				maxDeliver := l.svcCtx.Config.Listener.MaxDeliver
				if maxDeliver <= 0 {
					maxDeliver = 5
				}

				if metaErr == nil && int(meta.NumDelivered) >= maxDeliver {
					// Reached max deliver, send to DLQ
					if dlqErr := l.dlq.Handle(l.ctx, msg, err, int(meta.NumDelivered)); dlqErr != nil {
						logger.Errorf("[NatsListener] Failed to handle DLQ: %v", dlqErr)
						msg.Nak() // Still Nak if DLQ fails
					} else {
						msg.Ack() // Ack from main stream if DLQ success
					}
				} else {
					msg.Nak() // Normal retry
				}
			} else {
				msg.Ack()
			}
		}
	}
}

// handleMessage 反序列化单条 NATS 消息并委托 Service 持久化
func (l *NatsListener) handleMessage(msg *nats.Msg) error {
	var m protosvc.MessageSend
	if err := proto.Unmarshal(msg.Data, &m); err != nil {
		return fmt.Errorf("[NatsListener] unmarshal error: %v", err)
	}

	msgSvc := service.NewMessageService(l.svcCtx)

	// 由 Message 服务本地 SnowflakeNode 生成全局唯一的 MsgId
	// 覆盖网关层用 ClientId 占位的临时 MsgId
	ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
	defer cancel()

	sessionType := model.SessionTypeSingle
	if util.IsGroupSession(m.SessionKey) {
		sessionType = model.SessionTypeGroup
	}
	session, _, err := msgSvc.GetOrCreateSession(ctx, "", m.SessionKey, sessionType)
	if err != nil {
		return err
	}
	m.SessionId = session.SessionID

	dbMsg, err := msgSvc.PersistMessage(ctx, &m)
	if err != nil {
		// 持久化失败：直接发裸 PersistAck bytes，由 WS 层的 QueueSubscribeRaw handler 解码
		pack := &message.PersistAck{
			ClientId:   m.ClientId,
			SessionId:  m.SessionId,
			SessionKey: m.SessionKey,
			Target:     m.Sender,
			AckStatus:  message.AckStatus_ACK_STATUS_FAILED,
			Seq:        dbMsg.Seq,
		}
		if data, err2 := proto.Marshal(pack); err2 == nil {
			if pubErr := l.svcCtx.NatsConn.Publish(l.svcCtx.Config.Listener.AckSubject, data); pubErr != nil {
				logger.Error(pubErr.Error())
			}
		}
		return fmt.Errorf("[NatsListener] PersistMessage error: %v", err)
	} else {
		// 1. 发二级 ACK 给发送方：直接发裸 PersistAck bytes（AckSubject 用 QueueSubscribeRaw 接收）
		pack := &message.PersistAck{
			MsgId:     dbMsg.MsgID,
			ClientId:  dbMsg.ClientID,
			SessionId: dbMsg.SessionID,
			Target:    m.Sender,
			Seq:       dbMsg.Seq,
			AckStatus: message.AckStatus_ACK_STATUS_SUCCESS,
			Timestamp: time.Now().UnixMilli(),
		}
		if ackData, err2 := proto.Marshal(pack); err2 == nil {
			if pubErr := l.svcCtx.NatsConn.Publish(l.svcCtx.Config.Listener.AckSubject, ackData); pubErr != nil {
				logger.Error(pubErr.Error())
			}
		}

		// 2. 将消息投递给接收方：BroadcastSubject 依然用 WSMessage（所有 WS 节点广播转发）
		// 构造完整 WSMessage 传入 ParseMessage，repack 时路由字段会一并复制
		tmpWS := &transport.WSMessage{
			Type:            transport.MessageType(m.MsgType),
			Payload:         m.Payload,
			RouteTarget:     []uint64{m.Target},
			Timestamp:       dbMsg.CreateTime.UnixMilli(),
			RouteTargetType: transport.TargetType(util.GetSessionType(m.SessionId)),
			SenderId:        dbMsg.FromUserID,
		}
		base, _, repack, parseErr := transport.ParseMessage(tmpWS)
		if parseErr == nil && base != nil {
			// 用服务端持久化后的真实值覆盖客户端占位字段
			base.MsgId = dbMsg.MsgID
			base.SessionId = dbMsg.SessionID
			base.MsgSeq = int32(dbMsg.Seq)
		}
		var deliverMsg *transport.WSMessage
		if parseErr == nil {
			deliverMsg, _ = repack()
		}
		if deliverMsg == nil {
			// 解析失败 fallback：路由字段正确，仅 payload 内部字段未更新
			logger.Errorf("[NatsListener] ParseMessage failed, fallback to raw payload: %v", parseErr)
			deliverMsg = tmpWS
		}
		deliverMsgData, _ := proto.Marshal(deliverMsg)
		if pubErr := l.svcCtx.NatsConn.Publish(l.svcCtx.Config.Listener.BroadcastSubject, deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
	}
	return nil
}

// Stop 停止监听并释放资源
func (l *NatsListener) Stop() error {
	l.cancel()
	l.wg.Wait()
	// 注意：l.svcCtx.NatsConn 的生命周期现在由外层控制，不应在这里 Close
	return nil
}
