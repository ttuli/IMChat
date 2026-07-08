package listener

import (
	"context"
	"fmt"
	"hash/fnv"
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
	// 单批读取等待超时
	fetchWaitTimeout = 200 * time.Millisecond
	// 持久化消费者名称（多实例共享，NATS 负载均衡分配消息）
	durableConsumerName = "message_db_consumer"
	// 网关用户路由键前缀（与网关 router 的 routeKeyPrefix 保持一致）
	wsRouteKeyPrefix = "ws:route:"
	// 网关节点心跳键前缀（与网关 router 的 nodeKeyPrefix 保持一致）
	wsNodeKeyPrefix = "ws:node:"

	// 默认单批拉取条数
	defaultFetchBatchSize = 32
	// 默认并行 worker 数
	defaultWorkers = 8
	// 每个 worker 的待处理队列长度（打满时 runLoop 阻塞，形成天然背压）
	workerQueueSize = 128
)

// routeStatus 用户路由查询结果
type routeStatus int

const (
	// routeFallback 未配置精准投递 / 路由状态未知 / 路由指向已死节点 → 广播兜底
	routeFallback routeStatus = iota
	// routeOffline 确认不在线（路由键不存在）→ 只存不推
	routeOffline
	// routeOnline 路由有效且节点存活 → 精准投递
	routeOnline
)

// pendingMsg 已反序列化、待 worker 处理的消息
type pendingMsg struct {
	natsMsg *nats.Msg
	send    *protosvc.MessageSend
}

// NatsListener 监听 NATS 消息，委托 MessageService 完成业务处理（持久化等）。
//
// 消费模型：单 goroutine 批量 Fetch，按 sessionKey 哈希分发到固定 worker——
// 同会话消息在实例内严格串行（保证 seq 单调与处理顺序），不同会话并行。
type NatsListener struct {
	svcCtx    *svc.ServiceContext
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	dlq       DLQHandler
	workerChs []chan *pendingMsg
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
	// Stream 只保留需要持久化重放的落库队列；
	// 广播/节点 subject 走 core NATS，不纳入 Stream（避免无意义落盘）
	err := nats_util.InitStream(l.svcCtx.Js, []string{l.svcCtx.Config.Listener.DBSubject})
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

	workers := l.svcCtx.Config.Listener.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	l.workerChs = make([]chan *pendingMsg, workers)
	for i := range l.workerChs {
		ch := make(chan *pendingMsg, workerQueueSize)
		l.workerChs[i] = ch
		l.wg.Add(1)
		go l.runWorker(ch)
	}

	batch := l.svcCtx.Config.Listener.FetchBatchSize
	if batch <= 0 {
		batch = defaultFetchBatchSize
	}

	l.wg.Add(1)
	go l.runLoop(sub, batch)

	return nil
}

// runLoop 批量拉取消息，按会话哈希分发到固定 worker
func (l *NatsListener) runLoop(sub *nats.Subscription, batch int) {
	defer l.wg.Done()
	// 退出时关闭 worker 队列，worker 处理完剩余消息后自行退出
	defer func() {
		for _, ch := range l.workerChs {
			close(ch)
		}
	}()

	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		msgs, err := sub.Fetch(batch, nats.MaxWait(fetchWaitTimeout))
		if err != nil && err != nats.ErrTimeout {
			logger.Errorf("[NatsListener] fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, msg := range msgs {
			var m protosvc.MessageSend
			if unmarshalErr := proto.Unmarshal(msg.Data, &m); unmarshalErr != nil {
				l.finishMsg(msg, fmt.Errorf("[NatsListener] unmarshal error: %v", unmarshalErr))
				continue
			}
			// worker 队列满时此处阻塞，暂停拉取，形成背压
			l.workerChs[sessionWorkerIndex(&m, len(l.workerChs))] <- &pendingMsg{natsMsg: msg, send: &m}
		}
	}
}

// runWorker 串行处理分配到本 worker 的消息（同会话固定同 worker）
func (l *NatsListener) runWorker(ch chan *pendingMsg) {
	defer l.wg.Done()
	for pm := range ch {
		l.finishMsg(pm.natsMsg, l.process(pm.send))
	}
}

// sessionWorkerIndex 按会话 key 哈希选择 worker，保证同会话消息串行处理
func sessionWorkerIndex(m *protosvc.MessageSend, n int) int {
	key := m.SessionKey
	if key == "" {
		key = m.SessionId
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// finishMsg 根据处理结果完成消息确认：成功 Ack；失败 Nak 重投，达到 MaxDeliver 后转入 DLQ
func (l *NatsListener) finishMsg(msg *nats.Msg, err error) {
	if err == nil {
		msg.Ack()
		return
	}
	logger.Error(err.Error())

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
}

// process 处理单条消息：会话解析 → 持久化 → 二级 ACK → 投递
func (l *NatsListener) process(m *protosvc.MessageSend) error {
	msgSvc := service.NewMessageService(l.svcCtx)

	ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
	defer cancel()

	sessionType := model.SessionTypeSingle
	if util.IsGroupSession(m.SessionKey) {
		sessionType = model.SessionTypeGroup
	}
	sessionID, err := msgSvc.ResolveSessionID(ctx, m.SessionKey, sessionType)
	if err != nil {
		return err
	}
	m.SessionId = sessionID

	dbMsg, err := msgSvc.PersistMessage(ctx, m)
	if err != nil {
		// 持久化失败：二级 ACK（失败）投递给发送方
		pack := &message.PersistAck{
			ClientId:   m.ClientId,
			SessionId:  m.SessionId,
			SessionKey: m.SessionKey,
			Target:     m.Sender,
			AckStatus:  message.AckStatus_ACK_STATUS_FAILED,
			Timestamp:  time.Now().UnixMilli(),
		}
		l.publishPersistAck(ctx, pack)
		return fmt.Errorf("[NatsListener] PersistMessage error: %v", err)
	}

	// 1. 二级 ACK（成功）投递给发送方
	pack := &message.PersistAck{
		MsgId:     dbMsg.MsgID,
		ClientId:  dbMsg.ClientID,
		SessionId: dbMsg.SessionID,
		Target:    m.Sender,
		Seq:       dbMsg.Seq,
		AckStatus: message.AckStatus_ACK_STATUS_SUCCESS,
		Timestamp: time.Now().UnixMilli(),
	}
	l.publishPersistAck(ctx, pack)

	// 2. 投递消息给接收方。
	// 服务端生成的真实 MsgId / SessionId / Seq 直接在 WSMessage 层携带，
	// Payload 原样透传，不再做 ParseMessage → 改 BaseMessage → repack 的重建往返。
	targetType := transport.TargetType_USER
	if util.IsGroupSession(m.SessionId) {
		targetType = transport.TargetType_GROUP
	}
	deliverMsg := &transport.WSMessage{
		Type:            transport.MessageType(m.MsgType),
		Payload:         m.Payload,
		RouteTarget:     []uint64{m.Target},
		RouteTargetType: targetType,
		SenderId:        dbMsg.FromUserID,
		Timestamp:       dbMsg.CreateTime.UnixMilli(),
		MsgId:           dbMsg.MsgID,
		SessionId:       dbMsg.SessionID,
		MsgSeq:          dbMsg.Seq,
	}
	deliverMsgData, err := proto.Marshal(deliverMsg)
	if err != nil {
		logger.Errorf("[NatsListener] marshal deliver message failed: %v", err)
		return nil // 已持久化，不触发重投
	}

	if targetType == transport.TargetType_GROUP {
		// 群聊：全节点广播（各网关按本地在线群成员投递）
		if pubErr := l.svcCtx.NatsConn.Publish(l.svcCtx.Config.Listener.BroadcastSubject, deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
		return nil
	}

	// 单聊：查路由表精准投递
	node, status := l.lookupRoute(ctx, m.Target)
	switch status {
	case routeOnline:
		if pubErr := l.svcCtx.NatsConn.Publish(l.nodeSubject(node), deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
	case routeOffline:
		// 确认不在线：只存不推，上线后由客户端拉取
	default:
		// 路由状态不可信（Redis 异常 / 路由指向已死节点 / 未配置精准投递）：
		// 广播兜底，由持有连接的网关节点完成本地投递，避免静默漏推
		if pubErr := l.svcCtx.NatsConn.Publish(l.svcCtx.Config.Listener.BroadcastSubject, deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
	}
	return nil
}

// publishPersistAck 将二级 ACK 包装为 WSMessage 投递到发送方所在网关节点。
func (l *NatsListener) publishPersistAck(ctx context.Context, pack *message.PersistAck) {
	ackData, err := proto.Marshal(pack)
	if err != nil {
		logger.Errorf("[NatsListener] marshal PersistAck failed: %v", err)
		return
	}
	wsMsg := &transport.WSMessage{
		Type:            transport.MessageType_MSG_PERSIST_ACK,
		RouteTarget:     []uint64{pack.Target},
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       pack.Timestamp,
		Payload:         ackData,
		MsgId:           pack.MsgId,
		SessionId:       pack.SessionId,
		MsgSeq:          pack.Seq,
	}
	data, err := proto.Marshal(wsMsg)
	if err != nil {
		logger.Errorf("[NatsListener] marshal ack WSMessage failed: %v", err)
		return
	}

	node, status := l.lookupRoute(ctx, pack.Target)
	var subject string
	switch status {
	case routeOnline:
		subject = l.nodeSubject(node)
	case routeOffline:
		// 发送方已断线，ACK 无投递意义；消息状态由其重连后拉取对齐
		return
	default:
		subject = l.svcCtx.Config.Listener.BroadcastSubject
	}
	if pubErr := l.svcCtx.NatsConn.Publish(subject, data); pubErr != nil {
		logger.Error(pubErr.Error())
	}
}

// routeLookupScript 单次往返完成「查路由 + 校验节点存活」。
// 路由存在但节点心跳键已消失，说明路由是过期脏数据（节点宕机未清理）。
// 注：脚本内拼接第二个 key，仅适用于单实例/主从 Redis（本项目部署形态），不兼容 Cluster。
const routeLookupScript = `
local node = redis.call('GET', KEYS[1])
if not node then
	return {'', 0}
end
local alive = redis.call('EXISTS', ARGV[1] .. node)
return {node, alive}
`

// lookupRoute 查询用户当前所在网关节点及路由可信度
func (l *NatsListener) lookupRoute(ctx context.Context, userID uint64) (string, routeStatus) {
	if l.svcCtx.Config.Listener.NodeSubjectPrefix == "" {
		return "", routeFallback
	}
	raw, err := l.svcCtx.Redis.EvalCtx(ctx, routeLookupScript,
		[]string{fmt.Sprintf("%s%d", wsRouteKeyPrefix, userID)}, wsNodeKeyPrefix)
	if err != nil {
		logger.Errorf("[NatsListener] lookup route for user %d failed: %v", userID, err)
		return "", routeFallback
	}

	row, ok := raw.([]interface{})
	if !ok || len(row) != 2 {
		return "", routeFallback
	}
	node := fmt.Sprintf("%v", row[0])
	alive, _ := row[1].(int64)

	if node == "" {
		return "", routeOffline
	}
	if alive == 1 {
		return node, routeOnline
	}
	return "", routeFallback
}

// nodeSubject 拼接网关节点专属 subject
func (l *NatsListener) nodeSubject(nodeID string) string {
	return l.svcCtx.Config.Listener.NodeSubjectPrefix + nodeID
}

// Stop 停止监听并释放资源：先停拉取，待 worker 处理完队列内消息后返回
func (l *NatsListener) Stop() error {
	l.cancel()
	l.wg.Wait()
	// 注意：l.svcCtx.NatsConn 的生命周期现在由外层控制，不应在这里 Close
	return nil
}
