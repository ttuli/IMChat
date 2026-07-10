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
	"IM2/pkg/routing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	// 单批读取等待超时
	fetchWaitTimeout = 200 * time.Millisecond
	// 持久化消费者名称（多实例共享，NATS 负载均衡分配消息）
	durableConsumerName = "message_db_consumer"

	// 默认单批拉取条数
	defaultFetchBatchSize = 32
	// 默认并行 worker 数
	defaultWorkers = 8
	// 每个 worker 的待处理队列长度（打满时 runLoop 阻塞，形成天然背压）
	workerQueueSize = 128
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
	// groupEventSub 群操作通知订阅（维护成员缓存与 user_session）
	groupEventSub *nats.Subscription
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

	// 订阅群操作通知：失效成员缓存 + 维护 user_session
	if err := l.subscribeGroupEvents(); err != nil {
		return err
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

	// 会话形态只能由 SessionKey 判断：解析出的 SessionId 是雪花 ID，不携带类型前缀
	isGroup := util.IsGroupSession(m.SessionKey)
	sessionType := model.SessionTypeSingle
	if isGroup {
		sessionType = model.SessionTypeGroup
	}
	sessionID, err := msgSvc.ResolveSessionID(ctx, m.SessionKey, sessionType)
	if err != nil {
		return err
	}
	m.SessionId = sessionID

	// 单聊：补偿双方 user_session 行（进程内防重 + DB 幂等），
	// 保证离线拉取的会话列表/未读计数有据可查；失败不阻塞消息链路
	if !isGroup {
		if ensureErr := l.svcCtx.SessionDAO.EnsureUserSessions(ctx, sessionID, []uint64{m.Sender, m.Target}); ensureErr != nil {
			logger.Errorf("[NatsListener] ensure user_session for session %s failed: %v", sessionID, ensureErr)
		}
	}

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
	if isGroup {
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

	if isGroup {
		// 群聊：成员定向扇出（按网关节点聚合），替代旧的全节点广播
		l.deliverGroupMessage(ctx, m, deliverMsg)
		return nil
	}

	deliverMsgData, err := proto.Marshal(deliverMsg)
	if err != nil {
		logger.Errorf("[NatsListener] marshal deliver message failed: %v", err)
		return nil // 已持久化，不触发重投
	}

	// 单聊：查路由表精准投递
	node, status := l.lookupRoute(ctx, m.Target)
	switch status {
	case routing.RouteOnline:
		if pubErr := l.svcCtx.NatsConn.Publish(l.nodeSubject(node), deliverMsgData); pubErr != nil {
			logger.Error(pubErr.Error())
		}
	case routing.RouteOffline:
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

// deliverGroupMessage 群消息定向扇出。
//
// 流程：权威成员列表（Redis 缓存 + Group RPC 回源）→ 排除发送者 → 批量路由查询 →
// 按网关节点聚合，每个节点只发一条消息，deliver_to 携带该节点需投递的用户列表。
//   - 确认离线的成员：只存不推，上线后由客户端按会话 seq 增量拉取
//   - 路由不可信（Redis 异常/节点已死/未配置精准投递）的成员：广播兜底（仍带 deliver_to，
//     各网关按本地连接过滤投递，不依赖网关侧群成员映射）
//   - 成员列表获取失败：退化为旧的全节点广播（不带 deliver_to，网关走本地群成员映射），
//     保证 Group RPC 故障时不静默丢投
func (l *NatsListener) deliverGroupMessage(ctx context.Context, m *protosvc.MessageSend, deliverMsg *transport.WSMessage) {
	groupID := m.Target
	memberIDs, err := l.svcCtx.Members.GetMemberIDs(ctx, groupID)
	if err != nil || len(memberIDs) == 0 {
		logger.Errorf("[NatsListener] get members of group %d failed (broadcast fallback): %v", groupID, err)
		l.publishDeliverMsg(l.svcCtx.Config.Listener.BroadcastSubject, deliverMsg)
		return
	}

	// 群成员 user_session 存量补偿（进程内防重 + DB 幂等，失败允许下条消息重试）
	if ensureErr := l.svcCtx.SessionDAO.EnsureUserSessions(ctx, m.SessionId, memberIDs); ensureErr != nil {
		logger.Errorf("[NatsListener] ensure user_session for group session %s failed: %v", m.SessionId, ensureErr)
	}

	receivers := make([]uint64, 0, len(memberIDs))
	for _, uid := range memberIDs {
		if uid != m.Sender {
			receivers = append(receivers, uid)
		}
	}
	if len(receivers) == 0 {
		return
	}

	// 未配置精准投递：所有接收者广播兜底（带 deliver_to，网关按本地连接过滤）
	if l.svcCtx.Config.Listener.NodeSubjectPrefix == "" {
		l.publishDeliverMsg(l.svcCtx.Config.Listener.BroadcastSubject, withDeliverTo(deliverMsg, receivers))
		return
	}

	// 批量路由查询（含节点存活校验）；整体失败时全部接收者广播兜底，不漏投
	nodeTargets, fallback, err := l.svcCtx.Routes.LookupUsers(ctx, receivers)
	if err != nil {
		logger.Errorf("[NatsListener] batch route lookup failed: %v", err)
		nodeTargets, fallback = nil, receivers
	}
	for node, uids := range nodeTargets {
		l.publishDeliverMsg(l.nodeSubject(node), withDeliverTo(deliverMsg, uids))
	}
	if len(fallback) > 0 {
		l.publishDeliverMsg(l.svcCtx.Config.Listener.BroadcastSubject, withDeliverTo(deliverMsg, fallback))
	}
}

// withDeliverTo 复制一份仅 deliver_to 不同的投递消息（每个节点的目标列表不同）
func withDeliverTo(msg *transport.WSMessage, targets []uint64) *transport.WSMessage {
	return &transport.WSMessage{
		Type:            msg.Type,
		Payload:         msg.Payload,
		RouteTarget:     msg.RouteTarget,
		RouteTargetType: msg.RouteTargetType,
		SenderId:        msg.SenderId,
		Timestamp:       msg.Timestamp,
		MsgId:           msg.MsgId,
		SessionId:       msg.SessionId,
		MsgSeq:          msg.MsgSeq,
		DeliverTo:       targets,
	}
}

// publishDeliverMsg 序列化并发布投递消息，失败仅记录（消息已持久化，靠拉取兜底）
func (l *NatsListener) publishDeliverMsg(subject string, msg *transport.WSMessage) {
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("[NatsListener] marshal deliver message failed: %v", err)
		return
	}
	if err := l.svcCtx.NatsConn.Publish(subject, data); err != nil {
		logger.Error(err.Error())
	}
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
	case routing.RouteOnline:
		subject = l.nodeSubject(node)
	case routing.RouteOffline:
		// 发送方已断线，ACK 无投递意义；消息状态由其重连后拉取对齐
		return
	default:
		subject = l.svcCtx.Config.Listener.BroadcastSubject
	}
	if pubErr := l.svcCtx.NatsConn.Publish(subject, data); pubErr != nil {
		logger.Error(pubErr.Error())
	}
}

// lookupRoute 查询用户当前所在网关节点及路由可信度。
// 未配置精准投递时视为路由不可信（走广播兜底）。
func (l *NatsListener) lookupRoute(ctx context.Context, userID uint64) (string, routing.RouteStatus) {
	if l.svcCtx.Config.Listener.NodeSubjectPrefix == "" {
		return "", routing.RouteUnknown
	}
	node, status, err := l.svcCtx.Routes.LookupUser(ctx, userID)
	if err != nil {
		logger.Errorf("[NatsListener] lookup route for user %d failed: %v", userID, err)
	}
	return node, status
}

// nodeSubject 拼接网关节点专属 subject
func (l *NatsListener) nodeSubject(nodeID string) string {
	return l.svcCtx.Config.Listener.NodeSubjectPrefix + nodeID
}

// Stop 停止监听并释放资源：先停拉取，待 worker 处理完队列内消息后返回
func (l *NatsListener) Stop() error {
	l.unsubscribeGroupEvents()
	l.cancel()
	l.wg.Wait()
	// 注意：l.svcCtx.NatsConn 的生命周期现在由外层控制，不应在这里 Close
	return nil
}
