package dispatch

import (
	"context"
	"errors"
	"sync"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/callstate"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/call"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/routing"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// minDeliverRemainMs 补投的最小剩余振铃时间。
// 剩余不足此值时不再唤起被叫界面：响铃半秒就被 cancel 比不响更差。
const minDeliverRemainMs = 5000

// peerCache 通话对端缓存：callID → 对端用户 ID。
//
// SDP/ICE 转发需要知道把帧发给谁，而 ICE trickle 一通电话能产生数十个 candidate，
// 每帧都回查 Redis 是纯浪费。采用 late offer 握手时 SDP/ICE 全部发生在 accept 之后，
// 届时主被叫两侧的 Dispatcher 都已在 invite/accept 路径上填好缓存，命中率接近 100%。
// 未命中时回落到 Redis 查询，不影响正确性。
//
// Dispatcher 随连接创建、ReadPump 单协程串行调用，理论上无并发；
// 仍加锁以防将来出现旁路调用。
type peerCache struct {
	mu sync.RWMutex
	m  map[string]uint64
}

func (p *peerCache) get(callID string) (uint64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	uid, ok := p.m[callID]
	return uid, ok
}

func (p *peerCache) put(callID string, peerID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.m == nil {
		p.m = make(map[string]uint64, 2)
	}
	p.m[callID] = peerID
}

func (p *peerCache) drop(callID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.m, callID)
}

// processCallSignal 通话信令总入口。
//
// **与聊天消息完全隔离**：本路径不调用 transport.ParseMessage、不 publish 到 DBSubject、
// 不分配 msg_id/seq。信令是易失的，通话记录另由终态时的 CHAT_CALL 承载。
func (h *Dispatcher) processCallSignal(ctx context.Context, msg *transport.WSMessage) error {
	switch msg.Type {
	case transport.MessageType_CALL_INVITE:
		return h.onCallInvite(ctx, msg)
	case transport.MessageType_CALL_ACCEPT:
		return h.onCallAccept(ctx, msg)
	case transport.MessageType_CALL_REJECT:
		return h.onCallTerminate(ctx, msg, call.CallEndReason_CALL_END_REASON_REJECTED)
	case transport.MessageType_CALL_CANCEL:
		return h.onCallTerminate(ctx, msg, call.CallEndReason_CALL_END_REASON_CANCELED)
	case transport.MessageType_CALL_HANGUP:
		return h.onCallTerminate(ctx, msg, call.CallEndReason_CALL_END_REASON_COMPLETED)
	case transport.MessageType_CALL_SDP,
		transport.MessageType_CALL_ICE,
		transport.MessageType_CALL_MEDIA_UPDATE:
		return h.relayCallSignal(ctx, msg)
	case transport.MessageType_CALL_PENDING:
		return h.onCallPending(ctx, msg)
	default:
		logger.Infof("[CallSignal] unhandled call signal type: %v", msg.Type)
		return nil
	}
}

// onCallInvite 发起通话。
//
// **顺序契约（漏投防护）**：必须先 callstate.Create 成功，再推送给被叫。
// 反序（先查在线、离线才写状态）与被叫的「注册路由 → 查 pending」交叉时存在漏投窗口：
//
//	A查路由(未注册) → B注册 → B查pending(空) → A写状态   ← B 永远收不到，A 空振铃到超时
//
// 正序下两条路径至少一条命中，都命中时由客户端按 call_id 幂等去重。
func (h *Dispatcher) onCallInvite(ctx context.Context, msg *transport.WSMessage) error {
	var in call.CallInvite
	if err := protocol.DecodePayload(msg, &in); err != nil {
		return err
	}

	// 不采信客户端自报的 caller_id：一律以连接身份为准，防止伪造主叫
	callerID := h.conn.UserID
	if in.CalleeId == 0 || in.CalleeId == callerID || in.SessionKey == "" {
		h.sendCallError(in.CallId, call.CallEndReason_CALL_END_REASON_FAILED)
		return errors.New("invalid call invite")
	}

	now := time.Now().UnixMilli()
	c := &callstate.Call{
		CallID:     uuid.NewString(), // call_id 由服务端分配，客户端上报的忽略
		CallerID:   callerID,
		CalleeID:   in.CalleeId,
		SessionKey: in.SessionKey,
		MediaType:  in.MediaType,
		InviteAt:   now,
		ExpireAt:   now + callstate.RingTTL.Milliseconds(),
	}

	if err := h.svcCtx.CallState.Create(ctx, c); err != nil {
		if errors.Is(err, callstate.ErrPeerBusy) {
			h.sendCallError(c.CallID, call.CallEndReason_CALL_END_REASON_BUSY)
			return nil
		}
		logger.Errorf("[CallSignal] create call state failed: %v", err)
		h.sendCallError(c.CallID, call.CallEndReason_CALL_END_REASON_FAILED)
		return err
	}
	h.calls.put(c.CallID, c.CalleeID)

	invite := &call.CallInvite{
		CallId:       c.CallID,
		CallerId:     c.CallerID,
		CalleeId:     c.CalleeID,
		SessionKey:   c.SessionKey,
		MediaType:    c.MediaType,
		InviteAt:     c.InviteAt,
		RingDeadline: c.ExpireAt,
	}

	// 回执主叫：带回服务端分配的 call_id 与振铃截止，兼作本次 invite 的 ACK
	h.sendToSelf(transport.MessageType_CALL_INVITE, invite)

	// 推送被叫。被叫离线时**不失败**：通话保持振铃，等其在窗口内上线后由
	// CALL_PENDING 补投；窗口内未上线则由 sweeper 收敛为 PEER_OFFLINE。
	status := h.deliverToUser(ctx, c.CalleeID, transport.MessageType_CALL_INVITE, invite)
	if status == routing.RouteOffline {
		logger.Infof("[CallSignal] callee %d offline, call %s keeps ringing until %d",
			c.CalleeID, c.CallID, c.ExpireAt)
	}
	return nil
}

// onCallAccept 被叫接听：CAS 到 CONNECTED 后转发给主叫。
// 主叫收到本帧才开始产生 SDP offer（late offer 握手）。
func (h *Dispatcher) onCallAccept(ctx context.Context, msg *transport.WSMessage) error {
	var in call.CallAccept
	if err := protocol.DecodePayload(msg, &in); err != nil {
		return err
	}

	c, err := h.svcCtx.CallState.Accept(ctx, in.CallId, h.conn.UserID)
	if err != nil {
		// 已被 cancel / 超时抢先：告知被叫收起接听界面，不视为错误
		if errors.Is(err, callstate.ErrStateConflict) || errors.Is(err, callstate.ErrCallNotFound) {
			h.sendCallError(in.CallId, call.CallEndReason_CALL_END_REASON_MISSED)
			return nil
		}
		return err
	}
	h.calls.put(c.CallID, c.CallerID)

	return h.deliverAndLog(ctx, c.CallerID, transport.MessageType_CALL_ACCEPT, &call.CallAccept{
		CallId:   c.CallID,
		CalleeId: c.CalleeID,
		AcceptAt: c.ConnectedAt,
	})
}

// onCallTerminate 用户主动终止（拒接 / 取消 / 挂断）。
//
// 落库由 **CAS 赢家** 负责：Terminate 返回 nil 表示对端或 sweeper 已抢先收敛，
// 本次静默返回，避免一通电话写出多条记录。
func (h *Dispatcher) onCallTerminate(ctx context.Context, msg *transport.WSMessage, reason call.CallEndReason) error {
	callID, err := extractCallID(msg)
	if err != nil {
		return err
	}

	// 校验发起者确为通话参与方，防止第三方终止他人通话
	cur, err := h.svcCtx.CallState.Get(ctx, callID)
	if err != nil {
		if errors.Is(err, callstate.ErrCallNotFound) {
			return nil // 已清理，静默
		}
		return err
	}
	if h.conn.UserID != cur.CallerID && h.conn.UserID != cur.CalleeID {
		return errors.New("not a participant of the call")
	}

	// 振铃期间挂断按语义归一：主叫→取消，被叫→拒接
	if cur.State == callstate.StateRinging && reason == call.CallEndReason_CALL_END_REASON_COMPLETED {
		if h.conn.UserID == cur.CallerID {
			reason = call.CallEndReason_CALL_END_REASON_CANCELED
		} else {
			reason = call.CallEndReason_CALL_END_REASON_REJECTED
		}
	}

	ended, err := h.svcCtx.CallState.Terminate(ctx, callID, reason)
	if err != nil {
		return err
	}
	h.calls.drop(callID)
	if ended == nil {
		return nil // 非赢家：对端或 sweeper 已收敛并落库
	}

	peerID := ended.CallerID
	if h.conn.UserID == ended.CallerID {
		peerID = ended.CalleeID
	}
	_ = h.deliverAndLog(ctx, peerID, transport.MessageType_CALL_END, &call.CallEnd{
		CallId:   ended.CallID,
		Reason:   reason,
		EndAt:    ended.EndedAt,
		Duration: ended.Duration(),
	})

	h.publishCallRecord(ended, reason)
	return nil
}

// relayCallSignal 转发 SDP / ICE / 媒体开关状态。
//
// **纯透传**：不解析 sdp 内容、不做「握手已完成」的状态判断。
// 前端设计上首次握手后不再重协商，但网关不依赖该假设去拦帧 ——
// 拦错的表现是「开了摄像头对面看不到」，极难排查，而透传成本为零。
func (h *Dispatcher) relayCallSignal(ctx context.Context, msg *transport.WSMessage) error {
	callID, err := extractCallID(msg)
	if err != nil {
		return err
	}

	peerID, ok := h.calls.get(callID)
	if !ok {
		c, err := h.svcCtx.CallState.Get(ctx, callID)
		if err != nil {
			return nil // 通话已结束：迟到的信令直接丢弃
		}
		if h.conn.UserID != c.CallerID && h.conn.UserID != c.CalleeID {
			return errors.New("not a participant of the call")
		}
		peerID = c.CallerID
		if h.conn.UserID == c.CallerID {
			peerID = c.CalleeID
		}
		h.calls.put(callID, peerID)
	}

	// 原样透传 payload，只重写路由信息与发送者
	h.publishRaw(ctx, peerID, &transport.WSMessage{
		Type:      msg.Type,
		Payload:   msg.Payload,
		Timestamp: time.Now().UnixMilli(),
		SenderId:  h.conn.UserID,
	})
	return nil
}

// onCallPending 上线补投：客户端建连/重连后查询是否有仍在振铃的来电。
//
// 服务端在此完成两项复核，客户端拿到 has_pending=true 即可直接弹接听界面：
//  1. **主叫仍在线** —— 否则「接起来对面没人」（主叫崩溃后状态还在 TTL 内）
//  2. **剩余振铃时间充足** —— 否则响铃半秒即被 cancel
func (h *Dispatcher) onCallPending(ctx context.Context, _ *transport.WSMessage) error {
	c, err := h.svcCtx.CallState.PendingFor(ctx, h.conn.UserID)
	if err != nil {
		return err
	}
	if c == nil {
		h.sendToSelf(transport.MessageType_CALL_PENDING, &call.CallPending{HasPending: false})
		return nil
	}

	// 复核 1：主叫存活。状态平面不反向依赖路由表，故在此编排层做。
	_, status, err := h.svcCtx.Routes.LookupUser(ctx, c.CallerID)
	if err == nil && status == routing.RouteOffline {
		if ended, tErr := h.svcCtx.CallState.Terminate(
			ctx, c.CallID, call.CallEndReason_CALL_END_REASON_FAILED); tErr == nil && ended != nil {
			h.publishCallRecord(ended, call.CallEndReason_CALL_END_REASON_FAILED)
		}
		h.sendToSelf(transport.MessageType_CALL_PENDING, &call.CallPending{HasPending: false})
		return nil
	}

	// 复核 2：剩余振铃时间。不足则不打扰被叫，留给 sweeper 收敛为 MISSED。
	remain := c.RemainingMs(time.Now().UnixMilli())
	if remain < minDeliverRemainMs {
		h.sendToSelf(transport.MessageType_CALL_PENDING, &call.CallPending{HasPending: false})
		return nil
	}

	h.calls.put(c.CallID, c.CallerID)
	h.sendToSelf(transport.MessageType_CALL_PENDING, &call.CallPending{
		HasPending: true,
		Invite: &call.CallInvite{
			CallId:       c.CallID,
			CallerId:     c.CallerID,
			CalleeId:     c.CalleeID,
			SessionKey:   c.SessionKey,
			MediaType:    c.MediaType,
			InviteAt:     c.InviteAt,
			RingDeadline: c.ExpireAt,
		},
		RemainingMs: remain,
	})
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 投递与落库辅助
// ─────────────────────────────────────────────────────────────────────────────

// deliverToUser 按路由表投递给指定用户，返回其路由状态。
// RouteUnknown（Redis 异常 / 路由指向已死节点）时广播兜底，与 UserNotifier 同语义。
func (h *Dispatcher) deliverToUser(
	ctx context.Context, userID uint64, msgType transport.MessageType, payload proto.Message,
) routing.RouteStatus {
	wsMsg, err := protocol.NewWSMessage(msgType, payload)
	if err != nil {
		logger.Errorf("[CallSignal] marshal %v failed: %v", msgType, err)
		return routing.RouteUnknown
	}
	wsMsg.SenderId = h.conn.UserID
	return h.publishRaw(ctx, userID, wsMsg)
}

// publishRaw 完成路由查询与投递，msg 的路由字段由本函数填充
func (h *Dispatcher) publishRaw(ctx context.Context, userID uint64, msg *transport.WSMessage) routing.RouteStatus {
	msg.RouteTarget = []uint64{userID}
	msg.RouteTargetType = transport.TargetType_USER

	node, status, err := h.svcCtx.Routes.LookupUser(ctx, userID)
	if err != nil {
		logger.Errorf("[CallSignal] lookup user %d failed (broadcast fallback): %v", userID, err)
		h.svcCtx.Nats.Broadcast(msg)
		return routing.RouteUnknown
	}

	switch status {
	case routing.RouteOnline:
		h.svcCtx.Nats.PublishToNode(node, msg)
	case routing.RouteUnknown:
		h.svcCtx.Nats.Broadcast(msg)
	case routing.RouteOffline:
		// 信令易失：对端不在线即丢弃，不排队不补投
	}
	return status
}

func (h *Dispatcher) deliverAndLog(
	ctx context.Context, userID uint64, msgType transport.MessageType, payload proto.Message,
) error {
	if status := h.deliverToUser(ctx, userID, msgType, payload); status == routing.RouteOffline {
		logger.Infof("[CallSignal] peer %d offline, drop %v", userID, msgType)
	}
	return nil
}

// sendToSelf 回发给当前连接
func (h *Dispatcher) sendToSelf(msgType transport.MessageType, payload proto.Message) {
	wsMsg, err := protocol.NewWSMessage(msgType, payload)
	if err != nil {
		logger.Errorf("[CallSignal] marshal %v failed: %v", msgType, err)
		return
	}
	h.conn.Send(wsMsg)
}

// sendCallError 用 CALL_END 告知发起方本次通话未能建立
func (h *Dispatcher) sendCallError(callID string, reason call.CallEndReason) {
	h.sendToSelf(transport.MessageType_CALL_END, &call.CallEnd{
		CallId: callID,
		Reason: reason,
		EndAt:  time.Now().UnixMilli(),
	})
}

// publishCallRecord 落库通话记录。仅应由 Terminate 的 CAS 赢家调用。
//
// 与聊天消息同链路发布到 DBSubject，由 Message 服务分配 msg_id/seq 后持久化并扇出，
// 离线端按会话 seq 增量拉取即可看到这条记录。
func (h *Dispatcher) publishCallRecord(c *callstate.Call, reason call.CallEndReason) {
	record, err := util.NewCallRecordMsg(
		c.CallID, c.CallerID, c.CalleeID, c.SessionKey, c.MediaType, reason, c.Duration())
	if err != nil {
		logger.Errorf("[CallSignal] build call record failed: %v", err)
		return
	}

	data, err := proto.Marshal(record)
	if err != nil {
		logger.Errorf("[CallSignal] marshal call record failed: %v", err)
		return
	}

	// 以 call_id 作 JetStream 去重键：即使赢家判定之外还有重试，也不会重复落库
	if _, err := h.svcCtx.Nats.JetStream().Publish(
		nats_util.DBSubject, data, nats.MsgId("call:"+c.CallID),
	); err != nil {
		logger.Errorf("[CallSignal] publish call record failed: %v", err)
	}
}

// extractCallID 从信令载荷中取出 call_id。
// 各信令类型的 call_id 均为字段 1 的 string，但仍按类型精确解码，
// 避免依赖字段号布局（apis 侧曾因字段号漂移静默发错值）。
func extractCallID(msg *transport.WSMessage) (string, error) {
	switch msg.Type {
	case transport.MessageType_CALL_ACCEPT:
		var m call.CallAccept
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_REJECT:
		var m call.CallReject
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_CANCEL:
		var m call.CallCancel
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_HANGUP:
		var m call.CallHangup
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_SDP:
		var m call.CallSdp
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_ICE:
		var m call.CallIce
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	case transport.MessageType_CALL_MEDIA_UPDATE:
		var m call.CallMediaUpdate
		err := protocol.DecodePayload(msg, &m)
		return m.CallId, err
	default:
		return "", errors.New("no call_id in message type")
	}
}
