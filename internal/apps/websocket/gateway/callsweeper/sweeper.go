// Package callsweeper 负责通话状态的超时收敛。
//
// 存在的理由：**Redis key 过期不会执行业务代码**。振铃无人接听时必须有人把状态置为
// 终态并落一条「未接来电」记录，而这件事不能交给客户端 —— 主叫进程崩溃 / 断网时
// 它根本没有机会写。keyspace notification 是 at-most-once，丢了没有补偿，同样不可靠。
//
// 因此用 ZSET 到期索引 + 定时轮询：callstate.ClaimExpired 以可见性超时语义认领
// （重打分而非删除），认领者崩溃时条目会重新可见，不会出现「通话永远不收敛」。
//
// 多网关实例同时运行本 sweeper 是安全的：认领有可见性窗口，终态转换是 CAS，
// 只有赢家落库，重复认领至多产生一次无副作用的空转。
package callsweeper

import (
	"context"
	"errors"
	"time"

	"IM2/pkg/callstate"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/call"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/routing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultInterval 轮询周期。振铃超时精度要求不高（用户对 1-2 秒的偏差无感），
	// 周期太短反而增加空转扫描。
	defaultInterval = 2 * time.Second
	// defaultBatch 单轮认领上限，避免一次拉取过多条目阻塞循环
	defaultBatch = 200
)

// Sweeper 通话超时收敛器
type Sweeper struct {
	calls    *callstate.Store
	routes   *routing.Table
	notifier *nats_util.UserNotifier
	js       nats.JetStreamContext

	interval time.Duration
	batch    int
}

// New 创建收敛器
func New(
	calls *callstate.Store,
	routes *routing.Table,
	notifier *nats_util.UserNotifier,
	js nats.JetStreamContext,
) *Sweeper {
	return &Sweeper{
		calls:    calls,
		routes:   routes,
		notifier: notifier,
		js:       js,
		interval: defaultInterval,
		batch:    defaultBatch,
	}
}

// Start 启动收敛协程（非阻塞）
func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepOnce(ctx)
			}
		}
	}()
	logger.Infof("[CallSweeper] started, interval=%v batch=%d", s.interval, s.batch)
}

// sweepOnce 认领并收敛一批到期通话
func (s *Sweeper) sweepOnce(ctx context.Context) {
	callIDs, err := s.calls.ClaimExpired(ctx, s.batch)
	if err != nil {
		logger.Errorf("[CallSweeper] claim expired failed: %v", err)
		return
	}
	for _, callID := range callIDs {
		s.reap(ctx, callID)
	}
}

// reap 收敛单通电话
func (s *Sweeper) reap(ctx context.Context, callID string) {
	c, err := s.calls.Get(ctx, callID)
	if err != nil {
		if errors.Is(err, callstate.ErrCallNotFound) {
			// 状态已过期清理，索引是残留项：直接摘除
			s.calls.RemoveDeadline(ctx, callID)
			return
		}
		logger.Errorf("[CallSweeper] get call %s failed: %v", callID, err)
		return
	}

	// 已是终态：索引残留（Terminate 的 ZREM 失败过），清掉即可
	if c.State == callstate.StateEnded {
		s.calls.RemoveDeadline(ctx, callID)
		return
	}

	// **ZSET 分数只是调度提示，HASH 里的 expire_at 才是权威。**
	// Accept 续期时 ZADD 失败会让本条目提前到期，这里回读复核后重新排期，
	// 避免误杀一通正在进行的通话。
	now := time.Now().UnixMilli()
	if now < c.ExpireAt {
		if err := s.calls.RescheduleDeadline(ctx, callID, c.ExpireAt); err != nil {
			logger.Errorf("[CallSweeper] reschedule call %s failed: %v", callID, err)
		}
		return
	}

	reason := s.reasonFor(ctx, c)
	ended, err := s.calls.Terminate(ctx, callID, reason)
	if err != nil {
		logger.Errorf("[CallSweeper] terminate call %s failed: %v", callID, err)
		return
	}
	if ended == nil {
		// 非赢家：参与方已抢先挂断并落库，本次无事可做
		return
	}

	logger.Infof("[CallSweeper] reaped call %s state=%d reason=%v duration=%d",
		callID, c.State, reason, ended.Duration())

	s.notifyEnd(ctx, ended, reason)
	s.publishRecord(ended, reason)
}

// reasonFor 按当前状态判定终止原因。
//
// RINGING 超时细分为两种，用被叫**此刻**的在线状态近似：
//   - 被叫在线：人在但没接 → MISSED
//   - 被叫离线：整个振铃窗口都没上线 → PEER_OFFLINE
//
// CONNECTED 超时说明撞到了通话时长上限（多为双方进程同时消失、挂断信令都没发出），
// 按已接通处理，保留累计时长。
func (s *Sweeper) reasonFor(ctx context.Context, c *callstate.Call) call.CallEndReason {
	if c.State == callstate.StateConnected {
		return call.CallEndReason_CALL_END_REASON_COMPLETED
	}
	if _, status, err := s.routes.LookupUser(ctx, c.CalleeID); err == nil && status == routing.RouteOffline {
		return call.CallEndReason_CALL_END_REASON_PEER_OFFLINE
	}
	return call.CallEndReason_CALL_END_REASON_MISSED
}

// notifyEnd 通知双方通话已终止。
// 主叫需要据此收起拨号界面，被叫需要收起可能还亮着的接听界面（如振铃中途丢失连接又回来）。
// 投递是尽力而为：离线方上线后会在会话里看到那条通话记录。
func (s *Sweeper) notifyEnd(ctx context.Context, c *callstate.Call, reason call.CallEndReason) {
	payload, err := proto.Marshal(&call.CallEnd{
		CallId:   c.CallID,
		Reason:   reason,
		EndAt:    c.EndedAt,
		Duration: c.Duration(),
	})
	if err != nil {
		logger.Errorf("[CallSweeper] marshal call end failed: %v", err)
		return
	}

	s.notifier.Publish(ctx, &transport.WSMessage{
		RouteTarget:     []uint64{c.CallerID, c.CalleeID},
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       c.EndedAt,
		Type:            transport.MessageType_CALL_END,
		Payload:         payload,
	})
}

// publishRecord 落库通话记录。仅在本实例是 Terminate 的 CAS 赢家时调用。
// JetStream 以 call:{callID} 去重，与网关侧主动挂断路径同键，双保险。
func (s *Sweeper) publishRecord(c *callstate.Call, reason call.CallEndReason) {
	record, err := util.NewCallRecordMsg(
		c.CallID, c.CallerID, c.CalleeID, c.SessionKey, c.MediaType, reason, c.Duration())
	if err != nil {
		logger.Errorf("[CallSweeper] build call record failed: %v", err)
		return
	}

	data, err := proto.Marshal(record)
	if err != nil {
		logger.Errorf("[CallSweeper] marshal call record failed: %v", err)
		return
	}

	if _, err := s.js.Publish(nats_util.DBSubject, data, nats.MsgId("call:"+c.CallID)); err != nil {
		logger.Errorf("[CallSweeper] publish call record failed: %v", err)
	}
}
