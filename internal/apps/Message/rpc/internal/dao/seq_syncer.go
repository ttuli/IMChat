package dao

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/util"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultSeqBufSize      = 32768
	defaultFlushInterval   = 3 * time.Second
	defaultFlushBatchLimit = 500 // 定量触发：攒满 500 条即提前刷盘
)

// SeqUpdate 代表一条完整的会话状态更新事件。
// seq 由 Lamport 分配器在消息服务本地生成，SeqSyncer 负责异步批量刷
// MySQL actual_seq 及 Redis session:info 快照。
// 聚合时同一 conversationID 只保留 seq 最大的那条（视为最新状态，Lamport uint64 直接比较）。
type SeqUpdate struct {
	SessionID   string
	SessionKey  string // 业务主键（群号或单聊双方 ID 拼接），随快照缓存供读路径直接命中
	SessionType int
	Seq         uint64 // 写入 MySQL actual_seq 和 Redis 快照
	LastContent string
	LastSender  uint64
	UpdateTime  int64
	// 会话形态与消息目标由消费方显式传入。
	// sessionID 是雪花 ID，不再携带 group/private 前缀，无法按前缀推断类型。
	// target  uint64 // 群聊=群ID；单聊=接收方用户ID
}

// SeqSyncer 进程内 channel + 定时/定量批量刷盘器
// 职责：
//  1. 将会话状态（max_seq / last_content / last_sender）批量持久化到 MySQL
//  2. 批量更新参与该会话的用户的 Redis 活跃时间线 (user:conv:timeline:{userID})
//
// 双触发条件：
//   - 定时：每隔 flushInterval（默认 3 秒）触发一次
//   - 定量：channel 内积压达到 flushBatchLimit（默认 500）时立即触发
//
// 多实例并发安全：MySQL 按 seq 条件更新（GREATEST/IF），Redis 快照经 Lua 按 seq
// 条件写，时间线 ZSET 用 ZAddGT——旧 seq / 旧批次不会覆盖新状态。
//
// GroupMemberSource 按群 ID 获取成员列表（由上层注入，通常带 Redis 缓存 + Group RPC 回源）
type GroupMemberSource func(ctx context.Context, groupID uint64) ([]uint64, error)

// snapshotUpdateScript 更新会话完整快照（读路径 FindSessionsByIDs 可直接命中，
// 不必回源 MySQL）。随消息变化的字段仅当新 seq 更大时更新；session_key / type
// 不可变，无条件补写（兼容旧格式条目缺失这两个字段的情况）；无论是否更新都续期。
// seq 是 uint64，超出 Lua number（double，53 bit）的精确整数范围，故用
// 字符串比较：先比长度再比字典序（等长十进制字符串的字典序即数值序）。
// KEYS[1] = session:info:{convID}
// ARGV = seq, last_content, last_sender, update_time, session_key, type, expire_seconds, session_id
const snapshotUpdateScript = `
local cur = redis.call('HGET', KEYS[1], 'actual_seq')
local new = ARGV[1]
if (not cur) or (#cur < #new) or (#cur == #new and cur < new) then
	redis.call('HSET', KEYS[1], 'actual_seq', ARGV[1], 'last_content', ARGV[2], 'last_sender', ARGV[3], 'update_time', ARGV[4])
end
redis.call('HSET', KEYS[1], 'session_key', ARGV[5], 'type', ARGV[6], 'session_id', ARGV[8])
redis.call('EXPIRE', KEYS[1], ARGV[7])
return 1
`

type SeqSyncer struct {
	db              *gorm.DB
	cache           *redisx.Client
	ch              chan SeqUpdate
	flushInterval   time.Duration
	flushBatchLimit int
	stopCh          chan struct{}
	doneCh          chan struct{}

	memberMu     sync.RWMutex
	memberSource GroupMemberSource
}

// newSeqSyncer 创建并启动 SeqSyncer 后台 goroutine
func newSeqSyncer(db *gorm.DB, cache *redisx.Client) *SeqSyncer {
	s := &SeqSyncer{
		db:              db,
		cache:           cache,
		ch:              make(chan SeqUpdate, defaultSeqBufSize),
		flushInterval:   defaultFlushInterval,
		flushBatchLimit: defaultFlushBatchLimit,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *SeqSyncer) Push(u SeqUpdate) {
	// 背压告警：当 channel 使用率 > 80% 时触发告警
	if len(s.ch) > cap(s.ch)*8/10 {
		logger.Errorf("[BACKPRESSURE ALERT] SeqSyncer channel usage > 80%% (current: %d, cap: %d)", len(s.ch), cap(s.ch))
	}

	// 1. 优先尝试无阻塞写入
	select {
	case s.ch <- u:
		return
	default:
	}

	// 2. channel 满时，退化为带超时的阻塞等待，给后台协程消化数据的时间
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case s.ch <- u:
	case <-timer.C:
		// 3. 超时仍未入队：降级为同步直写，保证事件不丢——否则会话表 actual_seq
		// 与 Redis 时间线将长期滞后（若此后该会话无新消息则永久 stale）。
		// 代价是阻塞当前消费 worker，形成对上游拉取的真实背压；
		// 与后台批量刷盘并发时由 MySQL/Redis 的 seq 条件写保证不回退。
		logger.Errorf("[BACKPRESSURE ALERT] SeqSyncer channel full, degrade to synchronous flush for session %s seq %d", u.SessionID, u.Seq)
		s.batchFlush(map[string]SeqUpdate{u.SessionID: u})
	}
}

// Stop 优雅关闭：等待当前批次刷完后退出
func (s *SeqSyncer) Stop() {
	close(s.stopCh)
	<-s.doneCh
}

// run 是后台 goroutine，实现双触发刷盘逻辑
func (s *SeqSyncer) run() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	pending := make([]SeqUpdate, 0, s.flushBatchLimit)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		// 聚合：同一 conversationID 只保留 seq 最大的那条（含 lastContent 等完整信息）
		latest := make(map[string]SeqUpdate, len(pending))
		for _, u := range pending {
			if u.Seq > latest[u.SessionID].Seq {
				latest[u.SessionID] = u
			}
		}
		pending = pending[:0]
		s.batchFlush(latest)
	}

	for {
		select {
		case u, ok := <-s.ch:
			if !ok {
				flush()
				return
			}
			pending = append(pending, u)
			// 定量触发
			if len(pending) >= s.flushBatchLimit {
				flush()
			}

		case <-ticker.C:
			// 定时触发时 drain channel，避免本批次遗漏刚入队的事件
			draining := true
			for draining {
				select {
				case u := <-s.ch:
					pending = append(pending, u)
				default:
					draining = false
				}
			}
			flush()

		case <-s.stopCh:
			// 收到停止信号，drain 剩余后退出
			draining := true
			for draining {
				select {
				case u := <-s.ch:
					pending = append(pending, u)
				default:
					draining = false
				}
			}
			flush()
			return
		}
	}
}

// SetMemberSource 注入群成员来源（服务启动时调用一次）
func (s *SeqSyncer) SetMemberSource(src GroupMemberSource) {
	s.memberMu.Lock()
	s.memberSource = src
	s.memberMu.Unlock()
}

func (s *SeqSyncer) getMemberSource() GroupMemberSource {
	s.memberMu.RLock()
	defer s.memberMu.RUnlock()
	return s.memberSource
}

// groupMemberIDs 获取群会话的时间线更新对象。
// 优先走注入的成员来源（权威、含缓存）；未注入或失败时降级按 user_session 反查
// （降级结果可能包含已退群用户，仅造成其时间线一次多余更新，无正确性问题）。
func (s *SeqSyncer) groupMemberIDs(ctx context.Context, convID string, groupID uint64) []uint64 {
	if src := s.getMemberSource(); src != nil && groupID != 0 {
		ids, err := src(ctx, groupID)
		if err == nil {
			return ids
		}
		logger.Errorf("SeqSyncer load members of group %d failed, fallback to user_session: %v", groupID, err)
	}
	var userIDs []uint64
	s.db.WithContext(ctx).Model(&model.UserSession{}).
		Where("session_id = ?", convID).
		Pluck("user_id", &userIDs)
	return userIDs
}

// batchFlush 将聚合后的会话状态批量写入 MySQL，并更新 Redis 活跃时间线
func (s *SeqSyncer) batchFlush(latest map[string]SeqUpdate) {
	if len(latest) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now()
	var failed []string

	// 用于收集需要刷入 Redis timeline 的数据
	userTimelines := make(map[uint64]map[string]int64) // uid -> convID -> updateTime

	for convID, u := range latest {
		convType := model.SessionTypeSingle
		isGroup := u.SessionType == int(message.SessionType_SESSION_TYPE_GROUP)
		if isGroup {
			convType = model.SessionTypeGroup
		}

		// 1. MySQL Upsert：不存在则创建，存在则按 seq 条件更新——仅当新 seq 更大时
		// 才覆盖，保证多实例批量刷盘 / 同步直写并发时旧批次不能回退新状态。
		// 注意赋值顺序：MySQL 按 SET 从左到右求值，actual_seq 必须放在最后更新，
		// 否则前面字段的比较条件会读到已被覆盖的新值。
		res := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "session_id"}},
				DoUpdates: clause.Set{
					{Column: clause.Column{Name: "last_content"}, Value: gorm.Expr("IF(VALUES(actual_seq) > actual_seq, VALUES(last_content), last_content)")},
					{Column: clause.Column{Name: "last_sender"}, Value: gorm.Expr("IF(VALUES(actual_seq) > actual_seq, VALUES(last_sender), last_sender)")},
					{Column: clause.Column{Name: "update_time"}, Value: gorm.Expr("IF(VALUES(actual_seq) > actual_seq, VALUES(update_time), update_time)")},
					{Column: clause.Column{Name: "actual_seq"}, Value: gorm.Expr("GREATEST(actual_seq, VALUES(actual_seq))")},
				},
			}).
			Create(&model.Session{
				SessionID:   convID,
				Type:        convType,
				SessionKey:  u.SessionKey,
				LastContent: u.LastContent,
				LastSender:  u.LastSender,
				ActualSeq:   u.Seq,
				UpdateTime:  now,
			})
		if res.Error != nil {
			failed = append(failed, fmt.Sprintf("%s(seq=%d,err=%v)", convID, u.Seq, res.Error))
			continue
		}

		// 2. 收集需要更新的时间线信息（参与者由 SeqUpdate 显式携带，不再按 ID 前缀猜测）
		target, _ := util.GetTargetIdFromSessionKey(u.SessionKey, u.LastSender)

		var userIDs []uint64
		if isGroup {
			userIDs = s.groupMemberIDs(ctx, convID, target)
		} else {
			userIDs = []uint64{u.LastSender, target}
		}

		for _, uid := range userIDs {
			if uid == 0 {
				continue
			}
			if userTimelines[uid] == nil {
				userTimelines[uid] = make(map[string]int64)
			}
			userTimelines[uid][convID] = u.UpdateTime
		}
	}

	// 3. 批量更新 Redis：
	//    a) session:info:{convID} Hash：会话完整快照（session_key / type / actual_seq /
	//       last_content / last_sender / update_time），读路径可整条命中不回源 MySQL。
	//       Lamport 分配器不再经过 Redis，快照统一由此处异步维护（最多滞后一个刷盘周期）。
	//       通过 Lua 按 seq 条件写，并发刷盘时旧批次不能回退新快照。
	//    b) user:conv:timeline:{uid} ZSET：member = convID，score = updateTime。
	//       ZAddGT 只允许 score 前进，防止并发时旧 updateTime 回退时间线排序。
	err := s.cache.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		for convID, u := range latest {
			key := sessionInfoPrefix + convID
			convType := model.SessionTypeSingle
			isGroup := u.SessionType == int(message.SessionType_SESSION_TYPE_GROUP)
			if isGroup {
				convType = model.SessionTypeGroup
			}
			pipe.Eval(ctx, snapshotUpdateScript, []string{key},
				strconv.FormatUint(u.Seq, 10),
				u.LastContent,
				strconv.FormatUint(u.LastSender, 10),
				strconv.FormatInt(u.UpdateTime, 10),
				u.SessionKey,
				strconv.Itoa(int(convType)),
				int(sessionInfoExpire.Seconds()),
				u.SessionID,
			)
		}
		for uid, convs := range userTimelines {
			key := fmt.Sprintf("%s%d", sessionTimelinePrefix, uid)
			for convID, updateTime := range convs {
				pipe.ZAddGT(ctx, key, redis.Z{
					Score:  float64(updateTime),
					Member: convID,
				})
			}
			// 30 天过期，避免死号长期占用内存
			pipe.Expire(ctx, key, 30*24*time.Hour)
		}
		return nil
	})
	if err != nil {
		logger.Errorf("SeqSyncer update redis failed: %v", err)
	}

	if len(failed) > 0 {
		logger.Errorf("SeqSyncer batchFlush partial failure: %v", failed)
	} else {
		logger.Infof("SeqSyncer batchFlush ok: %d conversations flushed DB, %d users timeline updated", len(latest), len(userTimelines))
	}
}
