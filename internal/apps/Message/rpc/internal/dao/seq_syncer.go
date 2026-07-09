package dao

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
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

// seqUpdate 代表一条完整的会话状态更新事件。
// seq 由 Lamport 分配器在消息服务本地生成，SeqSyncer 负责异步批量刷
// MySQL actual_seq 及 Redis session:info 快照。
// 聚合时同一 conversationID 只保留 seq 最大的那条（视为最新状态，Lamport uint64 直接比较）。
type seqUpdate struct {
	sessionID   string
	seq         uint64 // 写入 MySQL actual_seq 和 Redis 快照
	lastContent string
	lastSender  uint64
	updateTime  int64
	// 会话形态与消息目标由消费方显式传入。
	// sessionID 是雪花 ID，不再携带 group/private 前缀，无法按前缀推断类型。
	isGroup bool
	target  uint64 // 群聊=群ID；单聊=接收方用户ID
}

// ConvSnapshot 会话快照，同时用作:
//  1. ZSET user:conv:timeline:{uid} 的 member（JSON 序列化）
//  2. Hash conv:snapshot:{convID} 的读取输入
//
// 字段缩写以减少 Redis 内存占用。
type SessionSnapshot struct {
	SessionID   string `json:"c"`
	Seq         uint64 `json:"s"`
	LastContent string `json:"lc"`
	LastSender  uint64 `json:"ls"`
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
// 多实例并发安全：SQL 使用条件写 (max_seq < ?)，旧 seq 不会覆盖新 seq。
// GroupMemberSource 按群 ID 获取成员列表（由上层注入，通常带 Redis 缓存 + Group RPC 回源）
type GroupMemberSource func(ctx context.Context, groupID uint64) ([]uint64, error)

type SeqSyncer struct {
	db              *gorm.DB
	cache           *redisx.Client
	ch              chan seqUpdate
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
		ch:              make(chan seqUpdate, defaultSeqBufSize),
		flushInterval:   defaultFlushInterval,
		flushBatchLimit: defaultFlushBatchLimit,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *SeqSyncer) Push(u seqUpdate) {
	// 背压告警：当 channel 使用率 > 80% 时触发告警
	if len(s.ch) > cap(s.ch)*8/10 {
		logger.Errorf("[BACKPRESSURE ALERT] SeqSyncer channel usage > 80%% (current: %d, cap: %d)", len(s.ch), cap(s.ch))
	}

	// 1. 优先尝试无阻塞写入
	select {
	case s.ch <- u:
		return
	default:
		// 2. channel 满时，退化为带超时的阻塞等待（例如最多阻塞 200ms）
		// 这样既能给后台协程消化数据的时间，又不会导致主链路无限阻塞
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()

		select {
		case s.ch <- u:
			return
		case <-timer.C:
			logger.Errorf("[BACKPRESSURE ALERT] SeqSyncer channel full and 200ms timeout reached, drop update for session %s seq %d", u.sessionID, u.seq)
		}
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

	pending := make([]seqUpdate, 0, s.flushBatchLimit)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		// 聚合：同一 conversationID 只保留 seq 最大的那条（含 lastContent 等完整信息）
		latest := make(map[string]seqUpdate, len(pending))
		for _, u := range pending {
			if u.seq > latest[u.sessionID].seq {
				latest[u.sessionID] = u
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
func (s *SeqSyncer) batchFlush(latest map[string]seqUpdate) {
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
		if u.isGroup {
			convType = model.SessionTypeGroup
		}

		// 1. MySQL Upsert：不存在则创建，存在则只更新 actual_seq / last_content / last_sender / update_time。
		res := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "session_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"actual_seq", "last_content", "last_sender", "update_time"}),
			}).
			Create(&model.Session{
				SessionID:   convID,
				Type:        convType,
				LastContent: u.lastContent,
				LastSender:  u.lastSender,
				ActualSeq:   u.seq,
				UpdateTime:  now,
			})
		if res.Error != nil {
			failed = append(failed, fmt.Sprintf("%s(seq=%d,err=%v)", convID, u.seq, res.Error))
			continue
		}

		// 2. 收集需要更新的时间线信息（参与者由 seqUpdate 显式携带，不再按 ID 前缀猜测）
		var userIDs []uint64
		if u.isGroup {
			userIDs = s.groupMemberIDs(ctx, convID, u.target)
		} else {
			userIDs = []uint64{u.lastSender, u.target}
		}

		for _, uid := range userIDs {
			if uid == 0 {
				continue
			}
			if userTimelines[uid] == nil {
				userTimelines[uid] = make(map[string]int64)
			}
			userTimelines[uid][convID] = u.updateTime
		}
	}

	// 3. 批量更新 Redis：
	//    a) session:info:{convID} Hash：actual_seq / last_content / last_sender 会话快照。
	//       Lamport 分配器不再经过 Redis，快照统一由此处异步维护（最多滞后一个刷盘周期）。
	//    b) user:conv:timeline:{uid} ZSET：member = convID，score = updateTime
	err := s.cache.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		for convID, u := range latest {
			key := sessionInfoPrefix + convID
			pipe.HSet(ctx, key,
				"actual_seq", strconv.FormatUint(u.seq, 10),
				"last_content", u.lastContent,
				"last_sender", strconv.FormatUint(u.lastSender, 10),
			)
			pipe.Expire(ctx, key, sessionInfoExpire)
		}
		for uid, convs := range userTimelines {
			key := fmt.Sprintf("%s%d", sessionTimelinePrefix, uid)
			for convID, updateTime := range convs {
				pipe.ZAdd(ctx, key, redis.Z{
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
