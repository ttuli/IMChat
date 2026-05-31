package dao

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"IM2/internal/model"
	"IM2/pkg/proto/util"
	"IM2/pkg/logger"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

const (
	defaultSeqBufSize      = 4096
	defaultFlushInterval   = 3 * time.Second
	defaultFlushBatchLimit = 500 // 定量触发：攒满 500 条即提前刷盘
)

// seqUpdate 代表一条完整的会话状态更新事件。
// 号段模式下 MySQL max_seq 已由 allocSegment 实时写入，SeqSyncer 只负责异步批量刷下面三个元数据字段。
// 聚合时同一 conversationID 只保留 seq 最大的那条（视为最新状态）。
type seqUpdate struct {
	conversationID string
	seq            uint64 // 仅用于内部聚合去重，不写入 MySQL max_seq
	lastContent    string
	lastSender     uint64
	updateTime     int64
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
type SeqSyncer struct {
	db              *gorm.DB
	cache           *redisx.Client
	ch              chan seqUpdate
	flushInterval   time.Duration
	flushBatchLimit int
	stopCh          chan struct{}
	doneCh          chan struct{}
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

// Push 非阻塞推送一条完整的会话状态更新事件。
// channel 满时丢弃并打错误日志（不阻塞主链路）。
func (s *SeqSyncer) Push(u seqUpdate) {
	select {
	case s.ch <- u:
	default:
		logger.Errorf("SeqSyncer: channel full, drop update for conv %s seq %d", u.conversationID, u.seq)
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
			if u.seq > latest[u.conversationID].seq {
				latest[u.conversationID] = u
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

// batchFlush 将聚合后的会话状态批量写入 MySQL，并更新 Redis 活跃时间线
func (s *SeqSyncer) batchFlush(latest map[string]seqUpdate) {
	if len(latest) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now()
	var failed []string

	// 用于收集需要刷入 Redis 的 timeline 数据
	userTimelines := make(map[uint64]map[string]int64)

	for convID, u := range latest {
		// 1. MySQL 条件写：只更新 last_content / last_sender / update_time。
		// 号段模式下 max_seq 已由 allocSegment 实时写入 MySQL，无需在此同步。
		res := s.db.WithContext(ctx).
			Model(&model.Conversation{}).
			Where("conversation_id = ?", convID).
			Updates(map[string]interface{}{
				"last_content": u.lastContent,
				"last_sender":  u.lastSender,
				"update_time":  now,
			})
		if res.Error != nil {
			failed = append(failed, fmt.Sprintf("%s(seq=%d,err=%v)", convID, u.seq, res.Error))
			continue
		}

		// 2. 收集需要更新的时间线信息
		var userIDs []uint64
		if util.IsGroupSession(convID) {
			// 从 DB 查群成员 (UserConversation 记录了用户参与了哪些会话)
			s.db.WithContext(ctx).Model(&model.UserConversation{}).
				Where("conversation_id = ?", convID).
				Pluck("user_id", &userIDs)
		} else if util.IsPrivateSession(convID) {
			// 单谈直接从 SessionID 提取双方 ID
			parts := strings.Split(convID, "_")
			if len(parts) == 3 {
				u1, _ := strconv.ParseUint(parts[1], 10, 64)
				u2, _ := strconv.ParseUint(parts[2], 10, 64)
				userIDs = append(userIDs, u1, u2)
			}
		}

		for _, uid := range userIDs {
			if userTimelines[uid] == nil {
				userTimelines[uid] = make(map[string]int64)
			}
			userTimelines[uid][convID] = u.updateTime
		}
	}

	// 3. 批量更新 Redis Timeline
	if len(userTimelines) > 0 {
		err := s.cache.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
			for uid, convs := range userTimelines {
				key := fmt.Sprintf("user:conv:timeline:%d", uid)
				for convID, updateTime := range convs {
					pipe.ZAdd(ctx, key, redis.Z{
						Score:  float64(updateTime),
						Member: convID,
					})
				}
				// 为 timeline key 设置 30 天过期时间，避免死号长期占用内存
				pipe.Expire(ctx, key, 30*24*time.Hour)
			}
			return nil
		})
		if err != nil {
			logger.Errorf("SeqSyncer update redis timeline failed: %v", err)
		}
	}

	if len(failed) > 0 {
		logger.Errorf("SeqSyncer batchFlush partial failure: %v", failed)
	} else {
		logger.Infof("SeqSyncer batchFlush ok: %d conversations flushed DB, %d users timeline updated", len(latest), len(userTimelines))
	}
}
