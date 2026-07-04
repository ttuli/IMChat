package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/util"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	sessionTimelinePrefix = "user:session:timeline:"
	sessionInfoPrefix     = "session:info:"
	sessionInfoExpire     = 7 * 24 * time.Hour
	defaultSegmentStep    = uint64(100) // 号段步长：每次向 MySQL 预申请的 seq 数量
)

// SessionDAO 会话数据访问对象 (MySQL + Redis缓存)
type SessionDAO struct {
	db        *gorm.DB
	cache     *redisx.Client
	seqSyncer *SeqSyncer
}

// NewSessionDAO 创建会话DAO
func NewSessionDAO(dbSource string, redisConf redis.RedisConf) *SessionDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	client, err := redisx.NewClient(redisConf)
	if err != nil {
		panic(err)
	}
	return &SessionDAO{
		db:        db,
		cache:     client,
		seqSyncer: newSeqSyncer(db, client),
	}
}

// PushSeqUpdate 将完整的会话状态推送到 SeqSyncer，由后台批量刷 MySQL + 广播 UpdateSession。
// 非阻塞：channel 满时打日志丢弃，不影响主链路。
func (c *SessionDAO) PushSeqUpdate(sessionID string, seq uint64, lastContent string, lastSender uint64, updateTime int64) {
	c.seqSyncer.Push(seqUpdate{
		sessionID:   sessionID,
		seq:         seq,
		lastContent: lastContent,
		lastSender:  lastSender,
		updateTime:  updateTime,
	})
}

// FindSessionsByIDs 批量查询会话。
// 策略：先通过 Lua 脚本批量读取 Redis session:info:{id} Hash 中的快照字段，
//
//	Redis 没命中的会话再退化批量查询 MySQL。
func (c *SessionDAO) FindSessionsByIDs(ctx context.Context, sessionIDs []string) ([]*model.Session, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	// 1. 批量读 Redis：从 session:info:{id} Hash 中读取 actual_seq / last_content / last_sender
	// session:info 是号段模式下已有的 Key，全局最新，不必新增决策 Key
	batchScript := `
		local results = {}
		for i = 1, #KEYS do
			local v = redis.call('HMGET', KEYS[i], 'actual_seq', 'last_content', 'last_sender')
			results[i] = v
		end
		return results
	`
	keys := make([]string, len(sessionIDs))
	for i, id := range sessionIDs {
		keys[i] = sessionInfoPrefix + id
	}
	luaRaw, err := c.cache.EvalCtx(ctx, batchScript, keys)

	result := make([]*model.Session, 0, len(sessionIDs))
	missingIDs := make([]string, 0)

	if err == nil {
		rows, ok := luaRaw.([]interface{})
		if ok && len(rows) == len(sessionIDs) {
			for i, row := range rows {
				fields, ok := row.([]interface{})
				if !ok || len(fields) != 3 || fields[0] == nil {
					missingIDs = append(missingIDs, sessionIDs[i])
					continue
				}
				actualSeq, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[0]), 10, 64)
				lastSender, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[2]), 10, 64)
				result = append(result, &model.Session{
					SessionID:   sessionIDs[i],
					ActualSeq:   actualSeq,
					LastContent: fmt.Sprintf("%s", fields[1]),
					LastSender:  lastSender,
				})
			}
		} else {
			missingIDs = append(missingIDs, sessionIDs...)
		}
	} else {
		// Redis 全量失败，全部去查 DB
		missingIDs = append(missingIDs, sessionIDs...)
	}

	// 2. 对于 Redis 没命中的批量查 DB
	if len(missingIDs) > 0 {
		var dbSessions []*model.Session
		if err := c.db.WithContext(ctx).
			Where("session_id IN ?", missingIDs).
			Find(&dbSessions).Error; err != nil {
			return nil, err
		}
		result = append(result, dbSessions...)
	}
	return result, nil
}

// FindBySessionID 按 session_id 精确查询单条会话
func (c *SessionDAO) FindBySessionID(ctx context.Context, sessionID string) (*model.Session, error) {
	var session model.Session
	if err := c.db.WithContext(ctx).Where("session_id = ?", sessionID).First(&session).Error; err != nil {
		return nil, err
	}
	return &session, nil
}

// FindOrCreateBySessionKey 按 session_key 查询会话；若不存在则创建新会话并返回。
// newSessionID 为调用方预先生成的雪花 ID，仅在需要创建时使用。
func (c *SessionDAO) FindOrCreateBySessionKey(ctx context.Context, newSessionID string, sessionKey string, sessionType int8) (*model.Session, bool, error) {
	// 1. 查 DB
	var existing model.Session
	err := c.db.WithContext(ctx).
		Where("session_key = ?", sessionKey).
		First(&existing).Error
	if err == nil {
		return &existing, false, nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, false, err
	}

	// 2. 不存在，创建
	now := time.Now()
	session := &model.Session{
		SessionID:  newSessionID,
		Type:       sessionType,
		SessionKey: sessionKey,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "session_key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"update_time": now}),
	}).Create(session).Error; err != nil {
		return nil, false, err
	}
	return session, true, nil
}

// FindUserSessions 查询用户的会话列表 (按最后消息时间倒序)
func (c *SessionDAO) FindUserSessions(ctx context.Context, userID uint64) ([]*model.UserSession, error) {
	var userSessions []*model.UserSession
	if err := c.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("update_time DESC").
		Find(&userSessions).Error; err != nil {
		return nil, err
	}
	return userSessions, nil
}

// ClearUnread 清零未读并更新已读游标
func (c *SessionDAO) ClearUnread(ctx context.Context, userID uint64, sessionID string, lastReadMsgID, lastReadSeq uint64) error {
	return c.db.WithContext(ctx).Model(&model.UserSession{}).
		Where("user_id = ? AND session_id = ?", userID, sessionID).
		Updates(map[string]any{
			"unread_count":     0,
			"last_read_msg_id": lastReadMsgID,
			"last_read_seq":    lastReadSeq,
		}).Error
}

// UpdateUserSession 更新用户会话设置 (置顶/免打扰/静音)
func (c *SessionDAO) UpdateUserSession(ctx context.Context, userID uint64, sessionID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}

	// 构造完整的主键和默认值，用于不存在时的 Create
	userSession := model.UserSession{
		UserID:    userID,
		SessionID: sessionID,
		IsTop:     1, // 默认 1
		IsDisturb: 1, // 默认 1
	}

	// 将需要更新的字段也覆盖到结构体上，保证初次 Create 时的值是设置后的业务值
	if v, ok := updates["is_top"].(int32); ok {
		userSession.IsTop = int8(v)
	}
	if v, ok := updates["is_disturb"].(int32); ok {
		userSession.IsDisturb = int8(v)
	}

	// 执行 Upsert：冲突时按 updates 里的特定字段执行更新
	return c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "session_id"}},
		DoUpdates: clause.Assignments(updates),
	}).Create(&userSession).Error
}

// InsertUserSession 插入新的用户会话
func (c *SessionDAO) InsertUserSession(ctx context.Context, userId uint64, sessionId string, sessionType int8) error {
	// 从 Redis hash 的 seg_ceiling 字段读取当前已分配的最大 seq，作为新成员的已读起点。
	// 若缓存未命中则查 MySQL max_seq。
	maxSeq := c.currentMaxSeq(ctx, sessionId)

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Session 表 (存在则只更新 update_time)
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "session_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Session{
			SessionID: sessionId,
			Type:      sessionType,
		}).Error
		if err != nil {
			return err
		}

		// 2. 插入 UserSession 记录，忽略冲突
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.UserSession{
			UserID:      userId,
			SessionID:   sessionId,
			IsTop:       1,
			IsDisturb:   1,
			LastReadSeq: maxSeq,
		}).Error
	})
}

// Transaction 提供开启事务的能力
func (c *SessionDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return c.db.WithContext(ctx).Transaction(fc)
}

// BatchInsertUserSessions 批量插入用户会话记录
func (c *SessionDAO) BatchInsertUserSessions(ctx context.Context, userSessions []*model.UserSession, sessionType int8) error {
	if len(userSessions) == 0 {
		return nil
	}

	sessionId := userSessions[0].SessionID
	maxSeq := c.currentMaxSeq(ctx, sessionId)

	for _, session := range userSessions {
		session.LastReadSeq = maxSeq
	}

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Session 表
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "session_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Session{
			SessionID: sessionId,
			Type:      sessionType,
		}).Error
		if err != nil {
			return err
		}

		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&userSessions).Error
	})
}

// GetActiveSessionIDs 获取活跃的会话 ID 列表，按时间戳过滤大于 sinceTimestamp 的记录
func (c *SessionDAO) GetActiveSessionIDs(ctx context.Context, userID uint64, sinceTimestamp int64) ([]string, error) {
	snaps, err := c.getActiveSessionSnapshots(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(snaps))
	for _, s := range snaps {
		ids = append(ids, s.SessionID)
	}
	return ids, nil
}

// getActiveSessionSnapshots 获取在 sinceTimestamp 后有更新的会话快照列表。
//
// 流程：
//  1. 从 ZSET user:session:timeline:{uid} 读取活跃会话 ID 列表（member = sessionID，score = updateTime）
//  2. 批量 HMGET Redis session:info:{id} 获取快照字段（actual_seq / last_content / last_sender）
//  3. Redis 没命中的删退化批量查询 MySQL
func (c *SessionDAO) getActiveSessionSnapshots(ctx context.Context, userID uint64, sinceTimestamp int64) ([]SessionSnapshot, error) {
	key := fmt.Sprintf("%s%d", sessionTimelinePrefix, userID)

	// Step 1: 从 ZSET 获取会话 ID
	pairs, err := c.cache.ZRangeByScoreWithScoresCtx(ctx, key, sinceTimestamp+1, time.Now().UnixMilli()+100000)
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		logger.Errorf("get updated sessions failed for user %d: %v", userID, err)
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}

	// 降序排列（最新的排在前面）
	sessionIDs := make([]string, 0, len(pairs))
	for i := len(pairs) - 1; i >= 0; i-- {
		sessionIDs = append(sessionIDs, pairs[i].Key)
	}

	// Step 2: 批量 HMGET session:info Hash
	batchScript := `
		local results = {}
		for i = 1, #KEYS do
			results[i] = redis.call('HMGET', KEYS[i], 'actual_seq', 'last_content', 'last_sender')
		end
		return results
	`
	luaKeys := make([]string, len(sessionIDs))
	for i, id := range sessionIDs {
		luaKeys[i] = sessionInfoPrefix + id
	}
	luaRaw, luaErr := c.cache.EvalCtx(ctx, batchScript, luaKeys)

	result := make([]SessionSnapshot, 0, len(sessionIDs))
	missingIDs := make([]string, 0)

	if luaErr == nil {
		rows, ok := luaRaw.([]interface{})
		if ok && len(rows) == len(sessionIDs) {
			for i, row := range rows {
				fields, ok := row.([]interface{})
				if !ok || len(fields) != 3 || fields[0] == nil {
					missingIDs = append(missingIDs, sessionIDs[i])
					continue
				}
				actualSeq, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[0]), 10, 64)
				lastSender, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[2]), 10, 64)
				result = append(result, SessionSnapshot{
					SessionID:   sessionIDs[i],
					Seq:         actualSeq,
					LastContent: fmt.Sprintf("%s", fields[1]),
					LastSender:  lastSender,
				})
			}
		} else {
			missingIDs = append(missingIDs, sessionIDs...)
		}
	} else {
		missingIDs = append(missingIDs, sessionIDs...)
	}

	// Step 3: DB 退化查询
	if len(missingIDs) > 0 {
		var dbSessions []*model.Session
		if dbErr := c.db.WithContext(ctx).
			Where("session_id IN ?", missingIDs).
			Find(&dbSessions).Error; dbErr != nil {
			return nil, dbErr
		}
		for _, session := range dbSessions {
			result = append(result, SessionSnapshot{
				SessionID:   session.SessionID,
				Seq:         session.ActualSeq,
				LastContent: session.LastContent,
				LastSender:  session.LastSender,
			})
		}
	}
	return result, nil
}

// IncrSeq 以号段模式递增会话的 seq。
//
// 设计：
//   - Redis Hash（session:info:{id}）存两个字段：
//     cur_seq    当前已分配到的最大 seq（各实例共享的原子计数器）
//     seg_ceiling 当前号段的上限（对应 MySQL max_seq 的已预申请值）
//   - 快路径：cur_seq <= seg_ceiling，直接 HINCRBY 返回，无 DB 访问。
//   - 慢路径：cur_seq > seg_ceiling 或 Redis 冷启动，
//     去 MySQL 原子扩容 max_seq += step，
//     用 Lua 将 seg_ceiling 更新至新上限并保证 cur_seq 不低于新号段起点，
//     从而彻底消除 Redis 宕机/重启导致的 seq 回退。
func (c *SessionDAO) IncrSeq(ctx context.Context, sessionID string, lastContent string, lastSender uint64) (uint64, error) {
	cacheKey := sessionInfoPrefix + sessionID
	expireSecs := strconv.Itoa(int(sessionInfoExpire.Seconds()))
	lastSenderStr := strconv.FormatUint(lastSender, 10)

	// 快路径：HINCRBY cur_seq，同时读 seg_ceiling，判断是否在号段内。
	// 若在号段内，在同一个 Lua 内将 actual_seq / last_content / last_sender 同步写入，共一次网络往返。
	fastScript := `
		local cur = redis.call("HINCRBY", KEYS[1], "cur_seq", 1)
		local ceil = tonumber(redis.call("HGET", KEYS[1], "seg_ceiling")) or 0
		redis.call("HSET", KEYS[1],
			"actual_seq",   tostring(cur),
			"last_content", ARGV[2],
			"last_sender",  ARGV[3])

		redis.call("EXPIRE", KEYS[1], ARGV[1])
		return {cur, ceil}
	`
	fastVal, err := c.cache.EvalCtx(ctx, fastScript, []string{cacheKey}, expireSecs, lastContent, lastSenderStr)
	if err != nil {
		return 0, fmt.Errorf("redis eval fast script failed: %w", err)
	}

	result, ok := fastVal.([]interface{})
	if !ok || len(result) != 2 {
		return 0, fmt.Errorf("unexpected redis result type: %T", fastVal)
	}
	toInt64 := func(v interface{}) int64 {
		switch x := v.(type) {
		case int64:
			return x
		case int:
			return int64(x)
		}
		return 0
	}
	cur := uint64(toInt64(result[0]))
	segCeiling := uint64(toInt64(result[1]))

	if cur <= segCeiling {
		// 快路径：在当前号段内，直接返回。
		return cur, nil
	}

	// 慢路径：号段耗尽或冷启动，向 MySQL 申请新号段。
	newCeiling, err := c.allocSegment(ctx, sessionID, cacheKey, expireSecs)
	if err != nil {
		return 0, err
	}

	newBase := newCeiling - uint64(defaultSegmentStep) + 1
	if cur >= newBase && cur <= newCeiling {
		// 本次 HINCRBY 拿到的 cur 落在新号段内，直接使用。
		return cur, nil
	}

	// cur 不在有效号段内，分两种情况：
	// 1. cur < newBase：Redis 冷启动，allocSegment 已将 cur_seq 重置到 newBase，
	//    本次拿到的 cur（如 1）是已用过的旧 seq，必须重试。
	// 2. cur > newCeiling：并发竞争导致号段耗尽，同样重试。
	return c.IncrSeq(ctx, sessionID, lastContent, lastSender)
}

// allocSegment 原子地向 MySQL 申请下一个号段，并更新 Redis 中的 seg_ceiling 和 cur_seq。
//
// 参数 curSeq 是触发扩容的那次 HINCRBY 拿到的值，用于判断 Lua 里是否需要重置 cur_seq。
// 返回新的 seg_ceiling（即 MySQL 最新的 max_seq）。
func (c *SessionDAO) allocSegment(
	ctx context.Context,
	sessionID, cacheKey string,
	expireSecs string,
) (uint64, error) {
	// 1. MySQL 原子申请号段：INSERT 不存在则创建，存在则 max_seq += step。
	//    合并 SELECT 操作，消除读写间隙
	now := time.Now()
	step := defaultSegmentStep

	session := model.Session{
		SessionID:  sessionID,
		Type:       int8(util.GetSessionType(sessionID)),
		MaxSeq:     step, // 初次创建时 max_seq = step
		CreateTime: now,
		UpdateTime: now,
	}

	err := c.db.WithContext(ctx).Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "session_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"max_seq": gorm.Expr("max_seq + ?", step),
			}),
		},
		clause.Returning{Columns: []clause.Column{{Name: "max_seq"}}},
	).Create(&session).Error
	if err != nil {
		return 0, fmt.Errorf("allocSegment: upsert session failed: %w", err)
	}

	newCeiling := session.MaxSeq
	newBase := newCeiling - step + 1 // 新号段起点

	// 3. 原子更新 Redis：
	//    - seg_ceiling 设置为新上限（使用 HSET，Last-Write-Wins，多实例并发时取最大值即可）
	//    - cur_seq：若当前值低于 new_base（冷启动/旧数据），重置到 new_base，避免分配已用过的 seq。
	allocScript := `
		local new_ceiling = tonumber(ARGV[1])
		local new_base    = tonumber(ARGV[2])
		local expire      = tonumber(ARGV[3])

		-- 仅当新上限更大时才更新（防止并发时旧实例覆盖新实例的更大号段）
		local old_ceil = tonumber(redis.call("HGET", KEYS[1], "seg_ceiling")) or 0
		if new_ceiling > old_ceil then
			redis.call("HSET", KEYS[1], "seg_ceiling", new_ceiling)
		end

		-- 若 cur_seq 低于新号段起点（冷启动），重置到 new_base
		local cur = tonumber(redis.call("HGET", KEYS[1], "cur_seq")) or 0
		if cur < new_base then
			redis.call("HSET", KEYS[1], "cur_seq", new_base)
		end

		redis.call("EXPIRE", KEYS[1], expire)
		return new_ceiling
	`
	_, err = c.cache.EvalCtx(ctx, allocScript, []string{cacheKey},
		strconv.FormatUint(newCeiling, 10),
		strconv.FormatUint(newBase, 10),
		expireSecs,
	)
	if err != nil {
		logger.Errorf("allocSegment: redis update failed (non-fatal): %v", err)
	}
	return newCeiling, nil
}

// currentMaxSeq 返回当前会话已分配的最大 seq。
// 优先读 Redis Hash 的 seg_ceiling 字段，降级读 MySQL max_seq。
// 用于新成员加入时初始化 LastReadSeq。
func (c *SessionDAO) currentMaxSeq(ctx context.Context, sessionID string) uint64 {
	cacheKey := sessionInfoPrefix + sessionID
	if val, err := c.cache.HGetCtx(ctx, cacheKey, "seg_ceiling"); err == nil && val != "" {
		if seq, err := strconv.ParseUint(val, 10, 64); err == nil {
			return seq
		}
	}
	// 降级读 MySQL
	var session model.Session
	if err := c.db.WithContext(ctx).Select("max_seq").Where("session_id = ?", sessionID).First(&session).Error; err == nil {
		return session.MaxSeq
	}
	return 0
}

// ======================== 会话注册方法 ========================

const sessionMetaPrefix = "session:meta:"
const sessionMetaExpire = 30 * 24 * time.Hour

// RegisterGroupSession 为群组注册一个群聊会话。
// 如果群组已经有对应会话，将直接返回已有的 SessionID，并将增量成员加入 user_session。
// sessionID 是外部传入的预备用雪花 ID 字符串。
func (c *SessionDAO) RegisterGroupSession(ctx context.Context, preSessionID string, groupID uint64, memberIDs []uint64) (string, error) {
	// 查找是否已存在
	var existing model.Session
	err := c.db.WithContext(ctx).Where("type = ? AND session_key = ?", model.SessionTypeGroup, fmt.Sprintf("%d", groupID)).First(&existing).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return "", err
	}

	sessionID := preSessionID
	isExist := (err == nil)
	if isExist {
		sessionID = existing.SessionID
	}

	// 1. 事务写入 session + user_session
	err = c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1a. 如果不存在，则创建 session 表
		if !isExist {
			err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "session_id"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"update_time": time.Now(),
				}),
			}).Create(&model.Session{
				SessionID:  sessionID,
				Type:       model.SessionTypeGroup,
				SessionKey: fmt.Sprintf("%d", groupID),
			}).Error
			if err != nil {
				return err
			}
		}

		// 1b. 批量初始化成员的 user_session
		if len(memberIDs) > 0 {
			userSessions := make([]*model.UserSession, 0, len(memberIDs))
			for _, uid := range memberIDs {
				userSessions = append(userSessions, &model.UserSession{
					UserID:      uid,
					SessionID:   sessionID,
					IsTop:       1,
					IsDisturb:   1,
					LastReadSeq: 0,
				})
			}
			return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&userSessions).Error
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	// 2. 写入 Redis session:meta:{sessionID} Hash：存储 type 和 session_key 供路由逻辑使用
	metaKey := sessionMetaPrefix + sessionID
	_, err = c.cache.EvalCtx(ctx, `
		redis.call('HSET', KEYS[1], 'type', ARGV[1], 'session_key', ARGV[2])
		redis.call('EXPIRE', KEYS[1], ARGV[3])
		return 1
	`, []string{metaKey},
		fmt.Sprintf("%d", model.SessionTypeGroup),
		fmt.Sprintf("%d", groupID),
		fmt.Sprintf("%d", int(sessionMetaExpire.Seconds())),
	)
	if err != nil {
		logger.Errorf("[SessionDAO] write session:meta to redis failed (non-fatal): %v", err)
	}
	return sessionID, nil
}

// GetOrCreatePrivateSession 获取或创建单聊会话。
// 如果两个用户间已经有会话，返回已有的 SessionID。
// 否则使用传入的 sessionID（雪花 ID 字符串）创建新会话。
func (c *SessionDAO) GetOrCreatePrivateSession(ctx context.Context, sessionID string, userA, userB uint64) (string, error) {
	minUid, maxUid := userA, userB
	if minUid > maxUid {
		minUid, maxUid = maxUid, minUid
	}
	sessionKey := fmt.Sprintf("%d_%d", minUid, maxUid)

	// 1. 查缓存
	cachedSessionID, err := c.cache.GetCtx(ctx, sessionKey)
	if err == nil && cachedSessionID != "" {
		return cachedSessionID, nil
	}

	// 2. 查数据库 (直接使用 session_key 查询)
	var existingSession model.Session
	err = c.db.WithContext(ctx).
		Where("type = ? AND session_key = ?", model.SessionTypeSingle, sessionKey).
		Select("session_id").
		First(&existingSession).Error
	existingSessionID := existingSession.SessionID

	if err == nil && existingSessionID != "" {
		// 查到存在，写缓存
		_ = c.cache.SetexCtx(ctx, sessionKey, existingSessionID, int(sessionMetaExpire.Seconds()))
		return existingSessionID, nil
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return "", err
	}

	// 3. 不存在，创建新会话
	err = c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "session_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"update_time": time.Now()}),
		}).Create(&model.Session{
			SessionID:  sessionID,
			Type:       model.SessionTypeSingle,
			SessionKey: sessionKey,
		}).Error; err != nil {
			return err
		}

		userSessions := []*model.UserSession{
			{UserID: userA, SessionID: sessionID, IsTop: 1, IsDisturb: 1}, // 这里的IsTop和IsDisturb看情况，通常默认为1(非置顶，非免打扰，需要和你的数据库定义一致)
			{UserID: userB, SessionID: sessionID, IsTop: 1, IsDisturb: 1},
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&userSessions).Error
	})
	if err != nil {
		return "", err
	}

	// 创建成功，写入映射缓存
	_ = c.cache.SetexCtx(ctx, sessionKey, sessionID, int(sessionMetaExpire.Seconds()))

	// 写入 Redis session:meta
	metaKey := sessionMetaPrefix + sessionID
	_, metaErr := c.cache.EvalCtx(ctx, `
		redis.call('HSET', KEYS[1], 'type', ARGV[1], 'session_key', ARGV[2], 'user_a', ARGV[3], 'user_b', ARGV[4])
		redis.call('EXPIRE', KEYS[1], ARGV[5])
		return 1
	`, []string{metaKey},
		fmt.Sprintf("%d", model.SessionTypeSingle),
		sessionKey,
		fmt.Sprintf("%d", userA),
		fmt.Sprintf("%d", userB),
		fmt.Sprintf("%d", int(sessionMetaExpire.Seconds())),
	)
	if metaErr != nil {
		logger.Errorf("[SessionDAO] write session:meta to redis failed (non-fatal): %v", metaErr)
	}
	return sessionID, nil
}

// AddMemberToSession 将单个用户加入群聊会话
// 将 last_read_seq 设置为当前会话的 max_seq，避免新归小展示历史消息为未读
func (c *SessionDAO) AddMemberToSession(ctx context.Context, sessionID string, userID uint64) error {
	maxSeq := c.currentMaxSeq(ctx, sessionID)
	return c.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&model.UserSession{
		UserID:      userID,
		SessionID:   sessionID,
		IsTop:       1,
		IsDisturb:   1,
		LastReadSeq: maxSeq,
	}).Error
}
