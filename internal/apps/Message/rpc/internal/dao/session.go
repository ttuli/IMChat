package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/model"
	"IM2/pkg/logger"
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
)

// SessionDAO 会话数据访问对象 (MySQL + Redis缓存)
type SessionDAO struct {
	db          *gorm.DB
	cache       *redisx.Client
	seqSyncer   *SeqSyncer
	keyCache    *sessionKeyCache // sessionKey → sessionID 热路径缓存
	ensureGuard *ttlGuard        // user_session 补偿写入的进程内防重
}

// NewSessionDAO 创建会话DAO
func NewSessionDAO(dbSource string, redisConf redis.RedisConf) *SessionDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	if err := db.AutoMigrate(&model.Session{}, &model.UserSession{}); err != nil {
		panic(err)
	}


	client, err := redisx.NewClient(redisConf)
	if err != nil {
		panic(err)
	}
	return &SessionDAO{
		db:          db,
		cache:       client,
		seqSyncer:   newSeqSyncer(db, client),
		keyCache:    newSessionKeyCache(65536),
		ensureGuard: newTTLGuard(30*time.Minute, 65536),
	}
}

// SetGroupMemberSource 注入群成员来源，供 SeqSyncer 更新群会话时间线使用
func (c *SessionDAO) SetGroupMemberSource(src GroupMemberSource) {
	c.seqSyncer.SetMemberSource(src)
}

// ResolveSessionIDByKey 消息消费热路径专用：sessionKey → sessionID 解析。
// 命中进程内 LRU 时零 DB 访问；未命中时走 FindOrCreateBySessionKey 并回填缓存。
// key→ID 映射不可变，缓存无一致性问题。
func (c *SessionDAO) ResolveSessionIDByKey(ctx context.Context, newSessionID string, sessionKey string, sessionType int8) (string, error) {
	if id, ok := c.keyCache.get(sessionKey); ok {
		return id, nil
	}
	session, _, err := c.FindOrCreateBySessionKey(ctx, newSessionID, sessionKey, sessionType)
	if err != nil {
		return "", err
	}
	c.keyCache.put(sessionKey, session.SessionID)
	return session.SessionID, nil
}

// PushSeqUpdate 将完整的会话状态推送到 SeqSyncer，由后台批量刷 MySQL + 广播 UpdateSession。
// 通常非阻塞；channel 打满且短暂等待无效时降级为同步直写（阻塞调用方但不丢事件）。
// sessionKey 随快照写入 Redis，读路径按 session_id 可命中完整会话信息。
// isGroup/target 描述会话形态与消息目标（群聊=群ID，单聊=接收方），用于时间线扇出。
func (c *SessionDAO) PushSeqUpdate(sessionID, sessionKey string, seq uint64, lastContent string, lastSender uint64, updateTime int64, isGroup bool, target uint64) {
	c.seqSyncer.Push(seqUpdate{
		sessionID:   sessionID,
		sessionKey:  sessionKey,
		seq:         seq,
		lastContent: lastContent,
		lastSender:  lastSender,
		updateTime:  updateTime,
		isGroup:     isGroup,
		target:      target,
	})
}

// sessionInfoBatchScript 批量读取 session:info:{id} Hash 的完整快照字段
// （由 SeqSyncer 随消息异步维护，可整条命中不回源 MySQL）。
const sessionInfoBatchScript = `
	local results = {}
	for i = 1, #KEYS do
		results[i] = redis.call('HMGET', KEYS[i], 'actual_seq', 'last_content', 'last_sender', 'session_key', 'type', 'update_time')
	end
	return results
`

// FindSessionsByIDs 批量查询会话。
// 策略：先通过 Lua 脚本批量读取 Redis session:info:{id} Hash 的完整快照
// （session_key / type / actual_seq / last_content / last_sender / update_time），
// 未命中（含旧格式条目缺 session_key）的会话再批量查询 MySQL 兜底。
func (c *SessionDAO) FindSessionsByIDs(ctx context.Context, sessionIDs []string) ([]*model.Session, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	keys := make([]string, len(sessionIDs))
	for i, id := range sessionIDs {
		keys[i] = sessionInfoPrefix + id
	}
	luaRaw, err := c.cache.EvalCtx(ctx, sessionInfoBatchScript, keys)

	result := make([]*model.Session, 0, len(sessionIDs))
	missingIDs := make([]string, 0)

	if err == nil {
		rows, ok := luaRaw.([]interface{})
		if ok && len(rows) == len(sessionIDs) {
			for i, row := range rows {
				session := parseSessionSnapshot(sessionIDs[i], row)
				if session == nil {
					missingIDs = append(missingIDs, sessionIDs[i])
					continue
				}
				result = append(result, session)
			}
		} else {
			missingIDs = append(missingIDs, sessionIDs...)
		}
	} else {
		// Redis 全量失败，全部去查 DB
		missingIDs = append(missingIDs, sessionIDs...)
	}

	// Redis 没命中的批量查 DB
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

// parseSessionSnapshot 将 Lua HMGET 返回的一行快照解析为 Session。
// actual_seq 或 session_key 缺失（键不存在 / 旧格式条目）视为未命中，返回 nil。
func parseSessionSnapshot(sessionID string, row interface{}) *model.Session {
	fields, ok := row.([]interface{})
	if !ok || len(fields) != 6 || fields[0] == nil || fields[3] == nil {
		return nil
	}
	actualSeq, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[0]), 10, 64)
	lastSender, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[2]), 10, 64)
	sessionType, _ := strconv.ParseInt(fmt.Sprintf("%s", fields[4]), 10, 8)
	updateTime, _ := strconv.ParseInt(fmt.Sprintf("%s", fields[5]), 10, 64)
	return &model.Session{
		SessionID:   sessionID,
		Type:        int8(sessionType),
		SessionKey:  fmt.Sprintf("%s", fields[3]),
		ActualSeq:   actualSeq,
		LastContent: fmt.Sprintf("%s", fields[1]),
		LastSender:  lastSender,
		UpdateTime:  time.UnixMilli(updateTime),
	}
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

// MarkSessionRead 前进已读游标。
// 行不存在时创建（存量数据补偿）；存在时用 GREATEST 保证单调，乱序/并发上报不会回退游标。
func (c *SessionDAO) MarkSessionRead(ctx context.Context, userID uint64, sessionID string, readSeq uint64) error {
	return c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "session_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"last_read_seq": gorm.Expr("GREATEST(last_read_seq, VALUES(last_read_seq))"),
		}),
	}).Create(&model.UserSession{
		UserID:      userID,
		SessionID:   sessionID,
		IsTop:       1,
		IsDisturb:   1,
		LastReadSeq: readSeq,
	}).Error
}

// EnsureUserSessions 确保一批用户在指定会话下的 user_session 行存在（幂等，冲突忽略）。
// 供消息消费热路径做存量补偿：进程内 ttlGuard 防重，同一会话至多每个周期执行一次。
// 新行的 last_read_seq 取会话当前 actual_seq：补偿时点之前的消息视为已读，之后正常计未读。
func (c *SessionDAO) EnsureUserSessions(ctx context.Context, sessionID string, userIDs []uint64) error {
	if len(userIDs) == 0 || !c.ensureGuard.tryAcquire(sessionID) {
		return nil
	}

	maxSeq := c.currentMaxSeq(ctx, sessionID)
	rows := make([]*model.UserSession, 0, len(userIDs))
	for _, uid := range userIDs {
		if uid == 0 {
			continue
		}
		rows = append(rows, &model.UserSession{
			UserID:      uid,
			SessionID:   sessionID,
			IsTop:       1,
			IsDisturb:   1,
			LastReadSeq: maxSeq,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(&rows, 500).Error; err != nil {
		// 失败释放防重标记，允许后续消息重试补偿
		c.ensureGuard.release(sessionID)
		return err
	}
	return nil
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
	// 读取当前会话最后一条消息的 seq，作为新成员的已读起点。
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

// GetActiveSessions 获取在 sinceTimestamp 后有更新的会话完整信息列表。
//
// 流程：
//  1. 从 ZSET user:session:timeline:{uid} 读取活跃会话 ID（member = sessionID，
//     score = updateTime），按更新时间降序
//  2. 复用 FindSessionsByIDs 批量取会话详情：Redis 完整快照优先命中，
//     未命中的批量查询 MySQL 兜底
func (c *SessionDAO) GetActiveSessions(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Session, error) {
	key := fmt.Sprintf("%s%d", sessionTimelinePrefix, userID)

	// Step 1: 从 ZSET 获取活跃会话 ID
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

	// Step 2: 批量取会话详情（Redis 快照 + MySQL 兜底）
	return c.FindSessionsByIDs(ctx, sessionIDs)
}

// currentMaxSeq 返回当前会话已分配的最大 seq（Lamport 语义下即最后一条消息的 seq）。
// 优先读 Redis Hash 的 actual_seq 字段（由 SeqSyncer 异步刷入），降级读 MySQL actual_seq。
// 用于新成员加入时初始化 LastReadSeq。
func (c *SessionDAO) currentMaxSeq(ctx context.Context, sessionID string) uint64 {
	cacheKey := sessionInfoPrefix + sessionID
	if val, err := c.cache.HGetCtx(ctx, cacheKey, "actual_seq"); err == nil && val != "" {
		if seq, err := strconv.ParseUint(val, 10, 64); err == nil {
			return seq
		}
	}
	// 降级读 MySQL
	var session model.Session
	if err := c.db.WithContext(ctx).Select("actual_seq").Where("session_id = ?", sessionID).First(&session).Error; err == nil {
		return session.ActualSeq
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
