package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/Entity"
	"IM2/pkg/logger"
	"IM2/pkg/proto/util"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	convTimelinePrefix = "user:conv:timeline:"
	convInfoPrefix     = "conv:info:"
	convInfoExpire     = 7 * 24 * time.Hour
	defaultSegmentStep = uint64(100) // 号段步长：每次向 MySQL 预申请的 seq 数量
)

// ConversationDAO 会话数据访问对象 (MySQL + Redis缓存)
type ConversationDAO struct {
	db        *gorm.DB
	cache     *redisx.Client
	seqSyncer *SeqSyncer
}

// NewConversationDAO 创建会话DAO
func NewConversationDAO(dbSource string, redisConf redis.RedisConf) *ConversationDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	client, err := redisx.NewClient(redisConf)
	if err != nil {
		panic(err)
	}
	return &ConversationDAO{
		db:        db,
		cache:     client,
		seqSyncer: newSeqSyncer(db, client),
	}
}

// PushSeqUpdate 将完整的会话状态推送到 SeqSyncer，由后台批量刷 MySQL + 广播 UpdateSession。
// 非阻塞：channel 满时打日志丢弃，不影响主链路。
func (c *ConversationDAO) PushSeqUpdate(conversationID string, seq uint64, lastContent string, lastSender uint64, updateTime int64) {
	c.seqSyncer.Push(seqUpdate{
		conversationID: conversationID,
		seq:            seq,
		lastContent:    lastContent,
		lastSender:     lastSender,
		updateTime:     updateTime,
	})
}

// FindConversationsByIDs 批量查询会话。
// 策略：先通过 Lua 脚本批量读取 Redis conv:info:{id} Hash 中的快照字段，
//
//		Redis 没命中的会话再退化批量查询 MySQL。
func (c *ConversationDAO) FindConversationsByIDs(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	if len(conversationIDs) == 0 {
		return nil, nil
	}

	// 1. 批量读 Redis：从 conv:info:{id} Hash 中读取 actual_seq / last_content / last_sender
	// conv:info 是号段模式下已有的 Key，全局最新，不必新增决策 Key
	batchScript := `
		local results = {}
		for i = 1, #KEYS do
			local v = redis.call('HMGET', KEYS[i], 'actual_seq', 'last_content', 'last_sender')
			results[i] = v
		end
		return results
	`
	keys := make([]string, len(conversationIDs))
	for i, id := range conversationIDs {
		keys[i] = convInfoPrefix + id
	}
	luaRaw, err := c.cache.EvalCtx(ctx, batchScript, keys)

	result := make([]*model.Conversation, 0, len(conversationIDs))
	missingIDs := make([]string, 0)

	if err == nil {
		rows, ok := luaRaw.([]interface{})
		if ok && len(rows) == len(conversationIDs) {
			for i, row := range rows {
				fields, ok := row.([]interface{})
				if !ok || len(fields) != 3 || fields[0] == nil {
					missingIDs = append(missingIDs, conversationIDs[i])
					continue
				}
				actualSeq, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[0]), 10, 64)
				lastSender, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[2]), 10, 64)
				result = append(result, &model.Conversation{
					ConversationID: conversationIDs[i],
					ActualSeq:      actualSeq,
					LastContent:    fmt.Sprintf("%s", fields[1]),
					LastSender:     lastSender,
				})
			}
		} else {
			missingIDs = append(missingIDs, conversationIDs...)
		}
	} else {
		// Redis 全量失败，全部去查 DB
		missingIDs = append(missingIDs, conversationIDs...)
	}

	// 2. 对于 Redis 没命中的批量查 DB
	if len(missingIDs) > 0 {
		var dbConvs []*model.Conversation
		if err := c.db.WithContext(ctx).
			Where("conversation_id IN ?", missingIDs).
			Find(&dbConvs).Error; err != nil {
			return nil, err
		}
		result = append(result, dbConvs...)
	}
	return result, nil
}

// FindUserConversations 查询用户的会话列表 (按最后消息时间倒序)
func (c *ConversationDAO) FindUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	var userConvs []*model.UserConversation
	if err := c.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("update_time DESC").
		Find(&userConvs).Error; err != nil {
		return nil, err
	}
	return userConvs, nil
}

// ClearUnread 清零未读并更新已读游标
func (c *ConversationDAO) ClearUnread(ctx context.Context, userID uint64, conversationID string, lastReadMsgID, lastReadSeq uint64) error {
	return c.db.WithContext(ctx).Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(map[string]any{
			"unread_count":     0,
			"last_read_msg_id": lastReadMsgID,
			"last_read_seq":    lastReadSeq,
		}).Error
}

// UpdateUserConversation 更新用户会话设置 (置顶/免打扰/静音)
func (c *ConversationDAO) UpdateUserConversation(ctx context.Context, userID uint64, conversationID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}

	// 构造完整的主键和默认值，用于不存在时的 Create
	userConv := model.UserConversation{
		UserID:         userID,
		ConversationID: conversationID,
		IsTop:          1, // 默认 1
		IsDisturb:      1, // 默认 1
	}

	// 将需要更新的字段也覆盖到结构体上，保证初次 Create 时的值是设置后的业务值
	if v, ok := updates["is_top"].(int32); ok {
		userConv.IsTop = int8(v)
	}
	if v, ok := updates["is_disturb"].(int32); ok {
		userConv.IsDisturb = int8(v)
	}

	// 执行 Upsert：冲突时按 updates 里的特定字段执行更新
	return c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.Assignments(updates),
	}).Create(&userConv).Error
}

// InsertUserConversation 插入新的用户会话
func (c *ConversationDAO) InsertUserConversation(ctx context.Context, userId uint64, conversationId string, convType int8) error {
	// 从 Redis hash 的 seg_ceiling 字段读取当前已分配的最大 seq，作为新成员的已读起点。
	// 若缓存未命中则查 MySQL max_seq。
	maxSeq := c.currentMaxSeq(ctx, conversationId)

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Conversation 表 (存在则只更新 update_time)
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "conversation_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Conversation{
			ConversationID: conversationId,
			Type:           convType,
		}).Error
		if err != nil {
			return err
		}

		// 2. 插入 UserConversation 记录，忽略冲突
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.UserConversation{
			UserID:         userId,
			ConversationID: conversationId,
			IsTop:          1,
			IsDisturb:      1,
			LastReadSeq:    maxSeq,
		}).Error
	})
}

// Transaction 提供开启事务的能力
func (c *ConversationDAO) Transaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return c.db.WithContext(ctx).Transaction(fc)
}

// BatchInsertUserConversations 批量插入用户会话记录
func (c *ConversationDAO) BatchInsertUserConversations(ctx context.Context, userConvs []*model.UserConversation, convType int8) error {
	if len(userConvs) == 0 {
		return nil
	}

	conversationId := userConvs[0].ConversationID
	maxSeq := c.currentMaxSeq(ctx, conversationId)

	for _, conv := range userConvs {
		conv.LastReadSeq = maxSeq
	}

	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 同步插入或更新 Conversation 表
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "conversation_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"update_time": time.Now(),
			}),
		}).Create(&model.Conversation{
			ConversationID: conversationId,
			Type:           convType,
		}).Error
		if err != nil {
			return err
		}

		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&userConvs).Error
	})
}

// GetActiveConversationIDs 获取活跃的会话 ID 列表，按时间戳过滤大于 sinceTimestamp 的记录
func (c *ConversationDAO) GetActiveConversationIDs(ctx context.Context, userID uint64, sinceTimestamp int64) ([]string, error) {
	snaps, err := c.getActiveConvSnapshots(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(snaps))
	for _, s := range snaps {
		ids = append(ids, s.ConvID)
	}
	return ids, nil
}

// getActiveConvSnapshots 获取在 sinceTimestamp 后有更新的会话快照列表。
//
// 流程：
//  1. 从 ZSET user:conv:timeline:{uid} 读取活跃会话 ID 列表（member = convID，score = updateTime）
//  2. 批量 HMGET Redis conv:info:{id} 获取快照字段（actual_seq / last_content / last_sender）
//  3. Redis 没命中的删退化批量查询 MySQL
func (c *ConversationDAO) getActiveConvSnapshots(ctx context.Context, userID uint64, sinceTimestamp int64) ([]ConvSnapshot, error) {
	key := fmt.Sprintf("%s%d", convTimelinePrefix, userID)

	// Step 1: 从 ZSET 获取会话 ID
	pairs, err := c.cache.ZRangeByScoreWithScoresCtx(ctx, key, sinceTimestamp+1, time.Now().UnixMilli()+100000)
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		logger.Errorf("get updated conversations failed for user %d: %v", userID, err)
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}

	// 降序排列（最新的排在前面）
	convIDs := make([]string, 0, len(pairs))
	for i := len(pairs) - 1; i >= 0; i-- {
		convIDs = append(convIDs, pairs[i].Key)
	}

	// Step 2: 批量 HMGET conv:info Hash
	batchScript := `
		local results = {}
		for i = 1, #KEYS do
			results[i] = redis.call('HMGET', KEYS[i], 'actual_seq', 'last_content', 'last_sender')
		end
		return results
	`
	luaKeys := make([]string, len(convIDs))
	for i, id := range convIDs {
		luaKeys[i] = convInfoPrefix + id
	}
	luaRaw, luaErr := c.cache.EvalCtx(ctx, batchScript, luaKeys)

	result := make([]ConvSnapshot, 0, len(convIDs))
	missingIDs := make([]string, 0)

	if luaErr == nil {
		rows, ok := luaRaw.([]interface{})
		if ok && len(rows) == len(convIDs) {
			for i, row := range rows {
				fields, ok := row.([]interface{})
				if !ok || len(fields) != 3 || fields[0] == nil {
					missingIDs = append(missingIDs, convIDs[i])
					continue
				}
				actualSeq, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[0]), 10, 64)
				lastSender, _ := strconv.ParseUint(fmt.Sprintf("%s", fields[2]), 10, 64)
				result = append(result, ConvSnapshot{
					ConvID:      convIDs[i],
					Seq:         actualSeq,
					LastContent: fmt.Sprintf("%s", fields[1]),
					LastSender:  lastSender,
				})
			}
		} else {
			missingIDs = append(missingIDs, convIDs...)
		}
	} else {
		missingIDs = append(missingIDs, convIDs...)
	}

	// Step 3: DB 退化查询
	if len(missingIDs) > 0 {
		var dbConvs []*model.Conversation
		if dbErr := c.db.WithContext(ctx).
			Where("conversation_id IN ?", missingIDs).
			Find(&dbConvs).Error; dbErr != nil {
			return nil, dbErr
		}
		for _, conv := range dbConvs {
			result = append(result, ConvSnapshot{
				ConvID:      conv.ConversationID,
				Seq:         conv.ActualSeq,
				LastContent: conv.LastContent,
				LastSender:  conv.LastSender,
			})
		}
	}
	return result, nil
}

// IncrSeq 以号段模式递增会话的 seq。
//
// 设计：
//   - Redis Hash（conv:info:{id}）存两个字段：
//     cur_seq    当前已分配到的最大 seq（各实例共享的原子计数器）
//     seg_ceiling 当前号段的上限（对应 MySQL max_seq 的已预申请值）
//   - 快路径：cur_seq <= seg_ceiling，直接 HINCRBY 返回，无 DB 访问。
//   - 慢路径：cur_seq > seg_ceiling 或 Redis 冷启动，
//     去 MySQL 原子扩容 max_seq += step，
//     用 Lua 将 seg_ceiling 更新至新上限并保证 cur_seq 不低于新号段起点，
//     从而彻底消除 Redis 宕机/重启导致的 seq 回退。
func (c *ConversationDAO) IncrSeq(ctx context.Context, conversationID string, lastContent string, lastSender uint64) (uint64, error) {
	cacheKey := convInfoPrefix + conversationID
	expireSecs := strconv.Itoa(int(convInfoExpire.Seconds()))
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
	newCeiling, err := c.allocSegment(ctx, conversationID, cacheKey, expireSecs)
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
	return c.IncrSeq(ctx, conversationID, lastContent, lastSender)
}

// allocSegment 原子地向 MySQL 申请下一个号段，并更新 Redis 中的 seg_ceiling 和 cur_seq。
//
// 参数 curSeq 是触发扩容的那次 HINCRBY 拿到的值，用于判断 Lua 里是否需要重置 cur_seq。
// 返回新的 seg_ceiling（即 MySQL 最新的 max_seq）。
func (c *ConversationDAO) allocSegment(
	ctx context.Context,
	conversationID, cacheKey string,
	expireSecs string,
) (uint64, error) {
	// 1. MySQL 原子申请号段：INSERT 不存在则创建，存在则 max_seq += step。
	//    使用 ON DUPLICATE KEY UPDATE 保证即使会话不存在也能自动初始化。
	now := time.Now()
	step := defaultSegmentStep

	err := c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "conversation_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"max_seq": gorm.Expr("max_seq + ?", step),
		}),
	}).Create(&model.Conversation{
		ConversationID: conversationID,
		Type:           int8(util.GetConversationType(conversationID)),
		MaxSeq:         step, // 初次创建时 max_seq = step
		CreateTime:     now,
		UpdateTime:     now,
	}).Error
	if err != nil {
		return 0, fmt.Errorf("allocSegment: upsert conversation failed: %w", err)
	}

	// 2. 读取更新后的 max_seq（即新的 seg_ceiling）。
	var conv model.Conversation
	if err := c.db.WithContext(ctx).
		Select("max_seq").
		Where("conversation_id = ?", conversationID).
		First(&conv).Error; err != nil {
		return 0, fmt.Errorf("allocSegment: read max_seq failed: %w", err)
	}
	newCeiling := conv.MaxSeq
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
// 优先读 Redis Hash 的 seg_ceiling，降级读 MySQL max_seq。
// 用于新成员加入时初始化 LastReadSeq。
func (c *ConversationDAO) currentMaxSeq(ctx context.Context, conversationID string) uint64 {
	cacheKey := convInfoPrefix + conversationID
	if val, err := c.cache.HGetCtx(ctx, cacheKey, "seg_ceiling"); err == nil && val != "" {
		if seq, err := strconv.ParseUint(val, 10, 64); err == nil {
			return seq
		}
	}
	// 降级读 MySQL
	var conv model.Conversation
	if err := c.db.WithContext(ctx).Select("max_seq").Where("conversation_id = ?", conversationID).First(&conv).Error; err == nil {
		return conv.MaxSeq
	}
	return 0
}
