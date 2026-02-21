package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"

	"github.com/redis/go-redis/v9"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// Redis key 前缀: conv:seq:{conversationID} -> MaxSeq
	convSeqPrefix = "conv:seq:"
	// 缓存过期时间
	convSeqExpire = 30 * time.Minute
)

// MessageDAO 消息数据访问对象 (Gateway 专用)
// 主要用于获取会话的 MaxSeq，配合 Redis 缓存
type MessageDAO struct {
	db    *gorm.DB
	redis *redis.Client
}

// NewMessageDAO 创建 MessageDAO
func NewMessageDAO(mysqlDSN string, redisConf zeroredis.RedisConf) *MessageDAO {
	db, err := gorm.Open(mysql.Open(mysqlDSN), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	rds := redis.NewClient(&redis.Options{
		Addr:     redisConf.Host,
		Password: redisConf.Pass,
	})

	return &MessageDAO{
		db:    db,
		redis: rds,
	}
}

// IncrConversationSeq 原子递增会话消息序号，返回新的 seq
// 1. 尝试 Redis INCR（缓存命中时直接原子递增）
// 2. 缓存未命中则从 MySQL 加载 + 递增，回填 Redis
// 3. 异步更新 MySQL 的 max_seq
func (m *MessageDAO) IncrConversationSeq(ctx context.Context, conversationID string) (uint64, error) {
	cacheKey := convSeqPrefix + conversationID

	// 1. 尝试直接 INCR（key 存在时原子递增）
	newSeq, err := m.redis.Incr(ctx, cacheKey).Result()
	if err == nil && newSeq > 1 {
		// key 已存在且递增成功（newSeq > 1 说明之前已经有值）
		m.redis.Expire(ctx, cacheKey, convSeqExpire)
		seq := uint64(newSeq)
		// 异步同步到 MySQL
		go m.syncSeqToDB(conversationID, seq)
		return seq, nil
	}

	// newSeq == 1 说明 key 之前不存在（INCR 对不存在的 key 会初始化为 0 再 +1）
	// 需要先从 DB 加载正确的值
	if err == nil && newSeq == 1 {
		// 先删除这个错误的 key，后面会用 DB 值重新设置
		m.redis.Del(ctx, cacheKey)
	}

	// 2. 从 DB 加载并递增
	return m.incrSeqFromDB(ctx, conversationID, cacheKey)
}

// BatchGetConversationMaxSeq 批量获取多个会话的 MaxSeq
func (m *MessageDAO) BatchGetConversationMaxSeq(ctx context.Context, conversationIDs []string) (map[string]uint64, error) {
	if len(conversationIDs) == 0 {
		return map[string]uint64{}, nil
	}

	result := make(map[string]uint64, len(conversationIDs))

	// 1. 批量查 Redis (pipeline)
	keys := make([]string, len(conversationIDs))
	for i, id := range conversationIDs {
		keys[i] = convSeqPrefix + id
	}

	cmds, err := m.redis.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			pipe.Get(ctx, key)
		}
		return nil
	})

	missedIDs := make([]string, 0)
	if err != nil && err != redis.Nil {
		// pipeline 整体失败，全部走 DB
		missedIDs = conversationIDs
	} else {
		for i, cmd := range cmds {
			val, cmdErr := cmd.(*redis.StringCmd).Result()
			if cmdErr != nil {
				missedIDs = append(missedIDs, conversationIDs[i])
				continue
			}
			seq, parseErr := strconv.ParseUint(val, 10, 64)
			if parseErr != nil {
				missedIDs = append(missedIDs, conversationIDs[i])
				continue
			}
			result[conversationIDs[i]] = seq
		}
	}

	// 2. 批量查 MySQL 并回填缓存
	if len(missedIDs) > 0 {
		var convs []model.Conversation
		if dbErr := m.db.WithContext(ctx).
			Select("conversation_id, max_seq").
			Where("conversation_id IN ?", missedIDs).
			Find(&convs).Error; dbErr != nil {
			return nil, fmt.Errorf("batch query conversation max_seq failed: %w", dbErr)
		}

		// 用 pipeline 批量回填缓存
		pipe := m.redis.Pipeline()
		for _, conv := range convs {
			result[conv.ConversationID] = conv.MaxSeq
			pipe.Set(ctx, convSeqPrefix+conv.ConversationID, strconv.FormatUint(conv.MaxSeq, 10), convSeqExpire)
		}
		if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
			// 回填失败不影响返回结果，仅记录
			logger.Errorf("batch backfill conversation max_seq failed: %v", pipeErr)
		}

		// 不存在的会话 ID 默认 seq=0
		for _, id := range missedIDs {
			if _, ok := result[id]; !ok {
				result[id] = 0
			}
		}
	}

	return result, nil
}

// incrSeqFromDB 从 MySQL 递增 max_seq 并回填 Redis 缓存
func (m *MessageDAO) incrSeqFromDB(ctx context.Context, conversationID, cacheKey string) (uint64, error) {
	var conv model.Conversation
	err := m.db.WithContext(ctx).
		Select("max_seq").
		Where("conversation_id = ?", conversationID).
		First(&conv).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 会话不存在，自动创建，max_seq 初始化为 1
			newConv := model.Conversation{
				ConversationID: conversationID,
				Type:           int8(common.GetConversationType(conversationID)),
				MaxSeq:         1,
			}
			if createErr := m.db.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "conversation_id"}},
				DoUpdates: clause.Assignments(map[string]interface{}{"max_seq": gorm.Expr("max_seq + 1")}),
			}).Create(&newConv).Error; createErr != nil {
				return 0, fmt.Errorf("auto create conversation failed: %w", createErr)
			}
			// 回填 Redis
			m.redis.Set(ctx, cacheKey, strconv.FormatUint(1, 10), convSeqExpire)
			return 1, nil
		}
		return 0, fmt.Errorf("query conversation max_seq failed: %w", err)
	}

	// 递增 DB
	newSeq := conv.MaxSeq + 1
	if updateErr := m.db.WithContext(ctx).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Update("max_seq", newSeq).Error; updateErr != nil {
		return 0, fmt.Errorf("update conversation max_seq failed: %w", updateErr)
	}

	// 回填 Redis
	m.redis.Set(ctx, cacheKey, strconv.FormatUint(newSeq, 10), convSeqExpire)

	return newSeq, nil
}

// syncSeqToDB 异步将 Redis 中的 seq 同步到 MySQL
func (m *MessageDAO) syncSeqToDB(conversationID string, seq uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.db.WithContext(ctx).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Update("max_seq", seq)
}
