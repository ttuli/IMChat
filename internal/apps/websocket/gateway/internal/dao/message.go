package dao

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"

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

// GetConversationMaxSeq 获取会话的最大消息序号
// 优先从 Redis 缓存读取，缓存未命中则查 MySQL 并回填缓存
// 如果 MySQL 中不存在，则自动创建会话
func (m *MessageDAO) GetConversationMaxSeq(ctx context.Context, conversationID string) (uint64, error) {
	cacheKey := convSeqPrefix + conversationID

	// 1. 先查 Redis
	val, err := m.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		seq, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr == nil {
			return seq, nil
		}
		// 缓存值异常，删除后走 DB
		m.redis.Del(ctx, cacheKey)
	} else if err != redis.Nil {
		// Redis 异常，降级查 DB
		return m.getMaxSeqFromDB(ctx, conversationID, cacheKey)
	}

	// 2. 缓存未命中，查 MySQL
	return m.getMaxSeqFromDB(ctx, conversationID, cacheKey)
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

// getMaxSeqFromDB 从 MySQL 查询 MaxSeq 并回填 Redis 缓存
func (m *MessageDAO) getMaxSeqFromDB(ctx context.Context, conversationID, cacheKey string) (uint64, error) {
	var conv model.Conversation
	err := m.db.WithContext(ctx).
		Select("max_seq").
		Where("conversation_id = ?", conversationID).
		First(&conv).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 会话不存在，自动创建

			newConv := model.Conversation{
				ConversationID: conversationID,
				Type:           int8(common.GetConversationType(conversationID)),
				MaxSeq:         0,
			}
			// 忽略唯一键冲突错误（可能并发创建）
			if createErr := m.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&newConv).Error; createErr != nil {
				return 0, fmt.Errorf("auto create conversation failed: %w", createErr)
			}
			return 0, nil
		}
		return 0, fmt.Errorf("query conversation max_seq failed: %w", err)
	}

	// 异步回填缓存，避免阻塞主流程
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.redis.Set(ctx, cacheKey, strconv.FormatUint(conv.MaxSeq, 10), convSeqExpire)
	}()

	return conv.MaxSeq, nil
}
