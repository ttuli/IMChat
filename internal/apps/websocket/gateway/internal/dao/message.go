package dao

import (
	"context"
	"fmt"
	"strconv"

	"IM2/internal/model"
	"IM2/pkg/logger"

	"github.com/redis/go-redis/v9"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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
