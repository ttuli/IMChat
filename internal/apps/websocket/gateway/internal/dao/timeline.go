package dao

import (
	"IM2/pkg/logger"
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// 会话时间线 Prefix
const convTimelinePrefix = "user:conv:timeline:"

// UpdateUsersConversationTimeline 批量更新用户会话时间线
func (c *ConversationDAO) UpdateUsersConversationTimeline(ctx context.Context, userIDs []uint64, convID string, updateTime int64) error {
	if len(userIDs) == 0 {
		return nil
	}

	err := c.redis.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		for _, uid := range userIDs {
			key := fmt.Sprintf("%s%d", convTimelinePrefix, uid)
			pipe.ZAdd(ctx, key, redis.Z{
				Score:  float64(updateTime),
				Member: convID,
			})
			// 限制列表长度，保留最近 500 个活跃会话
			pipe.ZRemRangeByRank(ctx, key, 0, -501)
		}
		return nil
	})

	if err != nil {
		logger.Errorf("update user conversation timeline failed: %v", err)
		return err
	}

	return nil
}

// GetUpdatedConversations 获取用户有更新的会话 ID 列表
func (c *ConversationDAO) GetUpdatedConversations(ctx context.Context, userID uint64, lastSyncTime int64) ([]string, error) {
	key := fmt.Sprintf("%s%d", convTimelinePrefix, userID)

	// go-zero 用 ZrevrangebyscorebyscoreWithScoresCtx，但这里我们升序拿就行。
	// 但 go-zero 没提供直接类似 "(x" 的语法给 zrangebyscoreCtx
	// 这里可以用原生的 Query 或者转换一下
	
	pairs, err := c.redis.ZrangebyscoreWithScoresCtx(ctx, key, lastSyncTime+1, -1) // -1 意味着无穷大
	if err != nil {
		logger.Errorf("get updated conversations failed for user %d: %v", userID, err)
		return nil, err
	}

	var conversationIDs []string
	for _, p := range pairs {
		conversationIDs = append(conversationIDs, p.Key)
	}

	return conversationIDs, nil
}
