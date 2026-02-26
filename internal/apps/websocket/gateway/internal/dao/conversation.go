package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"IM2/internal/model"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	// Redis Hash Key: conv:info:{conversationID}
	convInfoPrefix   = "conv:info:"
	convInfoExpire   = 7 * 24 * time.Hour
	fieldLastContent = "last_content"
)

// syncItem 代表一条需要刷入 DB 的会话更新记录
type syncItem struct {
	ConversationID string
	LastContentStr string
}

// ConversationDAO 会话数据访问对象
type ConversationDAO struct {
	db      *gorm.DB
	redis   *redis.Client
	log     logx.Logger
	syncCh  chan *syncItem
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewConversationDAO 创建 ConversationDAO，引入 Redis 和 Channel
func NewConversationDAO(mysqlDSN string, redisConf zeroredis.RedisConf) *ConversationDAO {
	db, err := gorm.Open(mysql.Open(mysqlDSN), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	rds := redis.NewClient(&redis.Options{
		Addr:     redisConf.Host,
		Password: redisConf.Pass,
	})

	dao := &ConversationDAO{
		db:    db,
		redis: rds,
		log:   logx.WithContext(context.Background()),
		// 缓冲大小可根据业务并发量调整，这里给个1000作为例子
		syncCh:  make(chan *syncItem, 1000),
		closeCh: make(chan struct{}),
	}

	// 初始化并启动后台刷盘协程
	// go dao.startSyncWorker()

	return dao
}

// LastMessageContent 存储会话的最后一条消息内容
type LastMessageContent struct {
	Sender  uint64 `json:"sender"`
	Content string `json:"content"`
}

// UpdateLastContent 更新会话的最后一条消息 (写 Redis + 丢进 Channel 交给后台异步刷盘)
func (c *ConversationDAO) UpdateLastContent(ctx context.Context, conversationID string, sender uint64, content string) error {
	lastMsg := LastMessageContent{
		Sender:  sender,
		Content: content,
	}

	lastMsgBytes, err := json.Marshal(lastMsg)
	if err != nil {
		return fmt.Errorf("marshal last content failed: %w", err)
	}
	contentStr := string(lastMsgBytes)
	cacheKey := convInfoPrefix + conversationID

	// 1. 同步写 Redis (保证客户端读取时实时最新)
	pipe := c.redis.Pipeline()
	pipe.HSet(ctx, cacheKey, fieldLastContent, contentStr)
	pipe.Expire(ctx, cacheKey, convInfoExpire)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis update conversation last_content failed: %w", err)
	}

	// 2. 将数据交给 Channel 进行异步合并写库
	select {
	case c.syncCh <- &syncItem{
		ConversationID: conversationID,
		LastContentStr: contentStr,
	}:
		// 成功放入 channel
	default:
		// 通道满了，通常说明 DB 写入非常慢导致积压
		// 这个时候你可以选择扔掉（只要 Redis 有数据且不丢就行），或者在这里阻塞/写日志报警
		c.log.Errorf("DB sync channel is full for conversationID: %s, dropping DB update request", conversationID)
	}

	return nil
}

// startSyncWorker 后台持续运行，消费 channel 中的数据写 MySQL
func (c *ConversationDAO) StartSyncWorker() {
	c.wg.Add(1)
	defer c.wg.Done()

	// 使用 map 去重合并
	batchMap := make(map[string]string)

	for {
		select {
		case item := <-c.syncCh:
			// 累积到 map
			batchMap[item.ConversationID] = item.LastContentStr

			// 如果瞬时积压过多（如满 100 条了），直接触发一次写入
			if len(batchMap) >= 100 {
				c.flushToDB(batchMap)
				// 清空 map 以便下一轮收集
				batchMap = make(map[string]string)
			}

		case <-c.closeCh:
			// DAO 被关闭时退出
			c.log.Info("sync worker is closing, flushing remaining items...")
			if len(batchMap) > 0 {
				c.flushToDB(batchMap)
			}
			return
		}
	}
}

// flushToDB 将 map 里积累的数据批量执行 MySQL 更新
func (c *ConversationDAO) flushToDB(items map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// GORM 暂不支持原生的一维 Map 批量 Update 不同列的值
	// 简单的处理方式是启事务 + for 循环，或者依靠 database/sql 直接拼写原生 SQL CASE WHEN 语句
	// 在这里为了简单且性能比单个 Goroutine 去 Update 要好，采用事务方式包起来：
	err := c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for convID, contentStr := range items {
			res := tx.Model(&model.Conversation{}).
				Where("conversation_id = ?", convID).
				Update("last_content", contentStr)
			if res.Error != nil {
				c.log.Errorf("failed to bulk update db last_content for conv %s: %v", convID, res.Error)
			}
		}
		return nil // 遇到单条出错也不回滚整个事务
	})

	if err != nil {
		c.log.Errorf("flushToDB transaction failed: %v", err)
	}
}

// CloseDAO 可在主程序退出时调用，通知后台协程退出并刷盘
func (c *ConversationDAO) CloseDAO() {
	close(c.closeCh)
	c.wg.Wait()
}

// GetLastContent 获取会话的最后一条消息 (先查 Redis，Miss 则查 MySQL 并回填)
func (c *ConversationDAO) GetLastContent(ctx context.Context, conversationID string) (*LastMessageContent, error) {
	cacheKey := convInfoPrefix + conversationID

	// 1. 查 Redis
	val, err := c.redis.HGet(ctx, cacheKey, fieldLastContent).Result()
	if err == nil && val != "" {
		// 缓存命中
		return c.parseContent([]byte(val))
	}

	if err != nil && err != redis.Nil {
		c.log.Errorf("redis hget error: %v", err)
	}

	// 2. 缓存未命中，查 MySQL
	var conv model.Conversation
	if err := c.db.WithContext(ctx).
		Select("last_content").
		Where("conversation_id = ?", conversationID).
		First(&conv).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("query db last_content failed: %w", err)
	}

	if conv.LastContent == "" {
		return nil, nil // 无内容
	}

	// 3. MySQL 查到后，回填 Redis
	c.redis.HSet(ctx, cacheKey, fieldLastContent, conv.LastContent)
	c.redis.Expire(ctx, cacheKey, convInfoExpire)

	return c.parseContent([]byte(conv.LastContent))
}

// BatchGetLastContent 批量获取会话的最后一条消息
func (c *ConversationDAO) BatchGetLastContent(ctx context.Context, conversationIDs []string) (map[string]*LastMessageContent, error) {
	if len(conversationIDs) == 0 {
		return make(map[string]*LastMessageContent), nil
	}

	result := make(map[string]*LastMessageContent, len(conversationIDs))
	missedIDs := make([]string, 0)

	// 1. 走 Pipeline 批量查 Redis
	cmds, err := c.redis.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, id := range conversationIDs {
			pipe.HGet(ctx, convInfoPrefix+id, fieldLastContent)
		}
		return nil
	})

	if err != nil && err != redis.Nil {
		missedIDs = conversationIDs
	} else {
		for i, cmd := range cmds {
			convID := conversationIDs[i]
			val, cmdErr := cmd.(*redis.StringCmd).Result()
			if cmdErr == redis.Nil || val == "" {
				missedIDs = append(missedIDs, convID)
			} else if cmdErr == nil {
				if parsed, pErr := c.parseContent([]byte(val)); pErr == nil {
					result[convID] = parsed
				}
			} else {
				missedIDs = append(missedIDs, convID)
			}
		}
	}

	// 2. 全部命中
	if len(missedIDs) == 0 {
		return result, nil
	}

	// 3. 将 Miss 的 ID 批量查 MySQL
	var convs []model.Conversation
	if err := c.db.WithContext(ctx).
		Select("conversation_id, last_content").
		Where("conversation_id IN ?", missedIDs).
		Find(&convs).Error; err != nil {
		return nil, fmt.Errorf("batch query db last_content failed: %w", err)
	}

	// 4. 解析结果并用 Pipeline 批量回填 Redis
	if len(convs) > 0 {
		pipe := c.redis.Pipeline()
		for _, conv := range convs {
			if conv.LastContent != "" {
				if parsed, pErr := c.parseContent([]byte(conv.LastContent)); pErr == nil {
					result[conv.ConversationID] = parsed
				}
				cacheKey := convInfoPrefix + conv.ConversationID
				pipe.HSet(ctx, cacheKey, fieldLastContent, conv.LastContent)
				pipe.Expire(ctx, cacheKey, convInfoExpire)
			}
		}
		pipe.Exec(ctx)
	}

	return result, nil
}

// 辅助方法：解析 JSON 内容
func (c *ConversationDAO) parseContent(data []byte) (*LastMessageContent, error) {
	var lastMsg LastMessageContent
	if err := json.Unmarshal(data, &lastMsg); err != nil {
		return nil, err
	}
	return &lastMsg, nil
}
