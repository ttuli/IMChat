package dao

import (
	"context"
	"fmt"
	"sync"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"

	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// Redis Hash Key: conv:info:{conversationID}
	convInfoPrefix = "conv:info:"
	convInfoExpire = 7 * 24 * time.Hour
	convSeqPrefix  = "conv:seq:"
	convSeqExpire  = 24 * time.Hour
)

// syncItem 代表一条需要刷入 DB 的会话更新记录
type syncItem struct {
	ConversationID string
	MaxSeq         uint64
	LastContentStr string
	LastSender     uint64
	UpdateTime     int64
}

// ConversationDAO 会话数据访问对象
type ConversationDAO struct {
	db      *gorm.DB
	redis   *zeroredis.Redis
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

	rds := zeroredis.MustNewRedis(redisConf)

	dao := &ConversationDAO{
		db:    db,
		redis: rds,
		// 缓冲大小可根据业务并发量调整，这里给个1000作为例子
		syncCh:  make(chan *syncItem, 1000),
		closeCh: make(chan struct{}),
	}
	return dao
}

// SyncConversationToDB 异步收集待保存进数据库的记录，供MQ消费者调用
func (c *ConversationDAO) SyncConversationToDB(conversationID string, maxSeq uint64, lastContentStr string, lastSender uint64, updateTime int64) {
	item := &syncItem{
		ConversationID: conversationID,
		MaxSeq:         maxSeq,
		LastContentStr: lastContentStr,
		LastSender:     lastSender,
		UpdateTime:     updateTime,
	}
	select {
	case c.syncCh <- item:
		// 成功放入 channel
	default:
		logger.Errorf("DB sync channel is full for conversationID: %s, dropping DB update request", item.ConversationID)
	}
}

// StartSyncWorker 后台持续运行，消费 channel 中的数据写 MySQL
func (c *ConversationDAO) StartSyncWorker() {
	c.wg.Add(1)
	defer c.wg.Done()

	batchMap := make(map[string]*syncItem)
	// 定期 1 秒触发或满 100 条触发，确保写入性能与持久化时效平衡
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case item := <-c.syncCh:
			batchMap[item.ConversationID] = item
			if len(batchMap) >= 100 {
				c.flushToDB(batchMap)
				batchMap = make(map[string]*syncItem)
			}
		case <-ticker.C:
			if len(batchMap) > 0 {
				c.flushToDB(batchMap)
				batchMap = make(map[string]*syncItem)
			}
		case <-c.closeCh:
			logger.Info("sync worker is closing, flushing remaining items...")
			if len(batchMap) > 0 {
				c.flushToDB(batchMap)
			}
			return
		}
	}
}

// flushToDB 将 map 里积累的数据批量执行 MySQL 更新
func (c *ConversationDAO) flushToDB(items map[string]*syncItem) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for convID, item := range items {
			res := tx.Model(&model.Conversation{}).
				Where("conversation_id = ?", convID).
				Updates(map[string]interface{}{
					"last_content": item.LastContentStr,
					"last_sender":  item.LastSender,
					"max_seq":      item.MaxSeq,
					"update_time":  time.UnixMilli(item.UpdateTime),
				})
			if res.Error != nil {
				logger.Errorf("failed to bulk update db last_content and max_seq for conv %s: %v", convID, res.Error)
			}
		}
		return nil // 遇到单条出错也不回滚整个事务
	})

	if err != nil {
		logger.Errorf("flushToDB transaction failed: %v", err)
	}
}

// CloseDAO 可在主程序退出时调用，通知后台协程退出并刷盘
func (c *ConversationDAO) CloseDAO() {
	close(c.closeCh)
	c.wg.Wait()
}

// IncrSeq 仅递增会话的序号 (基于 Redis Hash 进行递增，保证整个 conversation 存储在 hset 里以便后续缓存查询)
func (c *ConversationDAO) IncrSeq(ctx context.Context, conversationID string) (uint64, error) {
	cacheKey := convInfoPrefix + conversationID

	// 1. 同步写 Redis (使用 Lua 脚本保证 HINCRBY, HSET 和 EXPIRE 的原子性)
	script := `
		local seq = redis.call("HINCRBY", KEYS[1], "max_seq", 1)
		redis.call("HSET", KEYS[1], "update_time", ARGV[1])
		redis.call("EXPIRE", KEYS[1], ARGV[2])
		return seq
	`
	updateTime := fmt.Sprintf("%d", time.Now().UnixMilli())
	expireSecs := int(convInfoExpire.Seconds())

	val, err := c.redis.EvalCtx(ctx, script, []string{cacheKey}, updateTime, expireSecs)
	if err != nil {
		return 0, fmt.Errorf("redis eval lua script for incr max_seq failed: %w", err)
	}

	var seq int64
	switch v := val.(type) {
	case int64:
		seq = v
	case int:
		seq = int64(v)
	default:
		return 0, fmt.Errorf("unexpected return type from redis lua script: %T", val)
	}

	// 如果 seq == 1说明原来缓存里没有，必须从DB加载正确的seq
	if seq == 1 {
		c.redis.HdelCtx(ctx, cacheKey, "max_seq")
		dbSeq, err := c.incrSeqFromDB(ctx, conversationID, cacheKey)
		if err != nil {
			return 0, err
		}
		return uint64(dbSeq), nil
	}

	return uint64(seq), nil
}

// incrSeqFromDB 从 MySQL 递增 max_seq 并回填 Redis 缓存
func (c *ConversationDAO) incrSeqFromDB(ctx context.Context, conversationID, cacheKey string) (int, error) {
	var conv model.Conversation
	err := c.db.WithContext(ctx).
		Where("conversation_id = ?", conversationID).
		First(&conv).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 会话不存在，自动创建，max_seq 初始化为 1
			now := time.Now()
			newConv := model.Conversation{
				ConversationID: conversationID,
				Type:           int8(common.GetConversationType(conversationID)),
				MaxSeq:         1,
				CreateTime:     now,
				UpdateTime:     now,
			}
			if createErr := c.db.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "conversation_id"}},
				DoUpdates: clause.Assignments(map[string]interface{}{"max_seq": gorm.Expr("max_seq + 1")}),
			}).Create(&newConv).Error; createErr != nil {
				return 0, fmt.Errorf("auto create conversation failed: %w", createErr)
			}
			// 回填 Redis，将该Conversation的每一个字段都设置进去
			fields := map[string]string{
				"conversation_id": newConv.ConversationID,
				"type":            fmt.Sprintf("%d", newConv.Type),
				"last_content":    newConv.LastContent,
				"last_sender":     fmt.Sprintf("%d", newConv.LastSender),
				"max_seq":         fmt.Sprintf("%d", newConv.MaxSeq),
				"create_time":     fmt.Sprintf("%d", newConv.CreateTime.UnixMilli()),
				"update_time":     fmt.Sprintf("%d", newConv.UpdateTime.UnixMilli()),
			}
			err = c.redis.HmsetCtx(ctx, cacheKey, fields)
			if err != nil {
				logger.Errorf("redis hmset failed: %v", err)
			}
			c.redis.ExpireCtx(ctx, cacheKey, int(convInfoExpire.Seconds()))
			return int(newConv.MaxSeq), nil
		}
		return 0, fmt.Errorf("query conversation max_seq failed: %w", err)
	}

	// 递增 DB
	newSeq := conv.MaxSeq + 1
	if updateErr := c.db.WithContext(ctx).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Update("max_seq", newSeq).Error; updateErr != nil {
		return 0, fmt.Errorf("update conversation max_seq failed: %w", updateErr)
	}

	// 回填 Redis，将该Conversation的每一个字段都设置进去
	fields := map[string]string{
		"conversation_id": conv.ConversationID,
		"type":            fmt.Sprintf("%d", conv.Type),
		"last_content":    conv.LastContent,
		"last_sender":     fmt.Sprintf("%d", conv.LastSender),
		"max_seq":         fmt.Sprintf("%d", newSeq),
		"create_time":     fmt.Sprintf("%d", conv.CreateTime.UnixMilli()),
		"update_time":     fmt.Sprintf("%d", time.Now().UnixMilli()),
	}
	err = c.redis.HmsetCtx(ctx, cacheKey, fields)
	if err != nil {
		logger.Errorf("redis hmset failed: %v", err)
	}
	c.redis.ExpireCtx(ctx, cacheKey, int(convInfoExpire.Seconds()))
	return int(newSeq), nil
}
