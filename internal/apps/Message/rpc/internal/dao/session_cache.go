package dao

import (
	"container/list"
	"sync"
)

// sessionKeyCache 是 sessionKey → sessionID 的进程内 LRU 缓存。
// 该映射一经创建不可变（key 与 ID 永久绑定），因此无需失效逻辑，
// 用于消除消息消费热路径上每条消息一次的 MySQL 会话查询。
type sessionKeyCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List               // 头部为最近使用
	items map[string]*list.Element // sessionKey → element
}

type sessionKeyEntry struct {
	key       string
	sessionID string
}

func newSessionKeyCache(capacity int) *sessionKeyCache {
	return &sessionKeyCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element, capacity),
	}
}

func (c *sessionKeyCache) get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*sessionKeyEntry).sessionID, true
}

func (c *sessionKeyCache) put(key, sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*sessionKeyEntry).sessionID = sessionID
		return
	}
	c.items[key] = c.ll.PushFront(&sessionKeyEntry{key: key, sessionID: sessionID})
	if c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.items, oldest.Value.(*sessionKeyEntry).key)
		}
	}
}
