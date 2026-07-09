package dao

import (
	"sync"
	"time"
)

// ttlGuard 进程内带过期的防重标记。
// 用于 user_session 补偿写入：同一 key 在 ttl 周期内只放行一次，
// 过期后自动允许再次执行（多实例/重启场景由 DB 侧 OnConflict 幂等兜底）。
type ttlGuard struct {
	mu   sync.Mutex
	ttl  time.Duration
	max  int
	seen map[string]time.Time
}

func newTTLGuard(ttl time.Duration, max int) *ttlGuard {
	return &ttlGuard{
		ttl:  ttl,
		max:  max,
		seen: make(map[string]time.Time),
	}
}

// tryAcquire 尝试获取 key 的执行权：周期内已放行过则返回 false
func (g *ttlGuard) tryAcquire(key string) bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()

	if t, ok := g.seen[key]; ok && now.Sub(t) < g.ttl {
		return false
	}

	// 容量兜底：先清过期项，仍然满则整体重置（代价只是多做一轮幂等补偿）
	if len(g.seen) >= g.max {
		for k, t := range g.seen {
			if now.Sub(t) >= g.ttl {
				delete(g.seen, k)
			}
		}
		if len(g.seen) >= g.max {
			g.seen = make(map[string]time.Time)
		}
	}

	g.seen[key] = now
	return true
}

// release 主动释放标记（执行失败时调用，允许重试）
func (g *ttlGuard) release(key string) {
	g.mu.Lock()
	delete(g.seen, key)
	g.mu.Unlock()
}
