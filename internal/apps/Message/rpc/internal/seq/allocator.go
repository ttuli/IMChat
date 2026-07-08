// Package seq 提供基于 Lamport 逻辑时钟的会话消息序号分配器。
//
// Seq 语义从「Redis 原子递增的连续序号」变为「Lamport 三元组」：
//
//	64-bit 布局：
//	┌──────────────────────┬─────────────┬───────────────┐
//	│  timestamp (42 bits) │ node (10)   │ counter (12)  │
//	│  毫秒级逻辑时钟        │ 最多1024节点 │ 每毫秒4096条    │
//	└──────────────────────┴─────────────┴───────────────┘
//
// 排序规则：先比 timestamp，再比 node_id，最后比 counter（uint64 直接比较即满足）。
// timestamp 取 max(本地物理时钟, 本会话上次分配的时间戳+1)，保证同会话内单节点严格递增，
// 并且对物理时钟回拨免疫。分配过程完全无外部依赖（不访问 Redis / MySQL）。
//
// seq 不再连续，客户端不能用减法计算未读数，历史拉取语义（范围查询）不受影响：
// B-tree 索引只关心大小比较，不关心连续性。
package seq

import (
	"sync"
	"time"
)

const (
	nodeBits    = 10
	counterBits = 12

	nodeShift = counterBits            // 12
	tsShift   = nodeBits + counterBits // 22

	maxNodeID  = (1 << nodeBits) - 1    // 1023
	maxCounter = (1 << counterBits) - 1 // 4095

	// lastTS map 的容量上限，超过时清理已冷却的会话条目防止内存无限增长
	pruneThreshold = 1 << 16
	// 条目冷却时间：时间戳落后当前时间超过该值即可安全清理
	// （物理时钟已远超 lastTS，清理后重新分配不会回退；且被清理的会话
	// 在下次 Alloc 前会经由 Known/Observe 从 DB 重新播种兜底）
	pruneAge = int64(10 * time.Minute / time.Millisecond)
)

// Allocator 每个 Message 服务实例独立持有一个，进程内并发安全。
type Allocator struct {
	mu      sync.Mutex
	nodeID  uint64
	counter uint64
	lastTS  map[string]int64 // sessionID → 该会话最后分配/观察到的逻辑时间戳
	nowFn   func() int64     // 物理时钟，可注入便于测试
}

// NewAllocator 创建分配器。nodeID 超出 10 bits 时取低 10 位。
func NewAllocator(nodeID int64) *Allocator {
	return &Allocator{
		nodeID: uint64(nodeID) & maxNodeID,
		lastTS: make(map[string]int64),
		nowFn:  func() int64 { return time.Now().UnixMilli() },
	}
}

// Alloc 为指定会话分配下一个 seq（热路径，纯内存操作）。
func (a *Allocator) Alloc(sessionID string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 1. 推进 counter；溢出时进位到时间戳，保证同毫秒内不重复
	now := a.nowFn()
	a.counter++
	if a.counter > maxCounter {
		a.counter = 0
		now++
	}

	// 2. Lamport 时钟：物理时钟未推进（或回拨）时，用 lastTS+1 强制推进，
	//    保证同会话内本节点分配的 seq 严格递增
	if last, ok := a.lastTS[sessionID]; ok && now <= last {
		now = last + 1
	}
	a.lastTS[sessionID] = now

	if len(a.lastTS) > pruneThreshold {
		a.pruneLocked(a.nowFn())
	}

	return compose(now, a.nodeID, a.counter)
}

// Observe 告知分配器该会话已存在的最大 seq（来自 DB 播种或其他节点的消息），
// 保证后续本地分配不落后于已持久化的序号（进程重启 / 时钟回拨兜底）。
func (a *Allocator) Observe(sessionID string, seq uint64) {
	if seq == 0 {
		return
	}
	ts := SeqTimestamp(seq)
	a.mu.Lock()
	defer a.mu.Unlock()
	if cur, ok := a.lastTS[sessionID]; !ok || ts > cur {
		a.lastTS[sessionID] = ts
	}
}

// Known 返回该会话是否已有时间戳记录。调用方在首次遇到会话时
// 应查询存储层最大 seq 并通过 Observe 播种。
func (a *Allocator) Known(sessionID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.lastTS[sessionID]
	return ok
}

// pruneLocked 清理时间戳已远落后于物理时钟的冷会话条目，须持锁调用。
func (a *Allocator) pruneLocked(now int64) {
	for id, ts := range a.lastTS {
		if now-ts > pruneAge {
			delete(a.lastTS, id)
		}
	}
}

func compose(ts int64, node, counter uint64) uint64 {
	return (uint64(ts) << tsShift) | (node << nodeShift) | counter
}

// SeqTimestamp 从 seq 中解出毫秒时间戳部分。
func SeqTimestamp(seq uint64) int64 {
	return int64(seq >> tsShift)
}

// SeqNodeID 从 seq 中解出节点 ID 部分。
func SeqNodeID(seq uint64) uint64 {
	return (seq >> nodeShift) & maxNodeID
}
