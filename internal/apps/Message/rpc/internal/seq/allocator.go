// Package seq 提供基于 Lamport 逻辑时钟的会话消息序号分配器。
//
// Seq 语义为「Lamport 三元组」：
//
//	64-bit 布局：
//	┌──────────────────────┬─────────────┬───────────────┐
//	│  timestamp (42 bits) │ node (10)   │ counter (12)  │
//	│  逻辑时钟             │ 最多1024节点 │ 同时间戳防重    │
//	└──────────────────────┴─────────────┴───────────────┘
//
// 排序规则：先比 timestamp，再比 node_id，最后比 counter（uint64 直接比较即满足）。
//
// timestamp 的取值源是消息的 JetStream stream sequence（而非本机物理时钟）：
// 同一 stream 内 stream sequence 全局单调且与消费实例无关，因此多台 Message
// 实例通过共享 Durable Consumer 并发消费同一会话时，不会因实例间物理时钟
// 倾斜产生 seq 逆序（后发布的消息拿到更小的 seq，导致客户端增量拉取丢消息）。
//
// timestamp 取 max(tsEpoch+streamSeq, 本会话上次时间戳+1)，保证：
//   - 正常路径：seq 顺序严格跟随消息进入 stream 的顺序，跨实例一致；
//   - 退化路径（stream 删除重建导致 sequence 回退、DLQ 重放缺失元数据传 0）：
//     由本会话 lastTS+1 兜底，同会话内仍严格递增。
//
// 分配过程完全无外部依赖（不访问 Redis / MySQL）。
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

	// tsEpoch 是 stream sequence 映射到时间戳域的基线偏移。
	// 必须大于旧版本（物理毫秒时钟）已分配的全部历史时间戳，保证升级后
	// 新分配的 seq 一定大于存量 seq；取 2027-01-01T00:00:00Z 的毫秒数。
	// 42 bits 时间戳上限约 4.39e12，扣除基线后可用 stream sequence 约 2.6e12 条。
	tsEpoch = int64(1798761600000)

	// lastTS map 的容量上限，超过时清理已冷却的会话条目防止内存无限增长
	pruneThreshold = 1 << 16
	// 条目冷却时间：最后一次访问距今超过该值即可安全清理
	//（被清理的会话在下次 Alloc 前会经由 Known/Observe 从 DB 重新播种兜底）
	pruneAge = int64(10 * time.Minute / time.Millisecond)
)

// sessionClock 单个会话的逻辑时钟状态
type sessionClock struct {
	ts      int64 // 该会话最后分配/观察到的逻辑时间戳
	touched int64 // 最后一次访问的物理毫秒，仅用于冷会话清理
}

// Allocator 每个 Message 服务实例独立持有一个，进程内并发安全。
type Allocator struct {
	mu      sync.Mutex
	nodeID  uint64
	counter uint64
	lastTS  map[string]*sessionClock // sessionID → 会话逻辑时钟
	nowFn   func() int64             // 物理时钟，仅用于冷会话清理与退化路径，可注入便于测试
}

// NewAllocator 创建分配器。nodeID 超出 10 bits 时取低 10 位。
func NewAllocator(nodeID int64) *Allocator {
	return &Allocator{
		nodeID: uint64(nodeID) & maxNodeID,
		lastTS: make(map[string]*sessionClock),
		nowFn:  func() int64 { return time.Now().UnixMilli() },
	}
}

// Alloc 为指定会话分配下一个 seq（热路径，纯内存操作）。
// streamSeq 是该消息的 JetStream stream sequence，作为跨实例一致的逻辑时钟源；
// 传 0（元数据缺失，如 DLQ 重放）时退化为本会话 lastTS+1 / 物理时钟。
func (a *Allocator) Alloc(sessionID string, streamSeq uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.nowFn()
	// counter 仅用于同时间戳防重（正常路径每条消息的 streamSeq 唯一，
	// 时间戳天然不同；退化路径同会话时间戳也严格 +1，碰撞仅存在于
	// 不同会话恰好推进到同一时间戳的极端情形）
	a.counter = (a.counter + 1) & maxCounter

	var ts int64
	if streamSeq > 0 {
		ts = tsEpoch + int64(streamSeq)
	} else {
		ts = now
	}
	if e, ok := a.lastTS[sessionID]; ok {
		if ts <= e.ts {
			ts = e.ts + 1
		}
		e.ts = ts
		e.touched = now
	} else {
		a.lastTS[sessionID] = &sessionClock{ts: ts, touched: now}
	}

	if len(a.lastTS) > pruneThreshold {
		a.pruneLocked(now)
	}

	return compose(ts, a.nodeID, a.counter)
}

// Observe 告知分配器该会话已存在的最大 seq（来自 DB 播种或其他节点的消息），
// 保证后续本地分配不落后于已持久化的序号（进程重启 / stream 重建兜底）。
func (a *Allocator) Observe(sessionID string, seq uint64) {
	if seq == 0 {
		return
	}
	ts := SeqTimestamp(seq)
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.nowFn()
	if e, ok := a.lastTS[sessionID]; ok {
		if ts > e.ts {
			e.ts = ts
		}
		e.touched = now
	} else {
		a.lastTS[sessionID] = &sessionClock{ts: ts, touched: now}
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

// pruneLocked 清理长时间未访问的冷会话条目，须持锁调用。
func (a *Allocator) pruneLocked(now int64) {
	for id, e := range a.lastTS {
		if now-e.touched > pruneAge {
			delete(a.lastTS, id)
		}
	}
}

func compose(ts int64, node, counter uint64) uint64 {
	return (uint64(ts) << tsShift) | (node << nodeShift) | counter
}

// SeqTimestamp 从 seq 中解出逻辑时间戳部分。
func SeqTimestamp(seq uint64) int64 {
	return int64(seq >> tsShift)
}

// SeqNodeID 从 seq 中解出节点 ID 部分。
func SeqNodeID(seq uint64) uint64 {
	return (seq >> nodeShift) & maxNodeID
}
