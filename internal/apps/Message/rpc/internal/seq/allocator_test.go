package seq

import (
	"sync"
	"sync/atomic"
	"testing"
)

// newTestAllocator 返回使用可控时钟的分配器
func newTestAllocator(nodeID int64, clock *int64) *Allocator {
	a := NewAllocator(nodeID)
	a.nowFn = func() int64 { return *clock }
	return a
}

func TestAllocMonotonicPerSession(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(5, &clock)

	var prev uint64
	for i := 0; i < 10000; i++ {
		s := a.Alloc("s1", uint64(i+1))
		if s <= prev {
			t.Fatalf("seq not strictly increasing at %d: prev=%d cur=%d", i, prev, s)
		}
		prev = s
	}
}

// streamSeq 缺失（如 DLQ 重放）时退化为本地逻辑时钟，同会话仍严格递增
func TestAllocMonotonicWithoutStreamSeq(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(5, &clock)

	var prev uint64
	for i := 0; i < 10000; i++ {
		s := a.Alloc("s1", 0)
		if s <= prev {
			t.Fatalf("seq not strictly increasing at %d: prev=%d cur=%d", i, prev, s)
		}
		prev = s
	}
}

func TestAllocUniqueAcrossSessions(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(5, &clock)

	seen := make(map[uint64]bool)
	sessions := []string{"a", "b", "c"}
	for i := 0; i < 30000; i++ {
		s := a.Alloc(sessions[i%3], uint64(i+1))
		if seen[s] {
			t.Fatalf("duplicate seq %d at iteration %d", s, i)
		}
		seen[s] = true
	}
}

// 跨实例物理时钟倾斜不影响 seq 顺序：顺序由 stream sequence 决定。
// 模拟 warn.txt 场景：两台实例通过共享 Durable Consumer 交错消费同一会话，
// 实例 A 的物理时钟比实例 B 慢 1 小时。
func TestClockSkewImmune(t *testing.T) {
	clockA := int64(1_700_000_000_000)
	clockB := clockA + 3600_000
	instA := newTestAllocator(1, &clockA)
	instB := newTestAllocator(2, &clockB)

	var prev uint64
	for i := 1; i <= 1000; i++ {
		inst := instA
		if i%2 == 0 {
			inst = instB
		}
		s := inst.Alloc("s1", uint64(i))
		if s <= prev {
			t.Fatalf("cross-instance seq regressed at stream seq %d: prev=%d cur=%d", i, prev, s)
		}
		prev = s
	}
}

// stream 删除重建导致 stream sequence 回退时，由本会话 lastTS+1 兜底不回退
func TestStreamSeqRegression(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	s1 := a.Alloc("s1", 1_000_000)
	s2 := a.Alloc("s1", 3) // stream 重建，sequence 从头再来
	if s2 <= s1 {
		t.Fatalf("seq regressed after stream seq rollback: %d -> %d", s1, s2)
	}

	s3 := a.Alloc("s1", 2_000_000)
	if s3 <= s2 {
		t.Fatalf("seq regressed after stream seq recovery: %d -> %d", s2, s3)
	}
	if SeqTimestamp(s3) != tsEpoch+2_000_000 {
		t.Fatalf("timestamp should realign to stream seq: got %d want %d", SeqTimestamp(s3), tsEpoch+2_000_000)
	}
}

func TestObserveSeeding(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	if a.Known("s1") {
		t.Fatal("session should be unknown before seeding")
	}

	// 模拟重启后从 DB 播种一个远超当前 stream sequence 的已持久化 seq
	persisted := compose(tsEpoch+5_000_000, 3, 100)
	a.Observe("s1", persisted)
	if !a.Known("s1") {
		t.Fatal("session should be known after Observe")
	}

	s := a.Alloc("s1", 10)
	if s <= persisted {
		t.Fatalf("alloc after seeding must exceed persisted seq: persisted=%d got=%d", persisted, s)
	}
}

func TestObserveOlderSeqIgnored(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	s1 := a.Alloc("s1", 100)
	a.Observe("s1", compose(tsEpoch+50, 2, 0)) // 更旧的观察值不应回退时钟
	s2 := a.Alloc("s1", 101)
	if s2 <= s1 {
		t.Fatalf("older Observe caused regression: %d -> %d", s1, s2)
	}
}

// 存量兼容：旧版本按物理毫秒时钟分配的 seq 必须小于升级后按 stream sequence
// 分配的 seq（tsEpoch 基线保证支配性）
func TestLegacySeqCompatibility(t *testing.T) {
	clock := int64(1_760_000_000_000)
	a := newTestAllocator(1, &clock)

	legacy := compose(1_752_000_000_000, 7, 42) // 旧版本 2026-07 分配的存量 seq
	a.Observe("s1", legacy)

	s := a.Alloc("s1", 1) // 升级后第一条消息，stream sequence 很小
	if s <= legacy {
		t.Fatalf("post-upgrade seq must exceed legacy seq: legacy=%d got=%d", legacy, s)
	}
}

func TestNodeIDInSeq(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(777, &clock)
	s := a.Alloc("s1", 1)
	if SeqNodeID(s) != 777 {
		t.Fatalf("node id mismatch: got %d want 777", SeqNodeID(s))
	}
}

func TestConcurrentAlloc(t *testing.T) {
	a := NewAllocator(9)
	var streamSeq atomic.Uint64
	var mu sync.Mutex
	seen := make(map[uint64]bool)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, 0, 2000)
			for i := 0; i < 2000; i++ {
				local = append(local, a.Alloc("s1", streamSeq.Add(1)))
			}
			mu.Lock()
			defer mu.Unlock()
			for _, s := range local {
				if seen[s] {
					t.Errorf("duplicate seq %d", s)
					return
				}
				seen[s] = true
			}
		}()
	}
	wg.Wait()
}
