package seq

import (
	"sync"
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
		s := a.Alloc("s1")
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
		s := a.Alloc(sessions[i%3])
		if seen[s] {
			t.Fatalf("duplicate seq %d at iteration %d", s, i)
		}
		seen[s] = true
	}
}

func TestClockRollback(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	s1 := a.Alloc("s1")
	clock -= 3600_000 // 物理时钟回拨 1 小时
	s2 := a.Alloc("s1")
	if s2 <= s1 {
		t.Fatalf("seq regressed after clock rollback: %d -> %d", s1, s2)
	}

	clock += 7200_000 // 时钟恢复并前进
	s3 := a.Alloc("s1")
	if s3 <= s2 {
		t.Fatalf("seq regressed after clock recovery: %d -> %d", s2, s3)
	}
	if SeqTimestamp(s3) != clock {
		t.Fatalf("timestamp should realign to physical clock: got %d want %d", SeqTimestamp(s3), clock)
	}
}

func TestObserveSeeding(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	if a.Known("s1") {
		t.Fatal("session should be unknown before seeding")
	}

	// 模拟重启后从 DB 播种一个"未来"的 seq（重启前时钟偏快的场景）
	persisted := compose(clock+5000, 3, 100)
	a.Observe("s1", persisted)
	if !a.Known("s1") {
		t.Fatal("session should be known after Observe")
	}

	s := a.Alloc("s1")
	if s <= persisted {
		t.Fatalf("alloc after seeding must exceed persisted seq: persisted=%d got=%d", persisted, s)
	}
}

func TestObserveOlderSeqIgnored(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	s1 := a.Alloc("s1")
	a.Observe("s1", compose(clock-10000, 2, 0)) // 更旧的观察值不应回退时钟
	s2 := a.Alloc("s1")
	if s2 <= s1 {
		t.Fatalf("older Observe caused regression: %d -> %d", s1, s2)
	}
}

func TestCounterOverflowCarries(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(1, &clock)

	// 同一毫秒内分配超过 4096 条，counter 溢出应进位不重复
	seen := make(map[uint64]bool)
	for i := 0; i < maxCounter+100; i++ {
		s := a.Alloc("s1")
		if seen[s] {
			t.Fatalf("duplicate seq on counter overflow at %d", i)
		}
		seen[s] = true
	}
}

func TestNodeIDInSeq(t *testing.T) {
	clock := int64(1_700_000_000_000)
	a := newTestAllocator(777, &clock)
	s := a.Alloc("s1")
	if SeqNodeID(s) != 777 {
		t.Fatalf("node id mismatch: got %d want 777", SeqNodeID(s))
	}
}

func TestConcurrentAlloc(t *testing.T) {
	a := NewAllocator(9)
	var mu sync.Mutex
	seen := make(map[uint64]bool)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, 0, 2000)
			for i := 0; i < 2000; i++ {
				local = append(local, a.Alloc("s1"))
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
