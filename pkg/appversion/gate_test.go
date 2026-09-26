package appversion

import "testing"

// fakeSubscriber 模拟 go-zero configcenter 的 etcd subscriber：
// 覆盖写入即替换当前值，删除即清空，每次变化回调监听
type fakeSubscriber struct {
	value     string
	listeners []func()
}

func (f *fakeSubscriber) AddListener(l func()) error {
	f.listeners = append(f.listeners, l)
	return nil
}

func (f *fakeSubscriber) Value() (string, error) {
	return f.value, nil
}

func (f *fakeSubscriber) put(v string) {
	f.value = v
	for _, l := range f.listeners {
		l()
	}
}

func TestGateFollowsKey(t *testing.T) {
	// 订阅创建时 key 已有值：创建完即生效，不依赖监听回调
	sub := &fakeSubscriber{value: "1.0.0"}
	g, err := newGate(sub, DefaultKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := g.MinVersion(); got != (Version{1, 0, 0}) {
		t.Fatalf("初始应为 1.0.0，实际 %s", got)
	}

	sub.put("1.2.0")
	if got := g.MinVersion(); got != (Version{1, 2, 0}) {
		t.Fatalf("覆盖写入后应为 1.2.0，实际 %s", got)
	}

	// 非法值：保留上一次的有效值，不能因为手误放开或锁死
	sub.put("1.3")
	if got := g.MinVersion(); got != (Version{1, 2, 0}) {
		t.Fatalf("非法值应沿用 1.2.0，实际 %s", got)
	}

	// 空值：取消限制
	sub.put("  ")
	if !g.MinVersion().IsZero() {
		t.Fatalf("空值应取消限制，实际 %s", g.MinVersion())
	}

	// 删除 key（subscriber 的 Value() 变为空串）：取消限制
	sub.put("2.0.0")
	sub.put("")
	if !g.MinVersion().IsZero() {
		t.Fatalf("删除 key 应取消限制，实际 %s", g.MinVersion())
	}
}

func TestGateWithoutKey(t *testing.T) {
	g, err := newGate(&fakeSubscriber{}, DefaultKey)
	if err != nil {
		t.Fatal(err)
	}
	if !g.MinVersion().IsZero() {
		t.Fatalf("key 不存在应不限制，实际 %s", g.MinVersion())
	}
}

func TestNilGate(t *testing.T) {
	var g *Gate
	if !g.MinVersion().IsZero() {
		t.Fatal("nil Gate 应视为不限制")
	}
}
