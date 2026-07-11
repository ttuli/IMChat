package service

import "testing"

// WithBeforeExit 与 WithHooks 共用时，二者设置的钩子都应保留，且与书写顺序无关
func TestWithHooksMergesWithBeforeExit(t *testing.T) {
	beforeExit := func() error { return nil }
	hooks := &LifecycleHooks{
		BeforeStart: func() error { return nil },
		AfterStop:   func() error { return nil },
	}

	orders := map[string][]Option{
		"BeforeExit先": {WithBeforeExit(beforeExit), WithHooks(hooks)},
		"Hooks先":      {WithHooks(hooks), WithBeforeExit(beforeExit)},
	}
	for name, opts := range orders {
		r := NewServiceRunner[struct{}](nil, "", opts...)
		if r.hooks.BeforeStop == nil {
			t.Errorf("%s: WithBeforeExit 设置的 BeforeStop 被丢弃", name)
		}
		if r.hooks.BeforeStart == nil {
			t.Errorf("%s: WithHooks 设置的 BeforeStart 被丢弃", name)
		}
		if r.hooks.AfterStop == nil {
			t.Errorf("%s: WithHooks 设置的 AfterStop 被丢弃", name)
		}
	}
}

// WithHooks(nil) 不应 panic，也不应破坏默认 hooks
func TestWithHooksNil(t *testing.T) {
	r := NewServiceRunner[struct{}](nil, "", WithHooks(nil))
	if r.hooks == nil {
		t.Fatal("hooks 不应为 nil")
	}
}

// WithLogger 只记录配置不产生副作用，与 WithName 顺序无关
func TestWithLoggerOrderIndependent(t *testing.T) {
	r := NewServiceRunner[struct{}](nil, "",
		WithLogger("/tmp/test.log", 0),
		WithName("after-logger"),
	)
	if r.loggerCfg == nil || r.loggerCfg.logPath != "/tmp/test.log" {
		t.Fatal("loggerCfg 未正确记录")
	}
	if r.name != "after-logger" {
		t.Fatalf("name = %q", r.name)
	}
}
