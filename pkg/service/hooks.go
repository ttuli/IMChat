package service

// LifecycleHooks 生命周期钩子
type LifecycleHooks struct {
	BeforeStart func() error
	AfterStart  func() error
	BeforeStop  func() error
	AfterStop   func() error
}