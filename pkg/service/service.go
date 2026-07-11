package service

// Service 服务接口，T 为业务配置结构体类型
type Service[T any] interface {
	// Start 务必是非阻塞函数
	Start() error

	Stop() error

	// Load 加载配置并初始化服务
	Load(config *T) error
}
