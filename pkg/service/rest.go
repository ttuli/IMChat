package service

import (
	"fmt"

	"github.com/zeromicro/go-zero/rest"
)

type RegisterRestServicesFunc[T any] func(c *T, server *rest.Server) error

// RestConfExtractor 从业务配置中取出 rest.RestConf
type RestConfExtractor[T any] func(c *T) *rest.RestConf

// RestService 通用的 REST API 服务实现
// T 为业务配置结构体类型
type RestService[T any] struct {
	registerServices RegisterRestServicesFunc[T]
	extractRestConf  RestConfExtractor[T]
	server           *rest.Server // rest 服务器
}

// NewRestService 创建通用的 RestService
// extractRestConf 为必填参数：REST 配置的可获取性在编译期保证，替代原先的运行期检查
func NewRestService[T any](registerServices RegisterRestServicesFunc[T], extractRestConf RestConfExtractor[T]) *RestService[T] {
	return &RestService[T]{
		registerServices: registerServices,
		extractRestConf:  extractRestConf,
	}
}

// Load 加载配置并初始化服务
func (rs *RestService[T]) Load(c *T) error {
	restConf := rs.extractRestConf(c)
	if restConf == nil {
		return fmt.Errorf("从配置中未提取到 REST 配置")
	}

	server, err := rest.NewServer(*restConf, rest.WithCors("*"))
	if err != nil {
		return fmt.Errorf("创建 rest 服务器失败: %w", err)
	}
	rs.server = server

	if err := rs.registerServices(c, server); err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}

	return nil
}

// Start 启动服务
// 注意：go-zero 的 rest.Server.Start 是阻塞调用且不返回错误，
// 启动失败（如端口被占用）时其内部会直接退出进程，此处无法捕获启动错误
func (rs *RestService[T]) Start() error {
	if rs.server == nil {
		return fmt.Errorf("服务器未初始化，请先调用 Load 方法")
	}

	// 在 goroutine 中启动，避免阻塞
	go func() {
		rs.server.Start()
	}()
	return nil
}

// Stop 停止服务
func (rs *RestService[T]) Stop() error {
	if rs.server != nil {
		rs.server.Stop()
	}
	return nil
}

// 确保 RestService 实现了 Service 接口
var _ Service[struct{}] = (*RestService[struct{}])(nil)
