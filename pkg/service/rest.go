package service

import (
	"fmt"

	"github.com/zeromicro/go-zero/rest"
)

// RestConfExtractor 从配置中提取 RestConf
type RestConfExtractor func(cfg any) *rest.RestConf

// RestService 通用的 REST API 服务实现
type RestService struct {
	registerServices  RegisterRestServicesFunc
	server            *rest.Server      // rest 服务器
	restConf          *rest.RestConf    // rest 配置
	restConfExtractor RestConfExtractor // rest 配置提取器
}

type RegisterRestServicesFunc func(cfg any, server *rest.Server) error

// RestOption 函数选项类型
type RestOption func(*RestService)

// WithRestConf 设置 REST 配置提取器
func WithRestConf(extractor RestConfExtractor) RestOption {
	return func(rs *RestService) {
		rs.restConfExtractor = extractor
	}
}

// NewRestService 创建通用的 RestService
func NewRestService(registerServices RegisterRestServicesFunc, opts ...RestOption) *RestService {
	rs := &RestService{
		registerServices: registerServices,
	}
	for _, opt := range opts {
		opt(rs)
	}
	return rs
}

// Load 加载配置并初始化服务
func (rs *RestService) Load(cfg any) error {
	// 使用提取器从 cfg 中提取配置
	if rs.restConfExtractor != nil {
		rs.restConf = rs.restConfExtractor(cfg)
	}

	if rs.restConf == nil {
		return fmt.Errorf("REST 配置不能为空，请使用 WithRestConf 设置提取器")
	}

	server, err := rest.NewServer(*rs.restConf, rest.WithCors("*"))
	if err != nil {
		return fmt.Errorf("创建 rest 服务器失败: %w", err)
	}
	rs.server = server

	if err := rs.registerServices(cfg, server); err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}

	return nil
}

// Start 启动服务
func (rs *RestService) Start() error {
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
func (rs *RestService) Stop() error {
	if rs.server != nil {
		rs.server.Stop()
	}
	return nil
}
