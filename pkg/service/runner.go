package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	configparser "IM2/pkg/configParser"
	"IM2/pkg/env"
	"IM2/pkg/logger"
)

// ServiceRunner 服务运行器
// 负责服务的生命周期管理：配置加载、启动、停止、信号处理等
type ServiceRunner struct {
	name string

	service Service

	// 配置对象
	config interface{}

	// 本地配置文件路径
	localConfigPath string

	// 优雅关闭超时时间
	shutdownTimeout time.Duration

	// 生命周期钩子
	hooks *LifecycleHooks
}

// Option 函数选项类型
type Option func(*ServiceRunner)

// NewServiceRunner 创建服务运行器（使用函数选项模式）
// service: 服务实例（必需）
// configPath: 配置文件路径
// v: 配置对象的指针
// opts: 可选的配置选项
func NewServiceRunner(service Service, configPath string, v any, opts ...Option) *ServiceRunner {
	// 创建默认的ServiceRunner
	runner := &ServiceRunner{
		service:         service,
		config:          v,
		localConfigPath: configPath,
		shutdownTimeout: 30 * time.Second,
		hooks:           &LifecycleHooks{},
	}

	// 应用所有选项
	for _, opt := range opts {
		opt(runner)
	}

	return runner
}

func WithName(name string) Option {
	return func(r *ServiceRunner) {
		r.name = name
	}
}

// 若要带上服务名，则需要在WithLogger前调用WithName
func WithLogger(logPath string, env logger.LoggerEnv) Option {
	return func(r *ServiceRunner) {
		logger.InitLogger(logPath, r.name, env)
	}
}

// WithShutdownTimeout 设置优雅关闭超时时间
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(r *ServiceRunner) {
		r.shutdownTimeout = timeout
	}
}

func WithBeforeExit(beforeExit func() error) Option {
	return func(r *ServiceRunner) {
		r.hooks.BeforeStop = beforeExit
	}
}

// WithHooks 设置生命周期钩子
func WithHooks(hooks *LifecycleHooks) Option {
	return func(r *ServiceRunner) {
		r.hooks = hooks
	}
}

func (r *ServiceRunner) Name() string {
	return r.name
}

// Run 运行服务
// 完整的生命周期：自动加载配置 -> 启动服务 -> 等待信号 -> 优雅关闭
// 如果设置了ConfigLoader和Config，会自动加载配置
func (r *ServiceRunner) Run() error {
	if err := env.LoadEnv(); err != nil {
		return fmt.Errorf("加载 .env 文件失败: %w", err)
	}

	configLoader := configparser.NewConfigLoader(r.localConfigPath)
	// 1. 自动加载配置（如果配置加载器和配置都存在）
	if err := configLoader.Load(r.config); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 2. 加载服务配置
	if err := r.service.Load(r.config); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 3. 执行启动前钩子
	if r.hooks.BeforeStart != nil {
		if err := r.hooks.BeforeStart(); err != nil {
			return fmt.Errorf("启动前钩子执行失败: %w", err)
		}
	}

	// 4. 启动服务
	log.Printf("服务%s正在启动...", r.Name())
	if err := r.service.Start(); err != nil {
		return fmt.Errorf("服务启动失败: %w", err)
	}

	// 5. 执行启动后钩子
	if r.hooks.AfterStart != nil {
		if err := r.hooks.AfterStart(); err != nil {
			log.Printf("启动后钩子执行失败: %v\n", err)
			// 不返回错误，因为服务已经启动
		}
	}

	log.Printf("服务%s启动成功\n", r.Name())

	// 6. 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("收到退出信号，服务%s正在关闭...\n", r.Name())

	// 7. 执行停止前钩子
	if r.hooks.BeforeStop != nil {
		if err := r.hooks.BeforeStop(); err != nil {
			log.Printf("停止前钩子执行失败: %v\n", err)
		}
	}

	// 7. 优雅关闭服务
	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.service.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("服务停止时出错: %v\n", err)
		}
	case <-ctx.Done():
		log.Printf("服务停止超时（%v），强制退出\n", r.shutdownTimeout)
		return fmt.Errorf("服务停止超时")
	}

	// 8. 执行停止后钩子
	if r.hooks.AfterStop != nil {
		if err := r.hooks.AfterStop(); err != nil {
			log.Printf("停止后钩子执行失败: %v", err)
		}
	}

	logger.Sync()

	log.Printf("服务%s已关闭\n", r.Name())
	return nil
}
