package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	configparser "IM2/pkg/configParser"
	"IM2/pkg/env"
	"IM2/pkg/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// runnerOptions 与配置类型无关的运行器选项集合
// 单独成结构体是为了让 Option 保持非泛型：调用方写 WithName("...") 时无需显式类型参数
type runnerOptions struct {
	name string

	// 优雅关闭超时时间
	shutdownTimeout time.Duration

	// 日志配置。选项只记录值，实际初始化延迟到 Run 中、.env 加载之后执行，
	// 保证 DISABLE_FILE_LOG 等变量可来自 .env，且不依赖 WithName/WithLogger 的书写顺序
	loggerCfg *loggerConfig

	// 生命周期钩子
	hooks *LifecycleHooks
}

type loggerConfig struct {
	logPath string
	env     logger.LoggerEnv
}

// Option 函数选项类型
type Option func(*runnerOptions)

// ServiceRunner 服务运行器
// 负责服务的生命周期管理：配置加载、启动、停止、信号处理等
// T 为业务配置结构体类型，配置对象由 runner 自行分配并在 Run 中加载
type ServiceRunner[T any] struct {
	runnerOptions

	service Service[T]

	// 配置对象
	config *T

	// 本地配置文件路径
	localConfigPath string
}

// NewServiceRunner 创建服务运行器（使用函数选项模式）
// service: 服务实例（必需）
// configPath: 配置文件路径
// opts: 可选的配置选项
func NewServiceRunner[T any](service Service[T], configPath string, opts ...Option) *ServiceRunner[T] {
	// 创建默认的ServiceRunner
	runner := &ServiceRunner[T]{
		runnerOptions: runnerOptions{
			shutdownTimeout: 30 * time.Second,
			hooks:           &LifecycleHooks{},
		},
		service:         service,
		config:          new(T),
		localConfigPath: configPath,
	}

	// 应用所有选项
	for _, opt := range opts {
		opt(&runner.runnerOptions)
	}

	return runner
}

func WithName(name string) Option {
	return func(o *runnerOptions) {
		o.name = name
	}
}

// WithLogger 声明日志配置，与其他选项的书写顺序无关
func WithLogger(logPath string, env logger.LoggerEnv) Option {
	return func(o *runnerOptions) {
		o.loggerCfg = &loggerConfig{logPath: logPath, env: env}
	}
}

// WithShutdownTimeout 设置优雅关闭超时时间
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *runnerOptions) {
		o.shutdownTimeout = timeout
	}
}

func WithBeforeExit(beforeExit func() error) Option {
	return func(o *runnerOptions) {
		o.hooks.BeforeStop = beforeExit
	}
}

// WithHooks 设置生命周期钩子
// 按字段合并：只覆盖非 nil 的钩子，可与 WithBeforeExit 共存且与书写顺序无关
func WithHooks(hooks *LifecycleHooks) Option {
	return func(o *runnerOptions) {
		if hooks == nil {
			return
		}
		if hooks.BeforeStart != nil {
			o.hooks.BeforeStart = hooks.BeforeStart
		}
		if hooks.AfterStart != nil {
			o.hooks.AfterStart = hooks.AfterStart
		}
		if hooks.BeforeStop != nil {
			o.hooks.BeforeStop = hooks.BeforeStop
		}
		if hooks.AfterStop != nil {
			o.hooks.AfterStop = hooks.AfterStop
		}
	}
}

func (r *ServiceRunner[T]) Name() string {
	return r.name
}

// initLogger 初始化日志，必须在 env.LoadEnv 之后调用
func (r *ServiceRunner[T]) initLogger() {
	if r.loggerCfg == nil {
		return
	}

	// 从环境变量读取，支持禁用文件日志（如 Docker 原生日志场景）
	if os.Getenv("DISABLE_FILE_LOG") == "true" {
		logger.InitLogger("", r.name, r.loggerCfg.env)
		return
	}

	// 在分布式环境/多实例部署下，防止日志文件名冲突，添加 uuid 后缀
	logPath := r.loggerCfg.logPath
	ext := filepath.Ext(logPath)
	base := strings.TrimSuffix(logPath, ext)
	u := uuid.New().String()
	// 取 uuid 前8位即可基本保证不冲突，且文件名不会太长
	finalLogPath := fmt.Sprintf("%s-%s%s", base, u[:8], ext)

	logger.InitLogger(finalLogPath, r.name, r.loggerCfg.env)
}

// infof / errorf 生命周期日志：logger 初始化后走 zap（可被 Loki 收集），否则回退标准库 log
func infof(format string, args ...any) {
	if logger.IsInitialized {
		logger.Logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Infof(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func errorf(format string, args ...any) {
	if logger.IsInitialized {
		logger.Logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Errorf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Run 运行服务
// 完整的生命周期：加载环境变量 -> 初始化日志 -> 加载配置 -> 启动服务 -> 等待信号 -> 优雅关闭
func (r *ServiceRunner[T]) Run() error {
	// 1. 加载 .env 环境变量
	if err := env.LoadEnv(); err != nil {
		return fmt.Errorf("加载 .env 文件失败: %w", err)
	}

	// 2. 初始化日志（在 .env 加载之后，保证 DISABLE_FILE_LOG/ENV 等变量生效）
	r.initLogger()
	// 无论以何种路径退出都刷掉 zap 缓冲，避免异常退出时丢最后几条日志
	defer logger.Sync()

	// 3. 加载配置
	if err := configparser.Load(r.localConfigPath, r.config); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 4. 加载服务配置
	if err := r.service.Load(r.config); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 5. 执行启动前钩子
	if r.hooks.BeforeStart != nil {
		if err := r.hooks.BeforeStart(); err != nil {
			return fmt.Errorf("启动前钩子执行失败: %w", err)
		}
	}

	// 6. 启动服务
	infof("服务%s正在启动...", r.Name())
	if err := r.service.Start(); err != nil {
		return fmt.Errorf("服务启动失败: %w", err)
	}

	// 7. 执行启动后钩子
	if r.hooks.AfterStart != nil {
		if err := r.hooks.AfterStart(); err != nil {
			errorf("启动后钩子执行失败: %v", err)
			// 不返回错误，因为服务已经启动
		}
	}

	infof("服务%s启动成功", r.Name())

	// 8. 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 二次信号直接强制退出，避免 Stop 卡死时只能干等超时
	go func() {
		<-quit
		errorf("收到第二次退出信号，强制退出")
		_ = logger.Sync()
		os.Exit(130)
	}()

	infof("收到退出信号，服务%s正在关闭...", r.Name())

	// 9. 执行停止前钩子
	if r.hooks.BeforeStop != nil {
		if err := r.hooks.BeforeStop(); err != nil {
			errorf("停止前钩子执行失败: %v", err)
		}
	}

	// 10. 优雅关闭服务
	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.service.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			errorf("服务停止时出错: %v", err)
		}
	case <-ctx.Done():
		// 超时说明服务没有停干净，跳过 AfterStop（其语义是服务已正常停止后的清理）
		errorf("服务停止超时（%v），强制退出", r.shutdownTimeout)
		return fmt.Errorf("服务停止超时")
	}

	// 11. 执行停止后钩子
	if r.hooks.AfterStop != nil {
		if err := r.hooks.AfterStop(); err != nil {
			errorf("停止后钩子执行失败: %v", err)
		}
	}

	infof("服务%s已关闭", r.Name())
	return nil
}
