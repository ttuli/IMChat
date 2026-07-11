package service

import (
	"fmt"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

type RegisterRpcServicesFunc[T any] func(c *T) (*zrpc.RpcServer, error)

// rpcOptions 与配置类型无关的选项集合
// 单独成结构体是为了让 RpcOption 保持非泛型：调用方写 WithUnaryInterceptors(...) 时无需显式类型参数
type rpcOptions struct {
	unaryInterceptors []grpc.UnaryServerInterceptor
}

// RpcOption 函数选项类型
type RpcOption func(*rpcOptions)

// WithUnaryInterceptors 注入 gRPC 一元拦截器
// 拦截器由调用方显式传入，pkg 层不绑定任何业务实现
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) RpcOption {
	return func(o *rpcOptions) {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptors...)
	}
}

// RpcService 通用的 RPC 服务实现
// T 为业务配置结构体类型
type RpcService[T any] struct {
	rpcOptions
	registerRpcServices RegisterRpcServicesFunc[T]
	server              *zrpc.RpcServer
}

func NewRpcService[T any](registerRpcServices RegisterRpcServicesFunc[T], opts ...RpcOption) *RpcService[T] {
	rs := &RpcService[T]{
		registerRpcServices: registerRpcServices,
	}
	for _, opt := range opts {
		opt(&rs.rpcOptions)
	}
	return rs
}

func (rs *RpcService[T]) Load(c *T) error {
	s, err := rs.registerRpcServices(c)
	if err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}
	rs.server = s
	if len(rs.unaryInterceptors) > 0 {
		rs.server.AddUnaryInterceptors(rs.unaryInterceptors...)
	}
	return nil
}

// Start 启动服务
// 注意：go-zero 的 RpcServer.Start 是阻塞调用且不返回错误，
// 启动失败（如端口被占用）时其内部会直接退出进程，此处无法捕获启动错误
func (rs *RpcService[T]) Start() error {
	if rs.server == nil {
		return fmt.Errorf("服务器未初始化，请先调用Load方法")
	}
	go func() {
		rs.server.Start()
	}()
	return nil
}

func (rs *RpcService[T]) Stop() error {
	if rs.server != nil {
		rs.server.Stop()
	}
	return nil
}

// 确保 RpcService 实现了 Service 接口
var _ Service[struct{}] = (*RpcService[struct{}])(nil)
