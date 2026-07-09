package service

import (
	"IM2/internal/interceptor"
	"fmt"

	"github.com/zeromicro/go-zero/zrpc"
)

type RegisterRpcServicesFunc func(cfg any) (*zrpc.RpcServer, error)

type RpcService struct {
	registerRpcServices RegisterRpcServicesFunc
	*zrpc.RpcServer
}

func NewRpcService(registerRpcServices RegisterRpcServicesFunc) *RpcService {
	return &RpcService{
		registerRpcServices: registerRpcServices,
	}
}

func (rs *RpcService) Load(cfg any) error {
	if cfg == nil {
		return fmt.Errorf("config 不能为空")
	}
	s, err := rs.registerRpcServices(cfg)
	if err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}
	rs.RpcServer = s
	rs.RpcServer.AddUnaryInterceptors(interceptor.ServerErrorInterceptor)
	return nil
}

func (rs *RpcService) Start() error {
	if rs.RpcServer == nil {
		return fmt.Errorf("服务器未初始化，请先调用Load方法")
	}
	go func() {
		rs.RpcServer.Start()
	}()
	return nil
}

func (rs *RpcService) Stop() error {
	if rs.RpcServer != nil {
		rs.RpcServer.Stop()
	}
	return nil
}
