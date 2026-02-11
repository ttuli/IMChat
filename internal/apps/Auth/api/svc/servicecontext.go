package svc

import (
	"IM2/interceptor"
	"IM2/internal/apps/Auth/api/config"
	"IM2/internal/apps/Auth/rpc/client/authrpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config  config.Config
	AuthRpc authrpc.AuthRpc
	*tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		AuthRpc: authrpc.NewAuthRpc(zrpc.MustNewClient(c.AuthRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
