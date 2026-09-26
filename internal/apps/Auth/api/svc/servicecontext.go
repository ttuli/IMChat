package svc

import (
	"IM2/internal/apps/Auth/api/config"
	"IM2/internal/apps/Auth/rpc/client/authrpc"
	"IM2/internal/interceptor"
	"IM2/pkg/appversion"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config  config.Config
	AuthRpc authrpc.AuthRpc
	*tokenmanager.TokenManager
	// VersionGate 客户端最低版本门槛；为 nil 时不限制（etcd 不可用或未配置）
	VersionGate *appversion.Gate
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		AuthRpc: authrpc.NewAuthRpc(zrpc.MustNewClient(c.AuthRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
