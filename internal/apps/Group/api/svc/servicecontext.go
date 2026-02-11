package svc

import (
	"IM2/interceptor"
	"IM2/internal/apps/Group/api/config"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config   config.Config
	GroupRpc grouprpc.GroupRpc
	*tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		GroupRpc: grouprpc.NewGroupRpc(zrpc.MustNewClient(c.GroupRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
