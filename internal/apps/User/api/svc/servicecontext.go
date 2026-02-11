package svc

import (
	"IM2/interceptor"
	"IM2/internal/apps/User/api/config"
	"IM2/internal/apps/User/rpc/userclient"
	tokenmanager "IM2/pkg/tokenManager"

	// "IM2/pkg/redisc"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	userclient.User
	*tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		User: userclient.NewUser(zrpc.MustNewClient(c.UserRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
