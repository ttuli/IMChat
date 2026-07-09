package svc

import (
	"IM2/internal/apps/User/api/config"
	"IM2/internal/apps/User/rpc/client/user"
	"IM2/internal/interceptor"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	user.User
	*tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		User: user.NewUser(zrpc.MustNewClient(c.UserRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
