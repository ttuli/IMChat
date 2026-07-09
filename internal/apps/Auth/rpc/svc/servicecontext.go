package svc

import (
	"IM2/internal/apps/Auth/rpc/config"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/internal/interceptor"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config      config.Config
	AuthService *service.AuthService
}

func NewServiceContext(c config.Config) *ServiceContext {
	idGenerator := idgenclient.NewIdgen(zrpc.MustNewClient(c.IDRpc,
		zrpc.WithUnaryClientInterceptor(interceptor.ClientPureErrorInterceptor)))
	return &ServiceContext{
		Config:      c,
		AuthService: service.NewAuthService(c, idGenerator),
	}
}
