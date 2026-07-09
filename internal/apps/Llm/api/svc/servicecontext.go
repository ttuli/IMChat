package svc

import (
	"IM2/internal/interceptor"
	"IM2/internal/apps/Llm/api/config"
	"IM2/internal/apps/Llm/rpc/client/llm"
	tokenmanager "IM2/pkg/tokenManager"
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	LlmRpc llm.Llm
	*tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		LlmRpc: llm.NewLlm(zrpc.MustNewClient(c.LlmRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor),
			zrpc.WithTimeout(time.Duration(c.Timeout)*time.Millisecond)),
		),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
