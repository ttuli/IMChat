package svc

import (
	"IM2/internal/apps/Message/api/config"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config       config.Config
	MessageRpc   messagerpc.MessageRpc
	TokenManager *tokenmanager.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config:       c,
		MessageRpc:   messagerpc.NewMessageRpc(zrpc.MustNewClient(c.MessageRpc)),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
	}
}
