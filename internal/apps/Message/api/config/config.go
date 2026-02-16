package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	MessageRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig
}
