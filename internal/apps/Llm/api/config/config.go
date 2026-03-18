package config

import (
	"IM2/pkg/service"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf

	LlmRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig

	APISIX service.APISIXConfig
}
