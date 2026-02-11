package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf

	UserRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig

	Redisx redis.RedisConf
}
