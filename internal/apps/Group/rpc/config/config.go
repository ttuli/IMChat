package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf

	DAO struct {
		GroupDAO struct {
			DataSource  string
			RedisSource redis.RedisConf
		}
		ApplyDAO string
	}

	IDRpc zrpc.RpcClientConf
}
