package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ListenerConfig struct {
	Url               string
	NodeSubjectPrefix string
	DBSubject         string
	DBAddress         string
}

type Config struct {
	zrpc.RpcServerConf
	Redisx redis.RedisConf

	Listener ListenerConfig

	DAO struct {
		ConversationTable string
	}
}
