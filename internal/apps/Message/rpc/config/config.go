package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ListenerConfig struct {
	Url              string
	BroadcastSubject string
	DBSubject        string
}

type Config struct {
	zrpc.RpcServerConf

	Listener ListenerConfig

	DAO struct {
		MessageDAO struct {
			Dbsource string
		}
		ConversationDAO struct {
			Dbsource string
			Redisx   redis.RedisConf
		}
	}
}
