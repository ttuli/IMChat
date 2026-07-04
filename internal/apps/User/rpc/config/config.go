package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf

	DAO struct {
		UserDAO struct {
			DataSource  string
			RedisSource redis.RedisConf
		}
		FriendDAO      string
		FriendApplyDAO string
	}

	NATS struct {
		Url                   string
		BroadcastSubject      string
		QueueBroadcastSubject string
	}

	IDRpc      zrpc.RpcClientConf
	MessageRpc zrpc.RpcClientConf
}
