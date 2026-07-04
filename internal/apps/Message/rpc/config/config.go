package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ListenerConfig struct {
	Url              string
	BroadcastSubject string
	DBSubject        string
	AckSubject       string
	DLQSubject       string

	MaxDeliver int
}

type Config struct {
	zrpc.RpcServerConf

	Listener ListenerConfig

	// SnowflakeNodeID 本地雪花节点 ID，0-1023，不填时自动从 hostname 派生
	SnowflakeNodeID int64 `json:",optional"`

	DAO struct {
		MessageDAO struct {
			Dbsource string
		}
		SessionDAO struct {
			Dbsource string
			Redisx   redis.RedisConf
		}
	}
}
