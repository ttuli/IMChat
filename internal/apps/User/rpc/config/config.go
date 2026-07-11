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
		Url string
	}

	// RouteStore 集群路由表 Redis（须与网关/Message 服务指向同一实例）。
	// 缺省时复用 UserDAO.RedisSource。
	RouteStore redis.RedisConf `json:",optional"`

	IDRpc      zrpc.RpcClientConf
	MessageRpc zrpc.RpcClientConf
}
