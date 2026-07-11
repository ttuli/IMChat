package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ListenerConfig struct {
	Url              string
	// FetchBatchSize 单次从 JetStream 批量拉取的消息数，默认 32
	FetchBatchSize int `json:",optional"`
	// Workers 消费并行度（按会话哈希分区，同会话串行、跨会话并行），默认 8
	Workers int `json:",optional"`

	MaxDeliver int
}

type MessageDAOConfig struct {
	Dbsource string
	// UnreadCountLimit 单次未读计数的扫描上限
	UnreadCountLimit int64
}

type SessionDAOConfig struct {
	Dbsource string
	Redisx   redis.RedisConf
}

type Config struct {
	zrpc.RpcServerConf

	Listener ListenerConfig

	// GroupRpc 群服务客户端：群消息扇出时获取权威成员列表
	GroupRpc zrpc.RpcClientConf

	RouteStore redis.RedisConf

	// SnowflakeNodeID 本地雪花节点 ID，0-1023，不填时自动从 hostname 派生
	SnowflakeNodeID int64 `json:",optional"`

	DAO struct {
		MessageDAO MessageDAOConfig
		SessionDAO SessionDAOConfig
	}
}
