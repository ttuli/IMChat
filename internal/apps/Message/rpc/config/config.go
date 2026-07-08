package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ListenerConfig struct {
	Url              string
	BroadcastSubject string
	DBSubject        string
	DLQSubject       string

	// NodeSubjectPrefix 网关节点专属 subject 前缀（须与网关 Nats.NodeSubjectPrefix 一致）。
	// 用于 ACK / 单聊消息的精准节点投递；为空时退化为 BroadcastSubject 全节点广播。
	NodeSubjectPrefix string `json:",optional"`

	// FetchBatchSize 单次从 JetStream 批量拉取的消息数，默认 32
	FetchBatchSize int `json:",optional"`
	// Workers 消费并行度（按会话哈希分区，同会话串行、跨会话并行），默认 8
	Workers int `json:",optional"`

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
