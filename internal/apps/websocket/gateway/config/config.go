package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf

	// WebSocket 相关配置
	WebSocket WebSocketConf

	// Redis 配置
	Redis redis.RedisConf

	// RPC 客户端配置
	AuthRpc zrpc.RpcClientConf
	UserRpc zrpc.RpcClientConf
}

// WebSocketConf WebSocket 配置
type WebSocketConf struct {
	// 节点ID，为空时自动生成
	NodeID string `json:",optional"`
	// 节点地址，格式 host:port
	NodeAddr string `json:",optional"`
	// WebSocket 路径
	Path string `json:",default=/ws"`
	// 读缓冲区大小
	ReadBufferSize int `json:",default=4096"`
	// 写缓冲区大小
	WriteBufferSize int `json:",default=4096"`
}
