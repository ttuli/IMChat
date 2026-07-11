package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	// WebSocket 相关配置
	WebSocket WebSocketConf

	// Redis 配置 (用于路由 KV 存储)
	RouteStore redis.RedisConf

	// NATS 配置 (用于跨节点消息转发)
	Nats struct {
		Url string `json:"url"`
	}

	// token 配置
	TokenConfig tokenmanager.TokenConfig
}


// WebSocketConf WebSocket 配置
type WebSocketConf struct {
	// 节点ID，为空时自动生成
	NodeID string `json:",optional"`
	// WebSocket 路径
	Path string
	// 读缓冲区大小
	ReadBufferSize int
	// 写缓冲区大小
	WriteBufferSize int
	// 协议版本
	Version int32
}
