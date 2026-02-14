package config

import (
	"IM2/pkg/service"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	// WebSocket 相关配置
	WebSocket WebSocketConf

	// Redis 配置 (用于路由 KV 存储)
	Redis redis.RedisConf

	// NATS 配置 (用于跨节点消息转发)
	Nats NatsConf

	// token 配置
	TokenConfig tokenmanager.TokenConfig

	APISIX service.APISIXConfig
}

// NatsConf NATS 配置
type NatsConf struct {
	// NATS 服务器地址，例如 nats://localhost:4222
	Url string `json:"url"`
}

// WebSocketConf WebSocket 配置
type WebSocketConf struct {
	// 节点ID，为空时自动生成
	NodeID string `json:",omitempty"`
	// WebSocket 路径
	Path string `json:"path"`
	// 读缓冲区大小
	ReadBufferSize int `json:",omitempty"`
	// 写缓冲区大小
	WriteBufferSize int `json:",omitempty"`
}
