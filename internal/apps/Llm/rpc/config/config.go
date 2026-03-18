package config

import (
	"github.com/zeromicro/go-zero/zrpc"
)

// LlmProviderConfig OpenAI 兼容提供商配置
type LlmProviderConfig struct {
	BaseURL string // e.g. https://api.deepseek.com
	ApiKey  string // Bearer token
	Model   string
	Prompt  string // 默认提示词
	Timeout int // 默认 60s
}

type Config struct {
	zrpc.RpcServerConf
	Llm struct {
		SuggestLlmProvider LlmProviderConfig
	}
}
