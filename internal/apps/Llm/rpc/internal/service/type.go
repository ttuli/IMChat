package service

import "context"

// ===== 通用数据类型 =====

// Role 消息角色
type Role uint8

const (
	Role_ROLE_ME     Role = 0
	Role_ROLE_OTHER  Role = 1
	Role_ROLE_SYSTEM Role = 2
)

// Message 对话消息
type Message struct {
	Role    Role
	Content string
}

// ChunkCallback 流式块回调
// text 是本次 chunk 的文本；done 为 true 时表示生成结束
type ChunkCallback func(text string, done bool) error

// ===== 服务接口 =====

type LlmServiceType int8

const (
	LlmServiceType_Suggest LlmServiceType = 0
)

// LlmService 封装底层 LLM 调用
type LlmService interface {
	// ChatStream 发起对话，并通过 cb 逐块回调生成的文本。
	// 如果调用方希望单次返回，可在 cb 中累积文本，等 done==true 后一次性使用。
	Suggestions(ctx context.Context, messages []Message) ([]string, error)
}
