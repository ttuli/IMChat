package service



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

// Removed LlmService interface
