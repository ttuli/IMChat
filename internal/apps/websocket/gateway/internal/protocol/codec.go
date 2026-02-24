package protocol

import (
	"IM2/internal/common"
)

// Codec 消息编解码接口
type Codec interface {
	// Encode 编码 WSMessage 为字节
	Encode(msg *common.WSMessage) ([]byte, error)
	// Decode 解码字节为 WSMessage
	Decode(data []byte) (*common.WSMessage, error)
}
