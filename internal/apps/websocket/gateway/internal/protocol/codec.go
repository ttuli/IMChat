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
	// EncodeInternal 编码内部消息 (用于跨节点通信)
	EncodeInternal(msg *common.InternalMessage) ([]byte, error)
	// DecodeInternal 解码内部消息
	DecodeInternal(data []byte) (*common.InternalMessage, error)
}
