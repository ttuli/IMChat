package protocol

import (
	"IM2/internal/apps/websocket/gateway/types"
)

// Codec 消息编解码接口
type Codec interface {
	// Encode 编码 WSMessage 为字节
	Encode(msg *types.WSMessage) ([]byte, error)
	// Decode 解码字节为 WSMessage
	Decode(data []byte) (*types.WSMessage, error)
	// EncodeInternal 编码内部消息 (用于跨节点通信)
	EncodeInternal(msg *types.InternalMessage) ([]byte, error)
	// DecodeInternal 解码内部消息
	DecodeInternal(data []byte) (*types.InternalMessage, error)
}
