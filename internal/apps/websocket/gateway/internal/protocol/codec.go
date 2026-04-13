package protocol

import "IM2/pkg/proto/transport"

// Codec 消息编解码接口
type Codec interface {
	// Encode 编码 WSMessage 为字节
	Encode(msg *transport.WSMessage) ([]byte, error)
	// Decode 解码字节为 WSMessage
	Decode(data []byte) (*transport.WSMessage, error)
}
