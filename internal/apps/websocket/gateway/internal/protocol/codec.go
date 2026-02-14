package protocol

// Codec 消息编解码接口
type Codec interface {
	// Encode 编码消息
	Encode(msg *Message) ([]byte, error)
	// Decode 解码消息
	Decode(data []byte) (*Message, error)
	// EncodeInternal 编码内部消息 (用于跨节点通信)
	EncodeInternal(msg *InternalMessage) ([]byte, error)
	// DecodeInternal 解码内部消息 (用于跨节点通信)
	DecodeInternal(data []byte) (*InternalMessage, error)
}
