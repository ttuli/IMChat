package protocol


// Codec 消息编解码接口
type Codec interface {
	// Encode 编码任意结构为字节
	Encode(v any) ([]byte, error)
	// Decode 解码字节为任意结构
	Decode(data []byte, v any) error
}
