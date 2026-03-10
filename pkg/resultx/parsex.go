package resultx

import (
	"io"
	"net/http"
	"strings"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/protobuf/proto"
)

// ParseProto 通用请求解析器
// Content-Type 为 application/x-protobuf → 使用 protobuf 反序列化
// 其他 → 使用 go-zero httpx.Parse 解析（支持 JSON / Form / Query），JSON 兜底
//
// msg 必须同时实现 proto.Message 接口（protobuf 生成的 struct 均满足）
func ParseProto(r *http.Request, msg proto.Message) error {
	// GET / DELETE 请求通常参数在 URL 中，没有 Body，直接用 httpx.Parse（支持 form/path/query 解析）
	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		return httpx.Parse(r, msg)
	}

	if strings.Contains(r.Header.Get("Content-Type"), "application/x-protobuf") {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		return proto.Unmarshal(body, msg)
	}
	// JSON / Form 兜底（go-zero 原生逻辑）
	return httpx.Parse(r, msg)
}
