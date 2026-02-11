package xerr

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Error 统一错误结构
type Error struct {
	Code    ErrorCode              `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
	Err     error                  `json:"-"` // 原始错误,不序列化
}

// Error 实现 error 接口
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%d] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// Unwrap 支持 errors.Unwrap
func (e *Error) Unwrap() error {
	return e.Err
}

// WithDetail 添加详细信息
func (e *Error) WithDetail(key string, value interface{}) *Error {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// WithError 包装原始错误
func (e *Error) WithError(err error) *Error {
	e.Err = err
	return e
}

// New 创建新错误
func New(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

// Wrap 包装已有错误
func Wrap(err error, code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// HTTPStatus 返回对应的 HTTP 状态码
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case ErrSuccess:
		return http.StatusOK
	case ErrInvalidParams:
		return http.StatusBadRequest
	case ErrNotFound:
		return http.StatusNotFound
	case ErrAlreadyExists:
		return http.StatusConflict
	case ErrForbidden:
		return http.StatusForbidden
	case ErrUnauthorized:
		return http.StatusUnauthorized
	case ErrTimeout:
		return http.StatusRequestTimeout
	case ErrServiceBusy:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// ToJSON 转换为 JSON
func (e *Error) ToJSON() []byte {
	data, _ := json.Marshal(e)
	return data
}

// Reset 实现 protoiface.MessageV1 接口
// 重置所有字段为零值
func (e *Error) Reset() {
	e.Code = 0
	e.Message = ""
	e.Details = nil
	e.Err = nil
}

// String 实现 protoiface.MessageV1 接口
// 返回错误的字符串表示
func (e *Error) String() string {
	return e.Error()
}

// ProtoMessage 实现 protoiface.MessageV1 接口
// 标记方法，用于标识这是一个 protobuf 消息
func (e *Error) ProtoMessage() {}
