package resultx

import (
	"context"
	"fmt"
	"net/http"

	"IM2/pkg/logger"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// Response HTTP 统一响应结构
// 用于统一成功和错误的响应格式
type Response struct {
	Code    int                    `json:"code"`              // 错误码
	Message string                 `json:"message"`           // 响应消息
	Data    interface{}            `json:"data,omitempty"`    // 响应数据（成功时）
	Details map[string]interface{} `json:"details,omitempty"` // 错误详情（失败时）
}

// Success 成功响应
func Success(data interface{}) *Response {
	return &Response{
		Code:    200,
		Message: "success",
		Data:    data,
	}
}

// Failure 失败响应
func Failure(err *xerr.Error) (int, *Response) {
	httpCode := xerr.HTTPStatusFromErrorCode(err.Code)
	resp := &Response{
		Code:    httpCode,
		Message: err.Message,
	}
	if len(err.Details) > 0 {
		resp.Details = err.Details
	}
	if err.Err != nil {
		if logger.IsInitialized {
			logger.Error(err.Err.Error())
		} else if logger.Env == logger.LoggerEnvDev {
			fmt.Println(err.Err)
		}
	}
	return httpCode, resp
}

// OkJsonCtx 成功响应（带数据，带上下文）
func OkJsonCtx(ctx context.Context, w http.ResponseWriter, data interface{}) {
	httpx.OkJsonCtx(ctx, w, Success(data))
}

// ErrorCtx 错误响应（带上下文）
func ErrorCtx(ctx context.Context, w http.ResponseWriter, err *xerr.Error) {
	if err == nil {
		// 如果错误为nil，返回未知错误
		xerrErr := xerr.New(xerr.ErrUnknown, "未知错误")
		httpCode, resp := Failure(xerrErr)
		httpx.WriteJsonCtx(ctx, w, httpCode, resp)
		return
	}

	httpCode, resp := Failure(err)
	httpx.WriteJsonCtx(ctx, w, httpCode, resp)
}

// OkHandler go-zero httpx.SetOkHandler 使用的处理函数
// 用于统一处理所有成功响应
func OkHandler(ctx context.Context, data any) any {
	return Success(data)
}

// ErrorHandler go-zero 错误处理函数
// 用于统一处理所有错误响应
func ErrorHandler(ctx context.Context, err error) (int, any) {
	if e, ok := err.(*xerr.Error); ok {
		return Failure(e)
	}
	return Failure(xerr.New(xerr.ErrUnknown, "unknown error"))
}
