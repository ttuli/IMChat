package resultx

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"IM2/pkg/proto/transport"
	"IM2/pkg/logger"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/protobuf/proto"
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

// OkProtoCtx 根据 Accept 头协商响应格式
// Accept 包含 application/x-protobuf → 返回 protobuf 二进制 (包装在 common.ApiResponse 中)
// 其他 → 返回 JSON（包裹在 Response 结构中）
func OkProtoCtx(ctx context.Context, w http.ResponseWriter, r *http.Request, msg proto.Message) {
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		data, err := proto.Marshal(msg)
		if err != nil {
			httpx.WriteJsonCtx(ctx, w, http.StatusInternalServerError, &Response{
				Code:    500,
				Message: "proto marshal failed",
			})
			return
		}

		apiResp := &transport.ApiResponse{
			Code:    200,
			Message: "success",
			Data:    data,
		}
		respData, err := proto.Marshal(apiResp)
		if err != nil {
			httpx.WriteJsonCtx(ctx, w, http.StatusInternalServerError, &Response{
				Code:    500,
				Message: "api response marshal failed",
			})
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)
		return
	}
	httpx.OkJsonCtx(ctx, w, msg)
}

// ErrorProtoCtx 根据 Accept 头协商错误响应格式
// Accept 包含 application/x-protobuf → 返回 protobuf 二进制 (包装在 transport.ApiResponse 中)
// 其他 → 返回 JSON
func ErrorProtoCtx(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	if !strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		// 回退到 JSON 错误处理
		var e *xerr.Error
		if customErr, ok := err.(*xerr.Error); ok {
			e = customErr
		} else {
			e = xerr.New(xerr.ErrUnknown, err.Error())
		}
		ErrorCtx(ctx, w, e)
		return
	}

	var xerrErr *xerr.Error
	if e, ok := err.(*xerr.Error); ok {
		xerrErr = e
	} else {
		xerrErr = xerr.New(xerr.ErrUnknown, err.Error())
	}

	httpCode := xerr.HTTPStatusFromErrorCode(xerrErr.Code)
	apiResp := &transport.ApiResponse{
		Code:    int32(httpCode),
		Message: xerrErr.Message,
		Data:    nil,
	}

	respData, marshalErr := proto.Marshal(apiResp)
	if marshalErr != nil {
		httpx.WriteJsonCtx(ctx, w, http.StatusInternalServerError, &Response{
			Code:    500,
			Message: "api response marshal failed",
		})
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(httpCode)
	w.Write(respData)
}
