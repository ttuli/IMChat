package resultx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"

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

// OkJsonCtx 成功响应（带数据，带上下文）
func OkJsonCtx(ctx context.Context, w http.ResponseWriter, data interface{}) {
	resp := &Response{
		Code:    200,
		Message: "success",
		Data:    data,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		if logger.IsInitialized {
			logger.Error(fmt.Sprintf("json encode error: %v", err))
		}
	}
}

// ErrorJsonCtx 错误响应（带上下文）
func ErrorJsonCtx(ctx context.Context, w http.ResponseWriter, err *xerr.Error) {
	if err == nil {
		err = xerr.New(transport.ErrorCode_ERR_UNKNOWN, "未知错误")
	}

	resp := &Response{
		Code:    int(err.Code),
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

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(500)
	if e := json.NewEncoder(w).Encode(resp); e != nil {
		if logger.IsInitialized {
			logger.Error(fmt.Sprintf("json encode error: %v", e))
		}
	}
}

// OkProtoCtx 根据 Accept 头协商响应格式
// Accept 包含 application/x-protobuf → 返回 protobuf 二进制 (包装在 common.ApiResponse 中)
// 其他 → 返回 JSON（包裹在 Response 结构中）
// msg 可为 nil：无业务数据的操作类接口（移除成员/退群等）仅返回 code=200 信封，
// 客户端统一按 ApiResponse.code 判定成败（httpx.Ok 裸 200 空 body 会导致客户端解不出 code）
func OkProtoCtx(ctx context.Context, w http.ResponseWriter, r *http.Request, msg proto.Message) {
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		var data []byte
		var err error
		if msg != nil {
			data, err = proto.Marshal(msg)
		}
		if err != nil {
			ErrorJsonCtx(ctx, w, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "proto marshal failed"))
			return
		}

		apiResp := &transport.ApiResponse{
			Code:    200,
			Message: "success",
			Data:    data,
		}
		respData, err := proto.Marshal(apiResp)
		if err != nil {
			ErrorJsonCtx(ctx, w, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "api response marshal failed"))
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)
		return
	}
	OkJsonCtx(ctx, w, msg)
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
			e = xerr.New(transport.ErrorCode_ERR_UNKNOWN, err.Error())
		}
		ErrorJsonCtx(ctx, w, e)
		return
	}

	var xerrErr *xerr.Error
	if e, ok := err.(*xerr.Error); ok {
		xerrErr = e
	} else {
		xerrErr = xerr.New(transport.ErrorCode_ERR_UNKNOWN, err.Error())
	}

	apiResp := &transport.ApiResponse{
		Code:    int32(xerrErr.Code),
		Message: xerrErr.Message,
		Data:    nil,
	}
	if xerrErr.Details != nil {
		data, err := json.Marshal(xerrErr.Details)
		if err != nil {
			// 应该不会发生
			ErrorJsonCtx(ctx, w, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "proto marshal failed"))
			return
		}
		apiResp.Data = data
	}

	respData, marshalErr := proto.Marshal(apiResp)
	if marshalErr != nil {
		ErrorJsonCtx(ctx, w, xerr.New(transport.ErrorCode_ERR_INTERNAL_SERVER, "api response marshal failed"))
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(500)
	w.Write(respData)
}
