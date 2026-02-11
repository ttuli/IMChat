package xerr

import (
	"net/http"

	"google.golang.org/grpc/codes"
)

// ErrorCode 错误码类型
type ErrorCode int

const (
	// 通用错误码 1xxx
	ErrSuccess        ErrorCode = 0
	ErrUnknown        ErrorCode = 1000
	ErrInvalidParams  ErrorCode = 1001
	ErrNotFound       ErrorCode = 1002
	ErrPasswordError  ErrorCode = 1003
	ErrAlreadyExists  ErrorCode = 1004
	ErrForbidden      ErrorCode = 1005
	ErrUnauthorized   ErrorCode = 1006
	ErrInternalServer ErrorCode = 1007
	ErrTimeout        ErrorCode = 1008
	ErrServiceBusy    ErrorCode = 1009

	// 业务错误码 2xxx
	ErrDatabase      ErrorCode = 2001
	ErrCache         ErrorCode = 2002
	ErrRPC           ErrorCode = 2003
	ErrWebSocket     ErrorCode = 2004
	ErrEncoding      ErrorCode = 2005
	ErrDecoding      ErrorCode = 2006
	ErrTokenGenerate ErrorCode = 2007
	ErrAuthCodeError ErrorCode = 2008
	ErrInvalidIDType ErrorCode = 2009
)

var ErrorCodeToHTTPStatus = map[ErrorCode]int{
	// 通用错误码 1xxx
	ErrSuccess:        http.StatusOK,                  // 200
	ErrUnknown:        http.StatusInternalServerError, // 500
	ErrInvalidParams:  http.StatusBadRequest,          // 400
	ErrNotFound:       http.StatusNotFound,            // 404
	ErrPasswordError:  http.StatusBadRequest,          // 400
	ErrAlreadyExists:  http.StatusConflict,            // 409
	ErrForbidden:      http.StatusForbidden,           // 403
	ErrUnauthorized:   http.StatusUnauthorized,        // 401
	ErrInternalServer: http.StatusInternalServerError, // 500
	ErrTimeout:        http.StatusGatewayTimeout,      // 504
	ErrServiceBusy:    http.StatusServiceUnavailable,  // 503

	// 业务错误码 2xxx
	ErrDatabase:      http.StatusInternalServerError, // 500
	ErrCache:         http.StatusInternalServerError, // 500
	ErrRPC:           http.StatusBadGateway,          // 502
	ErrWebSocket:     http.StatusBadGateway,          // 502
	ErrEncoding:      http.StatusInternalServerError, // 500
	ErrDecoding:      http.StatusBadRequest,          // 400
	ErrTokenGenerate: http.StatusInternalServerError, // 401
	ErrAuthCodeError: http.StatusBadRequest,          // 400
	ErrInvalidIDType: http.StatusBadRequest,          // 400
}

func HTTPStatusFromErrorCode(code ErrorCode) int {
	if status, ok := ErrorCodeToHTTPStatus[code]; ok {
		return status
	}
	return http.StatusInternalServerError
}

func ToGRPCCode(e ErrorCode) codes.Code {
	switch e {
	case ErrSuccess:
		return codes.OK
	case ErrInvalidParams:
		return codes.InvalidArgument
	case ErrNotFound:
		return codes.NotFound
	case ErrAlreadyExists:
		return codes.AlreadyExists
	case ErrForbidden:
		return codes.PermissionDenied
	case ErrUnauthorized:
		return codes.Unauthenticated
	case ErrTimeout:
		return codes.DeadlineExceeded
	case ErrServiceBusy:
		return codes.Unavailable
	case ErrInternalServer, ErrDatabase, ErrCache, ErrEncoding, ErrDecoding:
		return codes.Internal
	case ErrRPC:
		return codes.Unknown
	default:
		return codes.Unknown
	}
}
