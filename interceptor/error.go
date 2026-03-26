package interceptor

import (
	"IM2/internal/common"
	"IM2/pkg/xerr"
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 服务端错误拦截器
func ServerErrorInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	resp, err = handler(ctx, req)
	if err != nil {
		if cusErr, ok := err.(*xerr.Error); ok {
			st := status.New(xerr.ToGRPCCode(cusErr.Code), cusErr.Message)
			con := &common.ErrorResp{
				Code:    int32(cusErr.Code),
				Message: cusErr.Message,
			}
			if cusErr.Err != nil {
				con.RawError = cusErr.Err.Error()
			}
			st, err = st.WithDetails(con)
			if err != nil {
				return resp, status.Error(codes.Internal, err.Error())
			}
			return resp, st.Err()
		}
		return resp, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

// 客户端错误拦截器:解析并返回服务端包装的xerr
func ClientErrorInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return xerr.Wrap(err, xerr.ErrInternalServer, "服务器内部错误")
		} else {
			for _, detail := range st.Details() {
				if cusErr, ok := detail.(*common.ErrorResp); ok {
					if cusErr.RawError != "" {
						return xerr.Wrap(errors.New(cusErr.RawError), xerr.ErrorCode(cusErr.Code), cusErr.Message)
					} else {
						return xerr.Wrap(errors.New(st.Message()), xerr.ErrorCode(cusErr.Code), cusErr.Message)
					}
				}
			}
			return xerr.Wrap(errors.New(st.Message()), xerr.ErrInternalServer, "服务器内部错误")
		}
	}
	return nil
}

// 客户端错误拦截器:从服务端包装的xerr中提取原始错误并返回
func ClientPureErrorInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return err
		} else {
			for _, detail := range st.Details() {
				if cusErr, ok := detail.(*common.ErrorResp); ok {
					if cusErr.RawError != "" {
						return errors.New(cusErr.RawError)
					} else {
						return st.Err()
					}
				}
			}
			return st.Err()
		}
	}
	return nil
}
