package callback

import (
	"bytes"
	"io"
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/callback"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"
	"IM2/pkg/logger"
	"IM2/pkg/resultx"

	"go.uber.org/zap"
)

// 上传回调
func UploadCallbackHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := verifyOssCallback(r)
		if err != nil {
			if logger.IsInitialized {
				logger.Warn("oss upload callback signature verify failed",
					zap.String("remote", r.RemoteAddr),
					zap.Error(err),
				)
			}
			http.Error(w, "invalid oss callback signature", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		var req types.CallbackData
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := callback.NewUploadCallbackLogic(r.Context(), svcCtx)
		err = l.UploadCallback(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"Status":"OK"}`))
		}
	}
}
