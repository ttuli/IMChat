package callback

import (
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/callback"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"
	"IM2/pkg/resultx"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// 上传回调
func UploadCallbackHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.CallbackData
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := callback.NewUploadCallbackLogic(r.Context(), svcCtx)
		err := l.UploadCallback(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			httpx.Ok(w)
		}
	}
}
