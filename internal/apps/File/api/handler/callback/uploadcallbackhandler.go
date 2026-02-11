package callback

import (
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/callback"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// 上传回调
func UploadCallbackHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.CallbackData
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := callback.NewUploadCallbackLogic(r.Context(), svcCtx)
		err := l.UploadCallback(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
