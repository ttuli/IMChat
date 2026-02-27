package fileupload

import (
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/fileupload"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"

	"github.com/zeromicro/go-zero/rest/httpx"

	"IM2/pkg/resultx"
)

// 获取签名
func GetFileAccessUrlHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetAccessUrlReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := fileupload.NewGetAccessUrlLogic(r.Context(), svcCtx)
		resp, err := l.GetAccessUrlLogic(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
