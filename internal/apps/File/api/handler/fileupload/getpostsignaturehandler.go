package fileupload

import (
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/fileupload"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取签名
func GetPostSignatureHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetPostSignatureReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := fileupload.NewGetPostSignatureLogic(r.Context(), svcCtx)
		resp, err := l.GetPostSignature(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
