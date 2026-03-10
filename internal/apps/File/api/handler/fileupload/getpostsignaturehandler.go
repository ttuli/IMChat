package fileupload

import (
	"net/http"

	"IM2/internal/apps/File/api/internal/logic/fileupload"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"

	"IM2/pkg/resultx"
)

// 获取签名
func GetPostSignatureHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetPostSignatureReq
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := fileupload.NewGetPostSignatureLogic(r.Context(), svcCtx)
		resp, err := l.GetPostSignature(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
