package direct

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"

	logic "IM2/internal/apps/Auth/api/internal/logic/direct"
	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/pkg/resultx"
)

func GetAuthCodeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetAuthCodeReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := logic.NewGetAuthCodeLogic(r.Context(), svcCtx)
		resp, err := l.GetAuthCode(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
