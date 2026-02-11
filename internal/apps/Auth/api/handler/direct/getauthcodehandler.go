package direct

import (
	"net/http"

	"IM2/internal/apps/Auth/api/internal/logic/direct"
	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func GetAuthCodeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetAuthCodeReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewGetAuthCodeLogic(r.Context(), svcCtx)
		resp, err := l.GetAuthCode(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
