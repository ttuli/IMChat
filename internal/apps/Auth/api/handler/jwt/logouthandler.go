package jwt

import (
	"net/http"

	logic "IM2/internal/apps/Auth/api/internal/logic/jwt"
	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func LogoutHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.LogoutReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewLogoutLogic(r.Context(), svcCtx)
		err := l.Logout(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
