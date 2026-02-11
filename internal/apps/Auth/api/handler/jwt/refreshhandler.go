package jwt

import (
	"net/http"

	logic "IM2/internal/apps/Auth/api/internal/logic/jwt"
	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func RefreshHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.RefreshReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, xerr.New(xerr.ErrInvalidParams, "参数错误"))
			return
		}

		req.RefreshToken = tokenmanager.ExtractToken(r)
		l := logic.NewRefreshLogic(r.Context(), svcCtx)
		resp, err := l.Refresh(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
