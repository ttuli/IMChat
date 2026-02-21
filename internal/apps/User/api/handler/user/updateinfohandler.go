package user

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"

	"IM2/internal/apps/User/api/internal/logic/user"
	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/pkg/resultx"
)

// 更新个人信息
func UpdateInfoHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateInfoReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := user.NewUpdateInfoLogic(r.Context(), svcCtx)
		err := l.UpdateInfo(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			httpx.Ok(w)
		}
	}
}
