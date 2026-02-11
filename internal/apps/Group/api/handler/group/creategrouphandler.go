package group

import (
	"net/http"

	"IM2/internal/apps/Group/api/internal/logic/group"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 创建群组
func CreateGroupHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.CreateGroupReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := group.NewCreateGroupLogic(r.Context(), svcCtx)
		resp, err := l.CreateGroup(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
