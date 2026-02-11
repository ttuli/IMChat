package apply

import (
	"net/http"

	"IM2/internal/apps/Group/api/internal/logic/apply"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取待处理的群申请
func GetPendingAppliesHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetPendingAppliesReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := apply.NewGetPendingAppliesLogic(r.Context(), svcCtx)
		resp, err := l.GetPendingApplies(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
