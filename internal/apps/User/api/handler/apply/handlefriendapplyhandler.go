package apply

import (
	"net/http"

	"IM2/internal/apps/User/api/internal/logic/apply"
	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 处理好友申请
func HandleFriendApplyHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.HandleFriendApplyReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := apply.NewHandleFriendApplyLogic(r.Context(), svcCtx)
		err := l.HandleFriendApply(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
