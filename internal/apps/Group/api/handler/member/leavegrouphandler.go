package member

import (
	"net/http"

	"IM2/internal/apps/Group/api/internal/logic/member"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 退出群聊
func LeaveGroupHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.LeaveGroupReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := member.NewLeaveGroupLogic(r.Context(), svcCtx)
		err := l.LeaveGroup(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
