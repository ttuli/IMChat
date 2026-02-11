package member

import (
	"net/http"

	"IM2/internal/apps/Group/api/internal/logic/member"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 设置群成员角色
func SetMemberRoleHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.SetMemberRoleReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := member.NewSetMemberRoleLogic(r.Context(), svcCtx)
		err := l.SetMemberRole(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
