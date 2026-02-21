package member

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"

	"IM2/internal/apps/Group/api/internal/logic/member"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/pkg/resultx"
)

// 邀请用户加入群
func InviteMembersHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.InviteMembersReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := member.NewInviteMembersLogic(r.Context(), svcCtx)
		resp, err := l.InviteMembers(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
