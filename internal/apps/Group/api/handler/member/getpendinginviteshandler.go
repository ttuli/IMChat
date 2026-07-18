package member

import (
	"net/http"

	"IM2/internal/apps/Group/api/internal/logic/member"
	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/pkg/resultx"
)

// 获取我收到的待处理入群邀请
func GetPendingInvitesHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetPendingInvitesReq
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := member.NewGetPendingInvitesLogic(r.Context(), svcCtx)
		resp, err := l.GetPendingInvites(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
