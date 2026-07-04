package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/pkg/resultx"
)

// 获取历史消息
func GetUserActiveSessionsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetUserActiveSessionsReq
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := message.NewGetUserActiveSessionsLogic(r.Context(), svcCtx)
		resp, err := l.GetUserActiveSessions(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
