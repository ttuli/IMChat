package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/pkg/resultx"
)

// 获取用户的会话列表(根据UserID)
func GetUserSessionsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := message.NewGetUserSessionsLogic(r.Context(), svcCtx)
		resp, err := l.GetUserSessions()
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
