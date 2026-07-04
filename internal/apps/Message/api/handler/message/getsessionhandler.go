package message

import (
	"net/http"

	message "IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/pkg/resultx"
)

// 根据 session_id 或 session_key 查询（或创建）会话
func GetSessionHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetSessionReq
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := message.NewGetSessionLogic(r.Context(), svcCtx)
		resp, err := l.GetSession(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
