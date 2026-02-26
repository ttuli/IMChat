package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/pkg/resultx"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取历史消息
func GetUserActiveConversationsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetUserActiveConversationsReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := message.NewGetUserActiveConversationsLogic(r.Context(), svcCtx)
		resp, err := l.GetUserActiveConversations(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
