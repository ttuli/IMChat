package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取会话列表
func GetConversationListHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetConversationListReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := message.NewGetConversationListLogic(r.Context(), svcCtx)
		resp, err := l.GetConversationList(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
