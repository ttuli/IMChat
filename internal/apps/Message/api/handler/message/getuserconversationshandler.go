package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取用户的会话列表(根据UserID)
func GetUserConversationsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := message.NewGetUserConversationsLogic(r.Context(), svcCtx)
		resp, err := l.GetUserConversations()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
