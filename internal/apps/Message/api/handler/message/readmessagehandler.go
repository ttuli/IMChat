package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 消息已读上报
func ReadMessageHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ReadMessageReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := message.NewReadMessageLogic(r.Context(), svcCtx)
		err := l.ReadMessage(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
