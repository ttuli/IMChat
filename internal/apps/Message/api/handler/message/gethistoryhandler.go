package message

import (
	"net/http"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取历史消息
func GetHistoryHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetHistoryReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := message.NewGetHistoryLogic(r.Context(), svcCtx)
		resp, err := l.GetHistory(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
