package message

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"

	"IM2/internal/apps/Message/api/internal/logic/message"
	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/pkg/resultx"
)

// 上报会话已读游标
func MarkSessionReadHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.MarkSessionReadReq
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := message.NewMarkSessionReadLogic(r.Context(), svcCtx)
		err := l.MarkSessionRead(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			httpx.Ok(w)
		}
	}
}
