package llm

import (
	"net/http"

	"IM2/internal/apps/Llm/api/internal/logic/llm"
	"IM2/internal/apps/Llm/api/svc"
	"IM2/internal/apps/Llm/api/types"
	"IM2/pkg/resultx"
)

func SuggestHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.SuggestRequest
		if err := resultx.ParseProto(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := llm.NewSuggestLogic(r.Context(), svcCtx)
		resp, err := l.Suggest(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}
		resultx.OkProtoCtx(r.Context(), w, r, resp)
	}
}
