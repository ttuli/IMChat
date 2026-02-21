package user

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"

	"IM2/internal/apps/User/api/internal/logic/user"
	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/pkg/resultx"
)

// 搜索好友
func SearchFriendHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.SearchFriendReq
		if err := httpx.Parse(r, &req); err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
			return
		}

		l := user.NewSearchFriendLogic(r.Context(), svcCtx)
		resp, err := l.SearchFriend(&req)
		if err != nil {
			resultx.ErrorProtoCtx(r.Context(), w, r, err)
		} else {
			resultx.OkProtoCtx(r.Context(), w, r, resp)
		}
	}
}
