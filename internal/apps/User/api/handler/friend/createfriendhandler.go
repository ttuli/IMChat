package friend

import (
	"net/http"

	"IM2/internal/apps/User/api/internal/logic/friend"
	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 创建好友
func CreateFriendHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.CreateFriendReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := friend.NewCreateFriendLogic(r.Context(), svcCtx)
		err := l.CreateFriend(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
