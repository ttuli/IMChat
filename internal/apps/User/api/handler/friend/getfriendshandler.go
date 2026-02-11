package friend

import (
	"net/http"

	"IM2/internal/apps/User/api/internal/logic/friend"
	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取好友列表
func GetFriendsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetFriendsReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := friend.NewGetFriendsLogic(r.Context(), svcCtx)
		resp, err := l.GetFriends(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
