package direct

import (
	"net/http"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/pkg/resultx"
)

// VersionHandler 客户端启动时的版本探测。
// 本身不做任何事：版本过低时前置的版本中间件已直接返回 426，能走到这里说明版本满足要求。
func VersionHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resultx.OkProtoCtx(r.Context(), w, r, nil)
	}
}
