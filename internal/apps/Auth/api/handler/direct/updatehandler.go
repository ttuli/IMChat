package direct

import (
	"net/http"
	"net/url"

	"IM2/internal/apps/Auth/api/svc"

	"github.com/zeromicro/go-zero/rest/pathvar"
)

// UpdateHandler 客户端自动更新的更新源入口：/auth/update/<文件名> 302 到 AppVersion.UpdateFeedURL 下的同名文件。
//
// 客户端打包时写死的 publish.url 指向这里而不是 OSS，存储位置只在服务端配置：
// 换 bucket、加 CDN、换云厂商都只改配置，已经装出去的客户端照常更新。
// electron-updater 查询 latest.yml、下载安装包和 .blockmap 都会跟随 302，
// 增量下载只有第一段经过这里，之后的分段请求直接发往跳转后的地址。
func UpdateHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svcCtx.ReleaseFeed == nil {
			http.Error(w, "更新源尚未配置", http.StatusNotFound)
			return
		}

		target, err := svcCtx.ReleaseFeed.FileURL(pathvar.Vars(r)["file"])
		if err != nil {
			http.NotFound(w, r)
			return
		}
		// electron-updater 查 latest.yml 时带随机的 noCache 参数防缓存，带到存储端，途经的 CDN 才不会返回缓存的旧清单。
		// 其他参数一律丢弃，免得被拿去拼 OSS 的特殊参数
		if v := r.URL.Query().Get("noCache"); v != "" {
			target += "?" + url.Values{"noCache": {v}}.Encode()
		}

		// 跳转目标随服务端配置变化，不能让客户端或途经的代理缓存这次跳转
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, target, http.StatusFound)
	}
}
