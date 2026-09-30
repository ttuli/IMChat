package direct

import (
	"net/http"

	"IM2/internal/apps/Auth/api/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

// DownloadHandler 客户端下载入口：302 跳转到更新源上的最新安装包。
//
// 这是给人用的固定地址（426 提示文案里的下载地址、客户端「前往官网下载」都指向这里），
// 不随版本变化：安装包取自更新源的 latest.yml，发版后自动指向新包；换存储只改 AppVersion.UpdateFeedURL。
// 由浏览器直接打开，出错时回纯文本而不是 ApiResponse。
func DownloadHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svcCtx.ReleaseFeed == nil {
			http.Error(w, "下载地址尚未配置", http.StatusNotFound)
			return
		}

		target, err := svcCtx.ReleaseFeed.InstallerURL(r.Context())
		if err != nil {
			logx.WithContext(r.Context()).Errorf("[Download] 获取最新安装包地址失败: %v", err)
			http.Error(w, "暂时无法获取安装包，请稍后再试", http.StatusServiceUnavailable)
			return
		}

		// 跳转目标随发版变化，不能让浏览器或途经的代理缓存这次跳转
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, target, http.StatusFound)
	}
}
