package middleware

import (
	"fmt"
	"net/http"

	"IM2/pkg/appversion"
	"IM2/pkg/proto/transport"
	"IM2/pkg/resultx"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
)

// MinVersionProvider 提供当前生效的最低客户端版本，零值表示不限制
type MinVersionProvider interface {
	MinVersion() appversion.Version
}

// WithAppVersionGate 客户端最低版本拦截。
//
// 请求头 X-App-Version 低于最低版本时直接返回 HTTP 426（Upgrade Required），
// 客户端据状态码进入强制更新。没带请求头的按 0.0.0 处理：那是没有更新功能的旧版客户端，同样要拦。
//
// 响应体仍是常规的 ApiResponse 信封：message 是给旧版客户端原样展示的提示文案，
// 最低版本放在 details.min_version（protobuf 时序列化进 data）。
// downloadURL 非空时拼进提示文案，旧版客户端没有更新窗口，只能靠这段文字找到新版。
func WithAppVersionGate(provider MinVersionProvider, downloadURL string) rest.Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			minVersion := provider.MinVersion()
			if minVersion.IsZero() {
				next(w, r)
				return
			}

			raw := r.Header.Get(appversion.Header)
			current, _ := appversion.Parse(raw) // 缺失或非法 → 零值，视为最旧的客户端
			if !current.Less(minVersion) {
				next(w, r)
				return
			}

			logx.WithContext(r.Context()).Infof("[AppVersionGate] 拒绝过低版本客户端: %q < %s, path=%s", raw, minVersion, r.URL.Path)

			message := fmt.Sprintf("当前版本过低，请更新到 %s 或更高版本后再使用", minVersion)
			if downloadURL != "" {
				message += "，下载地址：" + downloadURL
			}
			err := xerr.New(transport.ErrorCode(http.StatusUpgradeRequired), message).
				WithDetail("min_version", minVersion.String())
			resultx.ErrorProtoStatusCtx(r.Context(), w, r, http.StatusUpgradeRequired, err)
		}
	}
}
