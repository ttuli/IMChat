package svc

import (
	"time"

	"IM2/internal/apps/Auth/api/config"
	"IM2/internal/apps/Auth/rpc/client/authrpc"
	"IM2/internal/interceptor"
	"IM2/pkg/appversion"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

// releaseFeedTTL 最新安装包地址的缓存时长，即发版后下载入口最多延迟这么久切到新包
const releaseFeedTTL = time.Minute

type ServiceContext struct {
	Config  config.Config
	AuthRpc authrpc.AuthRpc
	*tokenmanager.TokenManager
	// VersionGate 客户端最低版本门槛；为 nil 时不限制（etcd 不可用或未配置）
	VersionGate *appversion.Gate
	// ReleaseFeed 下载入口读取的更新源；为 nil 时 /auth/download 返回 404（未配置或地址无效）
	ReleaseFeed *appversion.ReleaseFeed
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		AuthRpc: authrpc.NewAuthRpc(zrpc.MustNewClient(c.AuthRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),
		ReleaseFeed:  newReleaseFeed(c.AppVersion.UpdateFeedURL),
	}
}

// newReleaseFeed 未配置更新源时返回 nil。地址写错只记日志：下载入口不能成为服务起不来的原因
func newReleaseFeed(feedURL string) *appversion.ReleaseFeed {
	if feedURL == "" {
		return nil
	}
	feed, err := appversion.NewReleaseFeed(feedURL, releaseFeedTTL)
	if err != nil {
		logx.Errorf("[Auth] 未启用下载入口: %v", err)
		return nil
	}
	return feed
}
