package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	
	AuthRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig

	// AppVersion 客户端最低版本门槛，可不配。
	// 最低版本本身存放在 etcd 的独立 key 中并实时监听，调整时改 key 即可，不需要重启服务
	AppVersion AppVersionConf `json:",optional"`
}

type AppVersionConf struct {
	// Key 存放最低版本号的 etcd key，默认 appversion.DefaultKey（config/app.min_version）
	Key string `json:",optional"`
	// DownloadURL 官网下载地址，拼进 426 提示文案，给没有更新功能的旧版客户端看
	DownloadURL string `json:",optional"`
}
