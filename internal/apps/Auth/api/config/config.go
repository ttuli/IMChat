package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf

	AuthRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig

	// AppVersion 客户端最低版本门槛与下载入口，可不配。
	// 最低版本本身存放在 etcd 的独立 key 中并实时监听，调整时改 key 即可，不需要重启服务
	AppVersion AppVersionConf `json:",optional"`
}

type AppVersionConf struct {
	// Etcd 存放最低版本号的 etcd 连接与 key，不配则不启用版本门槛。
	// 配置后 Hosts、Key 必填，Key 约定为 appversion.DefaultKey（config/app.min_version）。
	//
	// 独立于引导配置里的 etcd：引导配置只描述「业务配置从哪里读」，config_source
	// 可以是 file / nacos，未必有 etcd；版本门槛读哪个 etcd 属于业务配置本身。
	Etcd discov.EtcdConf `json:",optional"`
	// DownloadURL 官网下载地址，拼进 426 提示文案，给没有更新功能的旧版客户端看。
	// 旧版只在几秒即消失的报错提示里显示它，要短且固定：填本服务 /auth/download 经网关的完整地址，不要填 OSS 直链
	DownloadURL string `json:",optional"`
	// UpdateFeedURL 客户端自动更新的更新源目录（与 electron-builder 的 publish.url 相同），其下有 latest.yml。
	// /auth/download 据此跳转到最新安装包；不配则该接口返回 404
	UpdateFeedURL string `json:",optional"`
}
