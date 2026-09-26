package main

import (
	"flag"
	"log"

	"IM2/internal/apps/Auth/api/config"
	"IM2/internal/apps/Auth/api/handler"
	"IM2/internal/apps/Auth/api/svc"
	"IM2/pkg/appversion"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	service "IM2/pkg/service"

	"github.com/zeromicro/go-zero/core/configcenter/subscriber"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("Auth/api"),
	"the config file")

func RegisterServices(c *config.Config, server *rest.Server) error {
	svcCtx := svc.NewServiceContext(*c)
	svcCtx.VersionGate = newVersionGate(c)
	handler.RegisterHandlers(server, svcCtx)
	return nil
}

// newVersionGate 复用引导配置里的 etcd 连接信息，订阅最低版本 key。
// 拿不到 etcd 配置或连接失败时返回 nil，即不启用门槛：版本门槛不能成为服务起不来的原因。
func newVersionGate(c *config.Config) *appversion.Gate {
	etcdConf, err := configparser.LoadEtcdConfig(*configPath)
	if err != nil {
		logx.Errorf("[Auth] 未启用客户端版本门槛: %v", err)
		return nil
	}
	gate, err := appversion.NewGate(subscriber.EtcdConf{
		Hosts: etcdConf.Endpoints,
		Key:   c.AppVersion.Key, // 为空时用 appversion.DefaultKey
		User:  etcdConf.User,
		Pass:  etcdConf.Password,
	})
	if err != nil {
		logx.Errorf("[Auth] 未启用客户端版本门槛，连接 etcd 失败: %v", err)
		return nil
	}
	return gate
}

func main() {
	flag.Parse()
	runner := service.NewServiceRunner(
		service.NewRestService(RegisterServices,
			func(c *config.Config) *rest.RestConf { return &c.RestConf },
		),
		*configPath,
		service.WithName("Auth API Service"),
		service.WithLogger("/var/log/im/auth.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
