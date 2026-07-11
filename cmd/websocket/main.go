package main

import (
	"flag"
	"log"

	"IM2/internal/apps/websocket/gateway/config"
	gwserver "IM2/internal/apps/websocket/gateway/server"
	"IM2/internal/apps/websocket/gateway/transport"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	service "IM2/pkg/service"

	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("websocket/gateway"),
	"the config file")

func main() {
	flag.Parse()
	var wsServer *gwserver.GatewayServer

	// 服务注册函数
	registerServices := func(c *config.Config, server *rest.Server) error {
		svcCtx := gwserver.NewServiceContext(*c)
		wsServer = gwserver.NewGatewayServer(svcCtx)

		// 注册路由
		transport.RegisterHandlers(server, svcCtx)
		return nil
	}

	runner := service.NewServiceRunner(
		service.NewRestService(registerServices,
			func(c *config.Config) *rest.RestConf { return &c.RestConf },
		),
		*configPath,
		service.WithName("websocket-gateway"),
		service.WithLogger("/var/log/im/websocket.gateway.log", logger.LoggerEnvDev),
		service.WithHooks(&service.LifecycleHooks{
			BeforeStart: func() error {
				if wsServer != nil {
					return wsServer.Start()
				}
				return nil
			},
			AfterStop: func() error {
				if wsServer != nil {
					return wsServer.Stop()
				}
				return nil
			},
		}),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("service start failed: %v", err)
	}
}
