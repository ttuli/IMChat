package main

import (
	"flag"
	"fmt"
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
	registerServices := func(cfg any, server *rest.Server) error {
		c, ok := cfg.(*config.Config)
		if !ok {
			return fmt.Errorf("config types error")
		}

		svcCtx := gwserver.NewServiceContext(*c)
		wsServer = gwserver.NewGatewayServer(svcCtx)

		// 注册路由
		transport.RegisterHandlers(server, svcCtx)
		return nil
	}

	runner := service.NewServiceRunner(
		service.NewRestService(registerServices,
			service.WithRestConf(func(cfg any) *rest.RestConf {
				if c, ok := cfg.(*config.Config); ok {
					return &c.RestConf
				}
				return nil
			}),
		),
		*configPath,
		&config.Config{},
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
