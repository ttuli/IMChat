package main

import (
	"flag"
	"fmt"
	"log"

	"IM2/internal/apps/Message/api/config"
	"IM2/internal/apps/Message/api/handler"
	"IM2/internal/apps/Message/api/svc"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	"IM2/pkg/service"

	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("Message/api"),
	"the config file")

func RegisterServices(cfg any, server *rest.Server) error {
	if c, ok := cfg.(*config.Config); ok {
		if c == nil {
			return fmt.Errorf("config 不能为空")
		}
		handler.RegisterHandlers(server, svc.NewServiceContext(*c))
	} else {
		return fmt.Errorf("config 不是正确的配置类型")
	}
	return nil
}

func main() {
	flag.Parse()
	runner := service.NewServiceRunner(
		service.NewRestService(RegisterServices,
			service.WithRestConf(func(cfg any) *rest.RestConf {
				if c, ok := cfg.(*config.Config); ok {
					return &c.RestConf
				}
				return nil
			}),
			service.WithAPISIX(func(cfg any) *service.APISIXConfig {
				if c, ok := cfg.(*config.Config); ok {
					return &c.APISIX
				}
				return nil
			}),
		),
		*configPath,
		&config.Config{},
		service.WithName("Message API Service"),
		service.WithLogger("/var/log/im/message.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
