package main

import (
	"flag"
	"fmt"
	"log"

	"IM2/internal/apps/User/api/config"
	"IM2/internal/apps/User/api/handler"
	"IM2/internal/apps/User/api/svc"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	service "IM2/pkg/service"

	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("User/api"),
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
		),
		*configPath,
		&config.Config{},
		service.WithName("User API Service"),
		service.WithLogger("/var/log/im/user.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
