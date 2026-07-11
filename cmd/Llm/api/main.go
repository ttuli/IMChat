package main

import (
	"flag"
	"log"

	"IM2/internal/apps/Llm/api/config"
	"IM2/internal/apps/Llm/api/handler"
	"IM2/internal/apps/Llm/api/svc"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	service "IM2/pkg/service"

	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("Llm/api"),
	"the config file")

func RegisterServices(c *config.Config, server *rest.Server) error {
	handler.RegisterHandlers(server, svc.NewServiceContext(*c))
	return nil
}

func main() {
	flag.Parse()
	runner := service.NewServiceRunner(
		service.NewRestService(RegisterServices,
			func(c *config.Config) *rest.RestConf { return &c.RestConf },
		),
		*configPath,
		service.WithName("Llm API Service"),
		service.WithLogger("/var/log/im/llm.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
