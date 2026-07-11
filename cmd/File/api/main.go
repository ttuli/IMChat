package main

import (
	"flag"
	"log"
	"os"

	"IM2/internal/apps/File/api/config"
	"IM2/internal/apps/File/api/handler"
	"IM2/internal/apps/File/api/svc"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	"IM2/pkg/service"

	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("File/api"),
	"the config file")

func RegisterServices(c *config.Config, server *rest.Server) error {
	handler.RegisterHandlers(server, svc.NewServiceContext(*c))
	return nil
}

func main() {
	flag.Parse()
	os.Setenv("OSS_ACCESS_KEY_ID", os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"))
	os.Setenv("OSS_ACCESS_KEY_SECRET",os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"))
	runner := service.NewServiceRunner(
		service.NewRestService(RegisterServices,
			func(c *config.Config) *rest.RestConf { return &c.RestConf },
		),
		*configPath,
		service.WithName("File API Service"),
		service.WithLogger("/var/log/im/file.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
