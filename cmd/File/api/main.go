package main

import (
	"flag"
	"fmt"
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
	os.Setenv("OSS_ACCESS_KEY_ID", os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"))
	os.Setenv("OSS_ACCESS_KEY_SECRET",os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"))
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
		service.WithName("File API Service"),
		service.WithLogger("/var/log/im/file.api.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
