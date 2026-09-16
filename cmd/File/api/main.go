// 阿里云 OSS 上传回调的验签公钥是 512 位 RSA（历史遗留，gosspublic.alicdn.com 下发）。
// Go 1.24 起 crypto/rsa 拒绝小于 1024 位的密钥，会让 signverify.go 里的
// rsa.VerifyPKCS1v15 直接报 "512-bit keys are insecure"，把合法回调判成伪造并返回 400，
// OSS 随即给客户端回 203 CallbackFailed —— 表现为发图片失败、头像改了不生效。
//
// 放开该限制在本服务无额外攻击面：进程内唯一的 RSA 验签就是这个回调，公钥来源已在
// signverify.go 中白名单锁死为 gosspublic.alicdn.com；JWT 走 HS256，不涉及 RSA。
//
//go:debug rsa1024min=0

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
