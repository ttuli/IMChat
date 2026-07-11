package main

import (
	"flag"
	"log"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/config"
	server "IM2/internal/apps/Auth/rpc/server/authrpc"
	"IM2/internal/apps/Auth/rpc/svc"
	"IM2/internal/interceptor"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	"IM2/pkg/service"

	zservice "github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("Auth/rpc"),
	"the config file")

func RegisterServices(c *config.Config) (*zrpc.RpcServer, error) {
	server := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		auth.RegisterAuthRpcServer(grpcServer, server.NewAuthRpcServer(svc.NewServiceContext(*c)))

		if c.Mode == zservice.DevMode || c.Mode == zservice.TestMode {
			reflection.Register(grpcServer)
		}
	})
	return server, nil
}

func main() {
	flag.Parse()

	runner := service.NewServiceRunner(
		service.NewRpcService(RegisterServices,
			service.WithUnaryInterceptors(interceptor.ServerErrorInterceptor)),
		*configPath,
		service.WithName("Auth RPC Service"),
		service.WithLogger("/var/log/im/auth.rpc.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
