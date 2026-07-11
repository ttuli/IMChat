package main

import (
	"flag"
	"log"

	"IM2/internal/apps/Llm/rpc/config"
	"IM2/internal/apps/Llm/rpc/llm"
	server "IM2/internal/apps/Llm/rpc/server/llm"
	"IM2/internal/apps/Llm/rpc/svc"
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
	configparser.DefaultConfigPath("Llm/rpc"),
	"the config file")

func RegisterServices(c *config.Config) (*zrpc.RpcServer, error) {
	rpcServer := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		llm.RegisterLlmServer(grpcServer, server.NewLlmServer(svc.NewServiceContext(*c)))

		if c.Mode == zservice.DevMode || c.Mode == zservice.TestMode {
			reflection.Register(grpcServer)
		}
	})
	return rpcServer, nil
}

func main() {
	flag.Parse()

	runner := service.NewServiceRunner(
		service.NewRpcService(RegisterServices,
			service.WithUnaryInterceptors(interceptor.ServerErrorInterceptor)),
		*configPath,
		service.WithName("Llm RPC Service"),
		service.WithLogger("/var/log/im/llm.rpc.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
