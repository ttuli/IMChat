package main

import (
	"flag"
	"fmt"
	"log"

	"IM2/internal/apps/Llm/rpc/config"
	"IM2/internal/apps/Llm/rpc/llm"
	server "IM2/internal/apps/Llm/rpc/server/llm"
	"IM2/internal/apps/Llm/rpc/svc"
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

func RegisterServices(cfg any) (*zrpc.RpcServer, error) {
	if c, ok := cfg.(*config.Config); ok {
		if c == nil {
			return nil, fmt.Errorf("config 不能为空")
		}
		rpcServer := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
			llm.RegisterLlmServer(grpcServer, server.NewLlmServer(svc.NewServiceContext(*c)))

			if c.Mode == zservice.DevMode || c.Mode == zservice.TestMode {
				reflection.Register(grpcServer)
			}
		})
		return rpcServer, nil
	}
	return nil, fmt.Errorf("config 不是正确的配置类型")
}

func main() {
	flag.Parse()

	runner := service.NewServiceRunner(
		service.NewRpcService(RegisterServices),
		*configPath,
		&config.Config{},
		service.WithName("Llm RPC Service"),
		service.WithLogger("/var/log/im/llm.rpc.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
