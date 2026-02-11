package main

import (
	"flag"
	"fmt"
	"log"

	"IM2/internal/apps/Group/rpc/config"
	"IM2/internal/apps/Group/rpc/server/grouprpc"
	"IM2/internal/apps/Group/rpc/svc"
	"IM2/internal/apps/Group/rpc/group"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"
	"IM2/pkg/service"

	zservice "github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("Group/rpc"),
	"the config file")

func RegisterServices(cfg any) (*zrpc.RpcServer, error) {
	if c, ok := cfg.(*config.Config); ok {
		if c == nil {
			return nil, fmt.Errorf("config 不能为空")
		}
		server := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
			group.RegisterGroupRpcServer(grpcServer, server.NewGroupRpcServer(svc.NewServiceContext(*c)))

			if c.Mode == zservice.DevMode || c.Mode == zservice.TestMode {
				reflection.Register(grpcServer)
			}
		})
		return server, nil
	}
	return nil, fmt.Errorf("config 不是正确的配置类型")
}

func main() {
	flag.Parse()

	runner := service.NewServiceRunner(
		service.NewRpcService(RegisterServices),
		*configPath,
		&config.Config{},
		service.WithName("Group RPC Service"),
		service.WithLogger("/var/log/im/group.rpc.log", logger.LoggerEnvDev),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
