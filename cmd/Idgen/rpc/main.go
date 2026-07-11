package main

import (
	"context"
	"flag"
	"log"
	"time"

	"IM2/internal/apps/Idgen/rpc/config"
	"IM2/internal/apps/Idgen/rpc/idgen"
	"IM2/internal/apps/Idgen/rpc/server"
	"IM2/internal/apps/Idgen/rpc/svc"
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
	configparser.DefaultConfigPath("Idgen/rpc"),
	"the config file")

func main() {
	flag.Parse()

	var svcCtx *svc.ServiceContext

	registerServices := func(c *config.Config) (*zrpc.RpcServer, error) {
		svcCtx = svc.NewServiceContext(*c) // 赋值给外部变量
		server := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
			idgen.RegisterIdgenServer(grpcServer, server.NewIdgenServer(svcCtx))
			if c.Mode == zservice.DevMode || c.Mode == zservice.TestMode {
				reflection.Register(grpcServer)
			}
		})
		return server, nil
	}

	runner := service.NewServiceRunner(
		service.NewRpcService(registerServices,
			service.WithUnaryInterceptors(interceptor.ServerErrorInterceptor)),
		*configPath,
		service.WithName("Idgen RPC Service"),
		service.WithLogger("/var/log/im/idgen.rpc.log", logger.LoggerEnvDev),
		service.WithBeforeExit(func() error {
			if svcCtx != nil && svcCtx.IDService != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return svcCtx.IDService.SaveCacheState(ctx)
			}
			return nil
		}),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
