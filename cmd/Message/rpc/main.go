package main

import (
	"flag"
	"log"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/listener"
	"IM2/internal/apps/Message/rpc/message"
	server "IM2/internal/apps/Message/rpc/server/messagerpc"
	"IM2/internal/apps/Message/rpc/svc"
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
	configparser.DefaultConfigPath("Message/rpc"),
	"the config file")

func main() {
	flag.Parse()

	var svcCtx *svc.ServiceContext
	var listenService *listener.NatsListener

	registerServices := func(c *config.Config) (*zrpc.RpcServer, error) {
		svcCtx = svc.NewServiceContext(*c) // 赋值给外部变量
		listenService = listener.NewNatsListener(*c, svcCtx)

		server := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
			message.RegisterMessageRpcServer(grpcServer, server.NewMessageRpcServer(svcCtx))
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
		service.WithName("Message RPC Service"),
		service.WithLogger("/var/log/im/message.rpc.log", logger.LoggerEnvDev),
		service.WithHooks(&service.LifecycleHooks{
			BeforeStart: func() error {
				if listenService != nil {
					return listenService.Listen()
				}
				return nil
			},
			BeforeStop: func() error {
				if listenService != nil {
					return listenService.Stop()
				}
				return nil
			},
		}),
	)

	if err := runner.Run(); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
