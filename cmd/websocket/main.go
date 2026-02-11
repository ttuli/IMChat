package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/handler"
	"IM2/internal/apps/websocket/gateway/svc"
	configparser "IM2/pkg/configParser"
	"IM2/pkg/logger"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configPath = flag.String("f",
	configparser.DefaultConfigPath("websocket"),
	"the config file")

func main() {
	flag.Parse()

	// 加载配置
	var c config.Config
	conf.MustLoad(*configPath, &c)

	// 初始化日志
	logger.InitLogger("/var/log/im/websocket.gateway.log", "websocket-gateway", logger.LoggerEnvDev)
	defer logger.Sync()

	// 创建 REST 服务
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	// 创建服务上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcCtx := svc.NewServiceContext(c)

	// 启动服务上下文(注册节点、订阅消息)
	if err := svcCtx.Start(ctx); err != nil {
		log.Fatalf("start service context failed: %v", err)
	}
	defer svcCtx.Stop(context.Background())

	// 注册路由
	handler.RegisterHandlers(server, svcCtx)

	// 优雅关闭
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
		server.Stop()
	}()

	// 启动服务
	fmt.Printf("Starting WebSocket Gateway at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
