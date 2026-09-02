package service

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type dummyRestConfig struct {
	rest.RestConf
}

func TestRestServiceHealthzEndpoint(t *testing.T) {
	var c dummyRestConfig
	confText := `
Name: test-api
Host: 127.0.0.1
Port: 18022
Mode: test
`
	if err := conf.LoadFromJsonBytes([]byte(`{"Name":"test-api","Host":"127.0.0.1","Port":18022,"Mode":"test"}`), &c); err != nil {
		t.Fatalf("failed to load conf: %v", err)
		_ = confText
	}

	rs := NewRestService(
		func(c *dummyRestConfig, server *rest.Server) error {
			return nil
		},
		func(c *dummyRestConfig) *rest.RestConf {
			return &c.RestConf
		},
	)

	if err := rs.Load(&c); err != nil {
		t.Fatalf("failed to load RestService: %v", err)
	}

	if err := rs.Start(); err != nil {
		t.Fatalf("failed to start RestService: %v", err)
	}
	defer rs.Stop()

	// Wait for server to start listening
	var resp *http.Response
	var reqErr error
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		resp, reqErr = http.Get("http://127.0.0.1:18022/healthz")
		if reqErr == nil && resp.StatusCode == http.StatusOK {
			break
		}
	}

	if reqErr != nil {
		t.Fatalf("failed to get /healthz: %v", reqErr)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"status":"ok"}` {
		t.Fatalf("expected body {\"status\":\"ok\"}, got %s", string(body))
	}
}

type dummyRpcConfig struct {
	zrpc.RpcServerConf
}

func TestRpcServiceHealthCheck(t *testing.T) {
	var c dummyRpcConfig
	c.Name = "test-rpc"
	c.ListenOn = "127.0.0.1:18000"
	c.Mode = service.TestMode
	c.Health = true

	rs := NewRpcService(
		func(c *dummyRpcConfig) (*zrpc.RpcServer, error) {
			s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {})
			return s, nil
		},
	)

	if err := rs.Load(&c); err != nil {
		t.Fatalf("failed to load RpcService: %v", err)
	}

	if err := rs.Start(); err != nil {
		t.Fatalf("failed to start RpcService: %v", err)
	}
	defer rs.Stop()

	// Test gRPC health check client connection
	var conn *grpc.ClientConn
	var dialErr error
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		conn, dialErr = grpc.NewClient("127.0.0.1:18000", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			healthClient := grpc_health_v1.NewHealthClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, checkErr := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			cancel()
			if checkErr == nil && resp.Status == grpc_health_v1.HealthCheckResponse_SERVING {
				_ = conn.Close()
				t.Logf("gRPC health check passed: SERVING")
				return
			}
		}
		if conn != nil {
			_ = conn.Close()
		}
	}

	t.Fatalf("gRPC health check failed: dialErr=%v", dialErr)
}
