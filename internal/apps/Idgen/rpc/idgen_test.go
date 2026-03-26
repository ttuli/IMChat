//go:build integration

package idgen_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"IM2/internal/apps/Idgen/rpc/idgen"
	"IM2/internal/apps/Idgen/rpc/idgenclient"
	"IM2/pkg/env"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestGetUserId_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	
	env.LoadEnv()
	rpcAddr := env.GetString("ETCD_HOST", "127.0.0.1:8080") // 根据你的 RPC 服务配置修改

	// 创建 RPC 客户端配置
	clientConf := zrpc.RpcClientConf{
		Etcd: discov.EtcdConf{
			Hosts: []string{rpcAddr},
			Key:   "idgen.rpc",
		},
	}

	// 创建 RPC 客户端
	client := zrpc.MustNewClient(clientConf)
	idgenClient := idgenclient.NewIdgen(client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试多次调用，每次应该返回不同的ID
	const testCount = 10
	ids := make(map[int64]bool)

	for i := 0; i < testCount; i++ {
		resp, err := idgenClient.GetId(ctx, &idgen.GetIdReq{
			IdType: idgen.IDType_ID_TYPE_USER,
			Count:  1,
		})
		if err != nil {
			t.Fatalf("第%d次获取ID失败: %v", i+1, err)
		}

		if len(resp.Ids) == 0 {
			t.Fatalf("第%d次获取ID返回空列表", i+1)
		}

		id := resp.Ids[0]
		if ids[id] {
			t.Errorf("第%d次获取的ID %d 与之前重复！", i+1, id)
		}
		ids[id] = true
		fmt.Printf("第%d次调用，获取ID: %d\n", i+1, id)
	}

	t.Logf("成功获取 %d 个不同的ID", len(ids))
}
