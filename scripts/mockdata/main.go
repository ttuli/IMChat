package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"IM2/internal/apps/User/rpc/client/user"

	"github.com/zeromicro/go-zero/zrpc"
)

var (
	count      = flag.Int("count", 100, "创建的用户数量")
	rpcAddr    = flag.String("rpc", "localhost:10000", "User RPC 地址")
	namePrefix = flag.String("prefix", "test_user_", "用户名前缀")
)

// MockDataGenerator 用于生成 mock 用户数据
type MockDataGenerator struct {
	client user.User
}

// NewMockDataGenerator 创建 mock 数据生成器
func NewMockDataGenerator(client user.User) *MockDataGenerator {
	return &MockDataGenerator{client: client}
}

// generatePhone 生成随机手机号
func (g *MockDataGenerator) generatePhone() string {
	prefixes := []string{"138", "139", "150", "151", "152", "186", "187", "188", "189"}
	prefix := prefixes[rand.Intn(len(prefixes))]
	return fmt.Sprintf("%s%08d", prefix, rand.Intn(100000000))
}

// generateAvatar 生成随机头像URL
func (g *MockDataGenerator) generateAvatar(id int) string {
	return fmt.Sprintf("https://api.dicebear.com/7.x/avataaars/svg?seed=user%d", id)
}

// generateGender 生成随机性别 (1-男, 2-女, 3-未知)
func (g *MockDataGenerator) generateGender() int32 {
	return int32(rand.Intn(3) + 1)
}

// generateSignature 生成随机签名
func (g *MockDataGenerator) generateSignature(id int) string {
	signatures := []string{
		"生活不止眼前的苟且",
		"保持热爱，奔赴山海",
		"今天也要加油鸭！",
		"愿你历尽千帆，归来仍是少年",
		"每一天都是新的开始",
		"做最好的自己",
		"简单快乐每一天",
		"努力成为更好的人",
		"阳光正好，微风不燥",
		"慢慢来，比较快",
	}
	return signatures[id%len(signatures)]
}

// CreateMockUser 创建单个 mock 用户
func (g *MockDataGenerator) CreateMockUser(ctx context.Context, id int, namePrefix string) (*user.CreateUserResp, error) {
	phone := g.generatePhone()
	req := &user.CreateUserReq{
		Phone:             phone,
		Name:              fmt.Sprintf("%s%d", namePrefix, id),
		Gender:            g.generateGender(),
		JoinType:          1, // 需要验证
		Avatar:            g.generateAvatar(id),
		PersonalSignature: g.generateSignature(id),
		Password:          "Mock@123456", // 默认密码
	}

	resp, err := g.client.CreateUser(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("创建用户失败 [%s]: %w", req.Name, err)
	}
	return resp, nil
}

// CreateMockUsers 批量创建 mock 用户
func (g *MockDataGenerator) CreateMockUsers(ctx context.Context, count int, namePrefix string) ([]*user.CreateUserResp, error) {
	results := make([]*user.CreateUserResp, 0, count)

	for i := 1; i <= count; i++ {
		resp, err := g.CreateMockUser(ctx, i, namePrefix)
		if err != nil {
			log.Printf("创建用户 #%d 失败: %v", i, err)
			continue
		}
		results = append(results, resp)
		log.Printf("✓ 创建用户 #%d 成功, UserID: %d", i, resp.UserId)
	}

	return results, nil
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// 连接 User RPC
	conf := zrpc.RpcClientConf{
		Endpoints: []string{*rpcAddr},
		NonBlock:  true,
		Timeout:   5000,
	}

	client, err := zrpc.NewClient(conf)
	if err != nil {
		log.Fatalf("连接 User RPC 失败: %v", err)
	}
	defer client.Conn().Close()

	userClient := user.NewUser(client)
	generator := NewMockDataGenerator(userClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*count*5)*time.Second)
	defer cancel()

	log.Printf("开始创建 %d 个 mock 用户...", *count)
	log.Printf("RPC 地址: %s", *rpcAddr)
	log.Printf("用户名前缀: %s", *namePrefix)
	log.Println("-----------------------------")

	results, err := generator.CreateMockUsers(ctx, *count, *namePrefix)
	if err != nil {
		log.Fatalf("批量创建失败: %v", err)
	}

	log.Println("-----------------------------")
	log.Printf("创建完成! 成功: %d, 失败: %d", len(results), *count-len(results))

	// 打印所有创建的用户ID
	if len(results) > 0 {
		log.Println("创建的用户ID列表:")
		for i, r := range results {
			log.Printf("  %d. UserID: %d", i+1, r.UserId)
		}
	}
}
