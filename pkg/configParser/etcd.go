package configparser

import (
	"context"
	"fmt"
	"time"

	"IM2/pkg/env"

	"github.com/zeromicro/go-zero/core/conf"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdParser struct {
	EtcdConfig
}

type EtcdConfig struct {
	// Endpoints etcd 节点地址列表，如 ["10.0.0.1:2379", "10.0.0.2:2379"]
	Endpoints []string `json:"endpoints,optional"`
	// Key 业务配置所在的键，如 /im/config/file.api
	Key       string `json:"key,optional"`
	User      string `json:"user,optional"`
	Password  string `json:"password,optional"`
	TimeoutMs uint64 `json:"timeoutms,optional"`
}

// NewEtcdParser 创建etcd配置解析器
func NewEtcdParser(c EtcdConfig) ConfigParser {
	return &etcdParser{
		EtcdConfig: c,
	}
}

// Load 从 etcd 读取 Key 对应的 YAML 配置并解析到 v
// 与 nacos 解析器一致：取回的内容先做 ${VAR} 环境变量展开，再交给 go-zero conf 解析
func (p *etcdParser) Load(v any) error {
	ec := p.EtcdConfig
	if len(ec.Endpoints) == 0 {
		return fmt.Errorf("ETCD 地址(endpoints)未配置")
	}
	if ec.Key == "" {
		return fmt.Errorf("ETCD 配置键(key)未配置")
	}
	if ec.TimeoutMs == 0 {
		ec.TimeoutMs = 5000
	}
	timeout := time.Duration(ec.TimeoutMs) * time.Millisecond

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   ec.Endpoints,
		DialTimeout: timeout,
		Username:    ec.User,
		Password:    ec.Password,
	})
	if err != nil {
		return fmt.Errorf("创建 etcd 客户端失败: %w", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := cli.Get(ctx, ec.Key)
	if err != nil {
		return fmt.Errorf("从 etcd 获取配置失败: %w", err)
	}
	if resp.Count == 0 {
		return fmt.Errorf("etcd 中不存在配置键: %s", ec.Key)
	}

	content := env.ExpandEnv(string(resp.Kvs[0].Value))
	if err := conf.LoadFromYamlBytes([]byte(content), v); err != nil {
		return fmt.Errorf("解析 etcd 配置失败: %w", err)
	}
	return nil
}

// 确保 etcdParser 实现了 ConfigParser 接口
var _ ConfigParser = (*etcdParser)(nil)
