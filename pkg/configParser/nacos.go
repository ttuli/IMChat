package configparser

import (
	"IM2/pkg/env"
	"fmt"
	"os"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/zeromicro/go-zero/core/conf"
)

type nacosParser struct {
	NacosConfig
}

type NacosConfig struct {
	Host                string `mapstructure:"host"`
	Port                uint64 `mapstructure:"port"`
	Namespace           string `mapstructure:"namespace"`
	User                string `mapstructure:"user"`
	Password            string `mapstructure:"password"`
	DataId              string `mapstructure:"dataid"`
	Group               string `mapstructure:"group"`
	LogDir              string `mapstructure:"logdir"`
	CacheDir            string `mapstructure:"cachedir"`
	LogLevel            string `mapstructure:"loglevel"`
	TimeoutMs           uint64 `mapstructure:"timeoutms"`
	NotLoadCacheAtStart bool   `mapstructure:"notloadcacheatstart"`
}

// NewNacosParser 创建Nacos配置解析器
func NewNacosParser(c NacosConfig) ConfigParser {
	return &nacosParser{
		NacosConfig: c,
	}
}

// Load 加载配置，返回错误而不是panic
func (p *nacosParser) Load(v any) error {
	nc := p.NacosConfig
	// 优先使用环境变量，环境变量优先级高于配置文件
	// 这样可以在不修改配置文件的情况下，通过环境变量覆盖敏感信息
	if nacosUser := os.Getenv(nc.User); nacosUser != "" {
		nc.User = nacosUser
	}
	if nacosPassword := os.Getenv(nc.Password); nacosPassword != "" {
		nc.Password = nacosPassword
	} else if nc.Password == "" {
		// 如果环境变量和配置文件都没有密码，返回错误
		return fmt.Errorf("NACOS_PASSWORD 环境变量未设置，且配置文件中也没有密码")
	}
	if nc.Host == "" {
		// 如果配置文件中也没有设置，使用默认值
		nc.Host = "nacos.com"
	}

	sc := []constant.ServerConfig{
		{
			IpAddr: nc.Host,
			Port:   nc.Port,
		},
	}

	if nc.LogDir == "" {
		nc.LogDir = "nacos/log"
	}
	if nc.CacheDir == "" {
		nc.CacheDir = "nacos/cache"
	}
	if nc.LogLevel == "" {
		nc.LogLevel = "debug"
	}
	if nc.TimeoutMs == 0 {
		nc.TimeoutMs = 5000
	}

	cc := constant.ClientConfig{
		NamespaceId:         nc.Namespace, // 如果需要支持多namespace，我们可以场景多个client,它们有不同的NamespaceId
		TimeoutMs:           nc.TimeoutMs,
		NotLoadCacheAtStart: nc.NotLoadCacheAtStart,
		LogDir:              nc.LogDir,
		CacheDir:            nc.CacheDir,
		LogLevel:            nc.LogLevel,
		Username:            nc.User,
		Password:            nc.Password,
	}

	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return fmt.Errorf("创建Nacos配置客户端失败: %w", err)
	}
	content, err := configClient.GetConfig(vo.ConfigParam{
		DataId: nc.DataId,
		Group:  nc.Group,
	})
	if err != nil {
		return fmt.Errorf("从Nacos获取配置失败: %w", err)
	}
	content = env.ExpandEnv(content)
	if err := conf.LoadFromYamlBytes([]byte(content), v); err != nil {
		return err
	}

	// if err := json.Unmarshal([]byte(content), v); err != nil {
	// 	return fmt.Errorf("反序列化配置失败: %w", err)
	// }

	return nil
}

// MustLoad 加载配置，失败时panic（保持向后兼容）
func (p *nacosParser) MustLoad(v any) {
	if err := p.Load(v); err != nil {
		panic(err)
	}
}

// 确保 nacosParser 实现了 ConfigParser 接口
var _ ConfigParser = (*nacosParser)(nil)
