package configparser

import (
	"IM2/pkg/env"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/zeromicro/go-zero/core/conf"
)

type nacosParser struct {
	NacosConfig
}

type NacosConfig struct {
	Host                string `json:"host,optional"`
	Port                uint64 `json:"port,optional"`
	Namespace           string `json:"namespace,optional"`
	User                string `json:"user,optional"`
	Password            string `json:"password,optional"`
	DataId              string `json:"dataid,optional"`
	Group               string `json:"group,optional"`
	LogDir              string `json:"logdir,optional"`
	CacheDir            string `json:"cachedir,optional"`
	LogLevel            string `json:"loglevel,optional"`
	TimeoutMs           uint64 `json:"timeoutms,optional"`
	NotLoadCacheAtStart bool   `json:"notloadcacheatstart,optional"`
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
	// 敏感信息通过配置文件里的 ${NACOS_USER}/${NACOS_PASSWORD} 占位符在引导阶段展开，
	// 此处只做必填校验。host 不设默认值：漏配时若回退到某个公网域名，会把带凭证的
	// 连接指向不受控主机，造成凭证泄露，因此直接报错。
	if nc.Host == "" {
		return fmt.Errorf("NACOS 地址(host)未配置")
	}
	if nc.Password == "" {
		return fmt.Errorf("NACOS 密码(NACOS_PASSWORD)未配置")
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

	return nil
}

// 确保 nacosParser 实现了 ConfigParser 接口
var _ ConfigParser = (*nacosParser)(nil)
