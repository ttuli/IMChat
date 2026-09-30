package configparser

import (
	"fmt"
	"os"

	"IM2/pkg/env"

	"github.com/zeromicro/go-zero/core/conf"
)

// ConfigSourceType 配置源类型
type ConfigSourceType string

const (
	ConfigSourceNacos ConfigSourceType = "nacos" // Nacos配置中心
	ConfigSourceFile  ConfigSourceType = "file"  // 本地文件（go-zero conf）
	ConfigSourceEtcd  ConfigSourceType = "etcd"  // etcd 键值存储
)

// localConfig 引导配置文件结构（用于决定使用哪种解析器）
// 注意：这里用具名字段而非匿名内嵌——go-zero conf 会把匿名字段扁平化上提，
// 无法按 nacos/file 子键映射，与 viper 语义不同。
type localConfig struct {
	ConfigSource ConfigSourceType `json:"config_source,optional"`
	Nacos        NacosConfig      `json:"nacos,optional"`
	File         FileConfig       `json:"file,optional"`
	Etcd         EtcdConfig       `json:"etcd,optional"`
}

// Load 读取引导配置文件，按 config_source 选择解析器，将业务配置加载到 v。
// configFile: 引导配置文件路径；v: 目标配置结构体指针。
func Load(configFile string, v any) error {
	if configFile == "" {
		return fmt.Errorf("配置文件路径不能为空")
	}

	content, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	local, err := parseLocalConfig(content)
	if err != nil {
		return err
	}

	parser, err := createParserByType(local)
	if err != nil {
		return err
	}
	return parser.Load(v)
}

// parseLocalConfig 展开环境变量并解析引导配置。
// 与 Nacos 业务配置一样走 go-zero conf，保证两条加载路径解析行为一致。
func parseLocalConfig(content []byte) (localConfig, error) {
	var local localConfig
	expanded := env.ExpandEnv(string(content))
	if err := conf.LoadFromYamlBytes([]byte(expanded), &local); err != nil {
		return local, fmt.Errorf("解析本地配置文件失败: %w", err)
	}
	if local.ConfigSource == "" {
		return local, fmt.Errorf("配置类型(config_source)不能为空")
	}
	return local, nil
}

// createParserByType 根据类型创建解析器，未知类型返回错误而非 nil，避免上层空指针崩溃。
func createParserByType(c localConfig) (ConfigParser, error) {
	switch c.ConfigSource {
	case ConfigSourceFile:
		return NewFileParser(c.File), nil
	case ConfigSourceNacos:
		return NewNacosParser(c.Nacos), nil
	case ConfigSourceEtcd:
		return NewEtcdParser(c.Etcd), nil
	default:
		return nil, fmt.Errorf("不支持的配置源类型: %q", c.ConfigSource)
	}
}
