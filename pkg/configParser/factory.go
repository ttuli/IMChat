package configparser

import (
	"IM2/pkg/env"
	"fmt"

	"github.com/spf13/viper"
)

// ConfigSourceType 配置源类型
type ConfigSourceType string

const (
	ConfigSourceNacos ConfigSourceType = "nacos" // Nacos配置中心
	ConfigSourceFile  ConfigSourceType = "file"  // 本地文件（go-zero conf）
)

// LocalConfig 本地配置文件结构（用于决定使用哪种解析器）
type LocalConfig struct {
	ConfigSource ConfigSourceType `mapstructure:"config_source" yaml:"config_source"`
	NacosConfig  `mapstructure:"nacos" yaml:"nacos"`
	FileConfig   `mapstructure:"file" yaml:"file"`
}

type ConfigLoader struct {
	configFile string
	config     LocalConfig
	parser     ConfigParser
}

func NewConfigLoader(configFile string) *ConfigLoader {
	return &ConfigLoader{
		configFile: configFile,
	}
}

// Load 加载配置文件
// v: 目标配置结构体指针
func (l *ConfigLoader) Load(v any) error {
	if l.configFile == "" {
		return fmt.Errorf("配置文件路径不能为空")
	}

	vi := viper.New()
	vi.SetConfigFile(l.configFile)

	// 1. 读取配置文件到 viper 实例中
	if err := vi.ReadInConfig(); err == nil {

		// --- 新增逻辑：处理环境变量替换 ---
		// 将所有设置项导出为 map，处理后再重新加载回去
		settings := vi.AllSettings()

		env.ExpandEnvMap(settings)
		if err := vi.MergeConfigMap(settings); err != nil {
			return fmt.Errorf("合并环境变量配置失败: %w", err)
		}

		// 2. 解析到结构体
		if err := vi.Unmarshal(&l.config); err == nil {
			if l.config.ConfigSource != "" {
				l.parser = createParserByType(l.config)
				if err := l.parser.Load(v); err != nil {
					return fmt.Errorf("加载配置文件失败: %w", err)
				}
			} else {
				return fmt.Errorf("配置类型不能为空")
			}
		} else {
			return fmt.Errorf("解析本地配置文件失败: %w", err)
		}
	} else {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}
	return nil
}

// createParserByType 根据类型创建解析器
func createParserByType(c LocalConfig) ConfigParser {
	switch c.ConfigSource {
	case ConfigSourceFile:
		return NewFileParser(c.FileConfig)
	case ConfigSourceNacos:
		return NewNacosParser(c.NacosConfig)
	default:
		return nil
	}
}
