package configparser

import (
	"fmt"

	"github.com/zeromicro/go-zero/core/conf"
)

// fileParser 本地文件配置解析器（使用go-zero的conf包）
type fileParser struct {
	FileConfig
}

type FileConfig struct {
	Path string `mapstructure:"path"`
}

// NewFileParser 创建本地文件配置解析器
func NewFileParser(c FileConfig) ConfigParser {
	return &fileParser{
		FileConfig: c,
	}
}

// Load 从本地文件加载配置（使用go-zero的conf.Load）
func (p *fileParser) Load(v any) error {
	if p.FileConfig.Path == "" {
		return fmt.Errorf("未设置配置文件路径")
	}

	// 使用go-zero的conf.Load加载配置
	if err := conf.Load(p.FileConfig.Path, v); err != nil {
		return fmt.Errorf("加载配置文件失败: %w", err)
	}

	return nil
}

// MustLoad 加载配置，失败时panic（使用go-zero的conf.MustLoad）
func (p *fileParser) MustLoad(v any) {

	// 使用go-zero的conf.MustLoad加载配置
	conf.MustLoad(p.FileConfig.Path, v)
}
