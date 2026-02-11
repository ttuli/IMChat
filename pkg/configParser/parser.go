package configparser

import (
	"os"
	"path/filepath"
	"strings"
)

// ConfigParser 配置解析器接口
type ConfigParser interface {
	// Load 加载配置到目标结构体
	// filepath: 配置文件路径（对于Nacos是本地配置文件路径，对于File是业务配置文件路径）
	// v: 目标配置结构体指针
	Load(v any) error

	// MustLoad 加载配置，失败时panic（保持向后兼容）
	MustLoad(v any)
}

func DefaultConfigPath(serviceName string) string {
	// 1. 优先使用工作目录（适配 go run 和开发环境）
	if wd, err := os.Getwd(); err == nil {
		path := filepath.Join(
			wd,
			"../../..",
			"internal/apps",
			serviceName,
			"etc/config.yaml",
		)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// 2. 使用可执行文件路径（生产环境编译后的二进制）
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)

		// 跳过 /tmp 临时目录（go run 产生的）
		if !strings.Contains(exeDir, "/tmp/go-build") {
			root := filepath.Clean(filepath.Join(exeDir, "..", ".."))
			path := filepath.Join(
				root,
				"internal/apps",
				serviceName,
				"etc/config.yaml",
			)
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}

	return ""
}
