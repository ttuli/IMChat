package configparser

import (
	"os"
	"path/filepath"
	"strings"
)

// ConfigParser 配置解析器接口
type ConfigParser interface {
	// Load 加载配置到目标结构体
	// v: 目标配置结构体指针
	Load(v any) error
}

func DefaultConfigPath(serviceName string) string {
	configRelPath := filepath.Join("internal/apps", serviceName, "etc/config.yaml")

	// 1. 从工作目录向上查找项目根目录 (go.mod 所在目录)
	if wd, err := os.Getwd(); err == nil {
		if root := findProjectRoot(wd); root != "" {
			path := filepath.Join(root, configRelPath)
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}

	// 2. 从可执行文件路径向上查找（生产环境编译后的二进制）
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		// 跳过 /tmp 临时目录（go run 产生的）
		if !strings.Contains(exeDir, "/tmp/go-build") {
			if root := findProjectRoot(exeDir); root != "" {
				path := filepath.Join(root, configRelPath)
				if _, err := os.Stat(path); err == nil {
					return path
				}
			}
		}
	}

	return ""
}

// findProjectRoot 从 startDir 向上逐级查找包含 go.mod 的目录
func findProjectRoot(startDir string) string {
	dir := startDir
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// 已到根目录
			return ""
		}
		dir = parent
	}
}
