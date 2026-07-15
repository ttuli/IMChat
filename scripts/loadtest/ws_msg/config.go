package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Host     string `yaml:"host"`
	Path     string `yaml:"path"`
	TLS      bool   `yaml:"tls"`
	Insecure bool   `yaml:"insecure"`

	JWTExpire int64 `yaml:"jwt_expire"`

	Num         int    `yaml:"num"`
	StartUserID uint64 `yaml:"start_user_id"`
	DialRate    int    `yaml:"dial_rate"`
	DialTimeout string `yaml:"dial_timeout"`

	// Message sending configurations
	SendRate     int    `yaml:"send_rate"`
	SendCount    int    `yaml:"send_count"`
	Duration     string `yaml:"duration"`
	TargetUserID uint64 `yaml:"target_user_id"`
	Content      string `yaml:"content"`
	PayloadSize  int    `yaml:"payload_size"`

	// Parsed durations, not written to YAML
	dialTimeoutDur time.Duration
	durationDur    time.Duration
}

func (c *Config) parseDurations() error {
	var err error
	if c.dialTimeoutDur, err = time.ParseDuration(c.DialTimeout); err != nil {
		return fmt.Errorf("dial_timeout 格式错误: %w", err)
	}
	if c.durationDur, err = time.ParseDuration(c.Duration); err != nil {
		return fmt.Errorf("duration 格式错误: %w", err)
	}
	return nil
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 校验必填项
	if cfg.Host == "" {
		return cfg, fmt.Errorf("配置项不能为空: host")
	}
	if cfg.Path == "" {
		return cfg, fmt.Errorf("配置项不能为空: path")
	}
	if cfg.Num <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: num (必须 > 0)")
	}
	if cfg.StartUserID <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: start_user_id (必须 > 0)")
	}
	if cfg.DialRate <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: dial_rate (必须 > 0)")
	}
	if cfg.DialTimeout == "" {
		return cfg, fmt.Errorf("配置项不能为空: dial_timeout")
	}
	if cfg.JWTExpire <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: jwt_expire (必须 > 0)")
	}
	if cfg.Duration == "" {
		return cfg, fmt.Errorf("配置项不能为空: duration")
	}

	if err := cfg.parseDurations(); err != nil {
		return cfg, err
	}
	return cfg, nil
}
