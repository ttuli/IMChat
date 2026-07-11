package configparser

import "testing"

func TestParseLocalConfigNacos(t *testing.T) {
	t.Setenv("NACOS_HOST", "10.0.0.1")
	t.Setenv("NACOS_PASSWORD", "s3cret")

	yaml := `
config_source: 'nacos'
nacos:
  host: '${NACOS_HOST}'
  port: 8848
  namespace: 'ns-1'
  password: '${NACOS_PASSWORD}'
  dataid: 'file.api'
  group: 'DEFAULT_GROUP'
`
	local, err := parseLocalConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("parseLocalConfig: %v", err)
	}
	if local.ConfigSource != ConfigSourceNacos {
		t.Errorf("ConfigSource = %q, want nacos", local.ConfigSource)
	}
	if local.Nacos.Host != "10.0.0.1" {
		t.Errorf("Host = %q, want 10.0.0.1 (env not expanded?)", local.Nacos.Host)
	}
	if local.Nacos.Port != 8848 {
		t.Errorf("Port = %d, want 8848", local.Nacos.Port)
	}
	if local.Nacos.Password != "s3cret" {
		t.Errorf("Password = %q, want s3cret", local.Nacos.Password)
	}
	if local.Nacos.DataId != "file.api" {
		t.Errorf("DataId = %q, want file.api", local.Nacos.DataId)
	}
}

func TestParseLocalConfigFile(t *testing.T) {
	yaml := `
config_source: 'file'
file:
  path: '/etc/app/config.yaml'
`
	local, err := parseLocalConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("parseLocalConfig: %v", err)
	}
	if local.ConfigSource != ConfigSourceFile {
		t.Errorf("ConfigSource = %q, want file", local.ConfigSource)
	}
	if local.File.Path != "/etc/app/config.yaml" {
		t.Errorf("Path = %q, want /etc/app/config.yaml", local.File.Path)
	}
}

func TestParseLocalConfigEtcd(t *testing.T) {
	t.Setenv("ETCD_HOST", "10.0.0.9")

	yaml := `
config_source: 'etcd'
etcd:
  endpoints:
    - '${ETCD_HOST}:2379'
    - '10.0.0.2:2379'
  key: '/im/config/file.api'
  timeoutms: 3000
`
	local, err := parseLocalConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("parseLocalConfig: %v", err)
	}
	if local.ConfigSource != ConfigSourceEtcd {
		t.Errorf("ConfigSource = %q, want etcd", local.ConfigSource)
	}
	if len(local.Etcd.Endpoints) != 2 || local.Etcd.Endpoints[0] != "10.0.0.9:2379" {
		t.Errorf("Endpoints = %v, want [10.0.0.9:2379 10.0.0.2:2379]", local.Etcd.Endpoints)
	}
	if local.Etcd.Key != "/im/config/file.api" {
		t.Errorf("Key = %q, want /im/config/file.api", local.Etcd.Key)
	}
	if local.Etcd.TimeoutMs != 3000 {
		t.Errorf("TimeoutMs = %d, want 3000", local.Etcd.TimeoutMs)
	}
}

func TestEtcdParserValidation(t *testing.T) {
	if err := NewEtcdParser(EtcdConfig{Key: "/k"}).Load(&struct{}{}); err == nil {
		t.Error("endpoints 为空时应报错")
	}
	if err := NewEtcdParser(EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}}).Load(&struct{}{}); err == nil {
		t.Error("key 为空时应报错")
	}
}

func TestParseLocalConfigMissingSource(t *testing.T) {
	if _, err := parseLocalConfig([]byte("nacos:\n  host: '1.2.3.4'\n")); err == nil {
		t.Fatal("expected error for missing config_source, got nil")
	}
}

func TestCreateParserByType(t *testing.T) {
	if _, err := createParserByType(localConfig{ConfigSource: ConfigSourceNacos}); err != nil {
		t.Errorf("nacos: unexpected error %v", err)
	}
	if _, err := createParserByType(localConfig{ConfigSource: ConfigSourceFile}); err != nil {
		t.Errorf("file: unexpected error %v", err)
	}
	if _, err := createParserByType(localConfig{ConfigSource: ConfigSourceEtcd}); err != nil {
		t.Errorf("etcd: unexpected error %v", err)
	}
	if _, err := createParserByType(localConfig{ConfigSource: "redis"}); err == nil {
		t.Fatal("expected error for unknown config source, got nil")
	}
}
