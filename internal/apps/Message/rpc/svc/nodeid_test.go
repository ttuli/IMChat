package svc

import (
	"errors"
	"hash/crc32"
	"testing"
)

func envOf(nodeID string) func(string) string {
	return func(key string) string {
		if key == "NODE_ID" {
			return nodeID
		}
		return ""
	}
}

func hostOf(name string) func() (string, error) {
	return func() (string, error) { return name, nil }
}

func TestResolveNodeID(t *testing.T) {
	hashOf := func(host string) int64 { return int64(crc32.ChecksumIEEE([]byte("msg-"+host)) % 1024) }

	cases := []struct {
		name       string
		env        string
		configured int64
		wantID     int64
		wantSource string
		wantErr    bool
	}{
		// StatefulSet 第一个 Pod 的序号是 0，不能被当成"未设置"而退回哈希
		{name: "env 0 is valid", env: "0", wantID: 0, wantSource: nodeSourceEnv},
		{name: "env upper bound", env: "1023", wantID: 1023, wantSource: nodeSourceEnv},
		// 环境变量按实例注入，优先于所有副本共享的配置
		{name: "env beats config", env: "1", configured: 7, wantID: 1, wantSource: nodeSourceEnv},
		{name: "env out of range", env: "1024", wantErr: true},
		{name: "env negative", env: "-1", wantErr: true},
		// 误把网关那种 Pod 名注入进来时要报错，而不是悄悄退回会撞号的哈希
		{name: "env not a number", env: "message-rpc-0", wantErr: true},
		{name: "config used without env", configured: 7, wantID: 7, wantSource: nodeSourceConfig},
		{name: "config out of range", configured: 2000, wantErr: true},
		{name: "hostname hash fallback", wantID: hashOf("dev-box"), wantSource: nodeSourceHostname},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, source, err := resolveNodeID(tc.configured, envOf(tc.env), hostOf("dev-box"))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got id=%d source=%q", id, source)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id != tc.wantID || source != tc.wantSource {
				t.Fatalf("got (%d, %q), want (%d, %q)", id, source, tc.wantID, tc.wantSource)
			}
		})
	}
}

func TestResolveNodeIDHostnameError(t *testing.T) {
	failing := func() (string, error) { return "", errors.New("no hostname") }
	if _, _, err := resolveNodeID(0, envOf(""), failing); err == nil {
		t.Fatal("want error when NODE_ID is unset and hostname lookup fails")
	}
}
