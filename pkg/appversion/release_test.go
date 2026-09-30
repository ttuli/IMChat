package appversion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// latestYML 按 electron-builder 实际生成的 latest.yml 结构构造
func latestYML(version string) string {
	name := "Nexus-Windows-" + version + "-Setup.exe"
	return "version: " + version + "\n" +
		"files:\n" +
		"  - url: " + name + "\n" +
		"    sha512: oG8UlU3b==\n" +
		"    size: 131272622\n" +
		"path: " + name + "\n" +
		"sha512: oG8UlU3b==\n" +
		"releaseDate: '2026-09-30T06:16:06.845Z'\n"
}

// feedServer 在 /nexus/win/latest.yml 提供更新源内容，可随时换内容、改状态码，并统计请求次数
type feedServer struct {
	*httptest.Server
	content atomic.Value // string
	status  atomic.Int32
	hits    atomic.Int32
}

func newFeedServer(t *testing.T, content string) *feedServer {
	t.Helper()
	s := &feedServer{}
	s.content.Store(content)
	s.status.Store(http.StatusOK)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nexus/win/latest.yml" {
			http.NotFound(w, r)
			return
		}
		s.hits.Add(1)
		if code := int(s.status.Load()); code != http.StatusOK {
			w.WriteHeader(code)
			return
		}
		_, _ = w.Write([]byte(s.content.Load().(string)))
	}))
	t.Cleanup(s.Close)
	return s
}

func mustFeed(t *testing.T, feedURL string) *ReleaseFeed {
	t.Helper()
	f, err := NewReleaseFeed(feedURL, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestReleaseFeedResolvesInstaller(t *testing.T) {
	srv := newFeedServer(t, latestYML("1.0.1"))
	want := srv.URL + "/nexus/win/Nexus-Windows-1.0.1-Setup.exe"

	// 更新源地址末尾有没有 / 都按目录处理
	for _, feedURL := range []string{srv.URL + "/nexus/win", srv.URL + "/nexus/win/"} {
		got, err := mustFeed(t, feedURL).InstallerURL(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", feedURL, err)
		}
		if got != want {
			t.Fatalf("%s: 期望 %s，实际 %s", feedURL, want, got)
		}
	}
}

func TestReleaseFeedInstallerSelection(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string // 相对 /nexus/win/ 或完整地址
	}{
		{"只有旧格式 path", "version: 1.0.1\npath: Nexus-Setup.exe\n", "Nexus-Setup.exe"},
		{"files 里优先取 .exe", "files:\n  - url: nexus-1.0.1-x64.nsis.7z\n  - url: Nexus-Web-Setup.EXE\npath: nexus-1.0.1-x64.nsis.7z\n", "Nexus-Web-Setup.EXE"},
		{"文件名里的空格要转义", "path: Nexus Setup 1.0.1.exe\n", "Nexus%20Setup%201.0.1.exe"},
		{"绝对地址原样使用", "files:\n  - url: https://cdn.example.com/Nexus-Setup.exe\n", "https://cdn.example.com/Nexus-Setup.exe"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := newFeedServer(t, c.content)
			want := c.want
			if !strings.HasPrefix(want, "https://") {
				want = srv.URL + "/nexus/win/" + want
			}
			got, err := mustFeed(t, srv.URL+"/nexus/win").InstallerURL(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("期望 %s，实际 %s", want, got)
			}
		})
	}
}

func TestReleaseFeedCachesAndRefreshes(t *testing.T) {
	srv := newFeedServer(t, latestYML("1.0.1"))
	f := mustFeed(t, srv.URL+"/nexus/win")
	ctx := context.Background()

	first, _ := f.InstallerURL(ctx)
	second, _ := f.InstallerURL(ctx)
	if first != second || srv.hits.Load() != 1 {
		t.Fatalf("ttl 内应只查询一次更新源，实际 %d 次", srv.hits.Load())
	}

	// 发版覆盖 latest.yml，缓存过期后切到新包
	srv.content.Store(latestYML("1.0.2"))
	f.checkedAt = time.Now().Add(-2 * time.Hour)
	got, err := f.InstallerURL(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "/Nexus-Windows-1.0.2-Setup.exe") || srv.hits.Load() != 2 {
		t.Fatalf("过期后应重新查询并拿到新版本，实际 %s（查询 %d 次）", got, srv.hits.Load())
	}
}

func TestReleaseFeedKeepsLastResultOnError(t *testing.T) {
	srv := newFeedServer(t, latestYML("1.0.1"))
	f := mustFeed(t, srv.URL+"/nexus/win")
	ctx := context.Background()

	want, err := f.InstallerURL(ctx)
	if err != nil {
		t.Fatal(err)
	}

	srv.status.Store(http.StatusInternalServerError)
	f.checkedAt = time.Now().Add(-2 * time.Hour)
	got, err := f.InstallerURL(ctx)
	if err != nil || got != want {
		t.Fatalf("更新源故障时应继续使用上次结果 %s，实际 %q, %v", want, got, err)
	}

	// 失败后同样等 ttl 再查，不会每个请求都去打故障中的更新源
	hits := srv.hits.Load()
	_, _ = f.InstallerURL(ctx)
	if srv.hits.Load() != hits {
		t.Fatal("查询失败后 ttl 内不应再次查询更新源")
	}
}

func TestReleaseFeedErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		status  int
	}{
		{"更新源返回 404", "", http.StatusNotFound},
		{"不是合法 YAML", "files: [unclosed\n", http.StatusOK},
		{"没有安装包", "version: 1.0.1\n", http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := newFeedServer(t, c.content)
			srv.status.Store(int32(c.status))
			// 从未成功过，没有可退回的结果，必须报错
			if got, err := mustFeed(t, srv.URL+"/nexus/win").InstallerURL(context.Background()); err == nil {
				t.Fatalf("期望报错，实际返回 %s", got)
			}
		})
	}
}

func TestReleaseFeedFileURL(t *testing.T) {
	f := mustFeed(t, "https://oss.example.com/nexus/win")
	for _, name := range []string{
		"latest.yml",
		"Nexus-Windows-1.0.1-Setup.exe",
		"Nexus-Windows-1.0.0-Setup.exe.blockmap",
		"Nexus-Windows-1.0.1-beta.1-Setup.EXE",
	} {
		got, err := f.FileURL(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if want := "https://oss.example.com/nexus/win/" + name; got != want {
			t.Fatalf("期望 %s，实际 %s", want, got)
		}
	}

	// 只放行更新源里的三类文件，且只能是目录下的单个文件名
	for _, name := range []string{
		"", ".", "..", ".latest.yml", "../latest.yml", "sub/latest.yml", `..\latest.yml`,
		"config.json", "latest.yml.bak", "Nexus-Setup.zip",
	} {
		if got, err := f.FileURL(name); err == nil {
			t.Fatalf("%q 应被拒绝，实际返回 %s", name, got)
		}
	}
}

func TestNewReleaseFeedRejectsInvalidURL(t *testing.T) {
	for _, feedURL := range []string{"", "nexus/win", "/nexus/win", "ftp://example.com/nexus/win", "https://"} {
		if _, err := NewReleaseFeed(feedURL, time.Minute); err == nil {
			t.Fatalf("%q 应被拒绝", feedURL)
		}
	}
}
