package direct

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/pkg/appversion"

	"github.com/zeromicro/go-zero/rest/router"
)

// updateRouter 按线上的注册方式挂载 UpdateHandler，连同 go-zero 的路径参数解析一起测
func updateRouter(t *testing.T, svcCtx *svc.ServiceContext) http.Handler {
	t.Helper()
	rt := router.NewRouter()
	if err := rt.Handle(http.MethodGet, "/auth/update/:file", UpdateHandler(svcCtx)); err != nil {
		t.Fatal(err)
	}
	return rt
}

func serveUpdate(handler http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func mustReleaseFeed(t *testing.T, feedURL string) *appversion.ReleaseFeed {
	t.Helper()
	feed, err := appversion.NewReleaseFeed(feedURL, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return feed
}

func TestUpdateRedirectsIntoFeed(t *testing.T) {
	rt := updateRouter(t, &svc.ServiceContext{ReleaseFeed: mustReleaseFeed(t, "https://oss.example.com/nexus/win")})

	cases := []struct {
		target string
		want   string
	}{
		// electron-updater 查清单时带的 noCache 要带到存储端，其他参数丢弃
		{"/auth/update/latest.yml?noCache=1jb3k8&x-oss-process=image", "https://oss.example.com/nexus/win/latest.yml?noCache=1jb3k8"},
		{"/auth/update/Nexus-Windows-1.0.1-Setup.exe", "https://oss.example.com/nexus/win/Nexus-Windows-1.0.1-Setup.exe"},
		{"/auth/update/Nexus-Windows-1.0.0-Setup.exe.blockmap", "https://oss.example.com/nexus/win/Nexus-Windows-1.0.0-Setup.exe.blockmap"},
	}
	for _, c := range cases {
		rec := serveUpdate(rt, c.target)
		if rec.Code != http.StatusFound {
			t.Fatalf("%s: 期望 302，实际 %d", c.target, rec.Code)
		}
		if got := rec.Header().Get("Location"); got != c.want {
			t.Fatalf("%s: 期望跳转到 %s，实际 %s", c.target, c.want, got)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("%s: 期望 Cache-Control: no-store，实际 %q", c.target, got)
		}
	}
}

func TestUpdateRejectsOtherFiles(t *testing.T) {
	rt := updateRouter(t, &svc.ServiceContext{ReleaseFeed: mustReleaseFeed(t, "https://oss.example.com/nexus/win")})
	for _, target := range []string{"/auth/update/config.json", "/auth/update/.latest.yml", "/auth/update/..%2Flatest.yml"} {
		if rec := serveUpdate(rt, target); rec.Code != http.StatusNotFound {
			t.Fatalf("%s: 期望 404，实际 %d", target, rec.Code)
		}
	}
}

func TestUpdateNotConfigured(t *testing.T) {
	if rec := serveUpdate(updateRouter(t, &svc.ServiceContext{}), "/auth/update/latest.yml"); rec.Code != http.StatusNotFound {
		t.Fatalf("未配置更新源应返回 404，实际 %d", rec.Code)
	}
}

// 用标准 HTTP 客户端走一遍完整跳转：请求 /auth/update 下的文件，最终拿到存储端的内容
func TestUpdateEndToEnd(t *testing.T) {
	var gotQuery string
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nexus/win/latest.yml" {
			http.NotFound(w, r)
			return
		}
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte("version: 1.0.1\n"))
	}))
	t.Cleanup(storage.Close)

	api := httptest.NewServer(updateRouter(t, &svc.ServiceContext{ReleaseFeed: mustReleaseFeed(t, storage.URL+"/nexus/win")}))
	t.Cleanup(api.Close)

	resp, err := http.Get(api.URL + "/auth/update/latest.yml?noCache=1jb3k8")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "version: 1.0.1\n" {
		t.Fatalf("期望跟随跳转拿到存储端的 latest.yml，实际 %d %q", resp.StatusCode, body)
	}
	if gotQuery != "noCache=1jb3k8" {
		t.Fatalf("存储端应收到 noCache 参数，实际 %q", gotQuery)
	}
}
