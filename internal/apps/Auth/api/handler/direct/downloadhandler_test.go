package direct

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/pkg/appversion"
)

func serveDownload(t *testing.T, svcCtx *svc.ServiceContext) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	DownloadHandler(svcCtx)(rec, httptest.NewRequest(http.MethodGet, "/auth/download", nil))
	return rec
}

func newFeed(t *testing.T, handler http.HandlerFunc) (*appversion.ReleaseFeed, string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	feed, err := appversion.NewReleaseFeed(srv.URL+"/nexus/win", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return feed, srv.URL
}

func TestDownloadRedirectsToLatestInstaller(t *testing.T) {
	feed, base := newFeed(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("version: 1.0.1\nfiles:\n  - url: Nexus-Windows-1.0.1-Setup.exe\npath: Nexus-Windows-1.0.1-Setup.exe\n"))
	})

	rec := serveDownload(t, &svc.ServiceContext{ReleaseFeed: feed})
	if rec.Code != http.StatusFound {
		t.Fatalf("期望 302，实际 %d", rec.Code)
	}
	if want, got := base+"/nexus/win/Nexus-Windows-1.0.1-Setup.exe", rec.Header().Get("Location"); got != want {
		t.Fatalf("期望跳转到 %s，实际 %s", want, got)
	}
	// 目标随发版变化，跳转本身不能被缓存
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("期望 Cache-Control: no-store，实际 %q", got)
	}
}

func TestDownloadNotConfigured(t *testing.T) {
	if rec := serveDownload(t, &svc.ServiceContext{}); rec.Code != http.StatusNotFound {
		t.Fatalf("未配置更新源应返回 404，实际 %d", rec.Code)
	}
}

func TestDownloadFeedUnavailable(t *testing.T) {
	feed, _ := newFeed(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	if rec := serveDownload(t, &svc.ServiceContext{ReleaseFeed: feed}); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("更新源不可用且无缓存时应返回 503，实际 %d", rec.Code)
	}
}
