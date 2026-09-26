package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"IM2/pkg/appversion"
	"IM2/pkg/proto/transport"

	"google.golang.org/protobuf/proto"
)

type fixedMin appversion.Version

func (f fixedMin) MinVersion() appversion.Version { return appversion.Version(f) }

func serveGate(t *testing.T, min appversion.Version, header string, accept string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	called := false
	h := WithAppVersionGate(fixedMin(min), "https://example.com/download")(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/auth/login", nil)
	if header != "" {
		req.Header.Set(appversion.Header, header)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec, called
}

func TestAppVersionGatePassThrough(t *testing.T) {
	min := appversion.Version{Major: 1, Minor: 2}
	cases := []struct {
		name   string
		min    appversion.Version
		header string
	}{
		{"未设置门槛时不看版本", appversion.Version{}, ""},
		{"等于最低版本", min, "1.2.0"},
		{"高于最低版本", min, "1.10.0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, called := serveGate(t, c.min, c.header, "application/x-protobuf")
			if !called || rec.Code != http.StatusOK {
				t.Fatalf("应放行，实际 code=%d called=%v", rec.Code, called)
			}
		})
	}
}

func TestAppVersionGateRejectsProto(t *testing.T) {
	min := appversion.Version{Major: 1, Minor: 2}
	for _, header := range []string{"1.1.9", "", "garbage"} {
		rec, called := serveGate(t, min, header, "application/x-protobuf")
		if called {
			t.Fatalf("版本 %q 不应放行", header)
		}
		if rec.Code != http.StatusUpgradeRequired {
			t.Fatalf("版本 %q 应返回 426，实际 %d", header, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/x-protobuf" {
			t.Fatalf("Content-Type 应为 protobuf，实际 %q", ct)
		}

		// 按客户端的解法解包：ApiResponse 信封，最低版本在 data 的 JSON 里
		var resp transport.ApiResponse
		if err := proto.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("响应体不是 ApiResponse: %v", err)
		}
		if resp.Code != http.StatusUpgradeRequired {
			t.Fatalf("ApiResponse.code 应为 426，实际 %d", resp.Code)
		}
		if !strings.Contains(resp.Message, "1.2.0") || !strings.Contains(resp.Message, "https://example.com/download") {
			t.Fatalf("提示文案应包含最低版本与下载地址: %q", resp.Message)
		}
		var details map[string]string
		if err := json.Unmarshal(resp.Data, &details); err != nil || details["min_version"] != "1.2.0" {
			t.Fatalf("data 应携带 min_version=1.2.0，实际 %s (err=%v)", resp.Data, err)
		}
	}
}

func TestAppVersionGateRejectsJSON(t *testing.T) {
	rec, called := serveGate(t, appversion.Version{Major: 1, Minor: 2}, "1.0.0", "")
	if called || rec.Code != http.StatusUpgradeRequired {
		t.Fatalf("应返回 426，实际 code=%d called=%v", rec.Code, called)
	}
	var body struct {
		Code    int               `json:"code"`
		Details map[string]string `json:"details"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 响应解析失败: %v", err)
	}
	if body.Code != http.StatusUpgradeRequired || body.Details["min_version"] != "1.2.0" {
		t.Fatalf("JSON 响应内容不符: %s", rec.Body.String())
	}
}
