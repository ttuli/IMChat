package appversion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"gopkg.in/yaml.v3"
)

const (
	// latestFileName electron-updater generic 更新源中描述最新版本的文件（Windows）
	latestFileName = "latest.yml"
	// maxLatestFileSize latest.yml 通常不到 1KB，带上长篇更新说明也远小于这个上限
	maxLatestFileSize = 1 << 20
	// fetchTimeout 单次查询更新源的超时
	fetchTimeout = 5 * time.Second
)

// feedFileExts electron-updater 会向更新源请求的三类文件：版本清单、安装包、增量下载用的分块索引
var feedFileExts = []string{".yml", ".exe", ".blockmap"}

// ReleaseFeed 客户端 electron-updater 的更新源（generic provider 的目录）：给出最新安装包的地址（InstallerURL），
// 以及更新源内各文件的真实地址（FileURL）。更新源的真实位置只在服务端配置，客户端经 /auth/update 转发访问。
//
// 最新版本只认更新源里的 latest.yml：与客户端自动更新读的是同一份，发版上传完即生效，不必另外维护版本号。
// 结果缓存 ttl；查询失败时继续用上一次的结果，更新源短暂不可用不影响下载入口。
type ReleaseFeed struct {
	base   *url.URL // 更新源目录，保证以 / 结尾
	ttl    time.Duration
	client *http.Client

	mu        sync.Mutex
	installer string    // 上一次解析出的安装包地址
	checkedAt time.Time // 上一次查询更新源的时间，成功失败都算
}

// NewReleaseFeed feedURL 为更新源目录，与客户端 electron-builder 配置的 publish.url 相同，须为 http(s) 绝对地址
func NewReleaseFeed(feedURL string, ttl time.Duration) (*ReleaseFeed, error) {
	base, err := url.Parse(strings.TrimSpace(feedURL))
	if err != nil {
		return nil, fmt.Errorf("更新源地址无效: %w", err)
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("更新源地址须为 http(s) 绝对地址: %q", feedURL)
	}
	// 与 electron-updater 一样按目录处理：不以 / 结尾时，ResolveReference 会把最后一段当成文件名替换掉
	if !strings.HasSuffix(base.Path, "/") {
		base.Path += "/"
	}
	return &ReleaseFeed{
		base:   base,
		ttl:    ttl,
		client: &http.Client{Timeout: fetchTimeout},
	}, nil
}

// InstallerURL 最新安装包的完整地址
func (f *ReleaseFeed) InstallerURL(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.installer != "" && time.Since(f.checkedAt) < f.ttl {
		return f.installer, nil
	}

	// 持锁查询，并发请求等同一次查询的结果，不会一起打到更新源。
	// 脱离单个请求的取消：浏览器断开不该打断这次查询，排队的请求还在等它
	installer, err := f.fetch(context.WithoutCancel(ctx))
	f.checkedAt = time.Now()
	if err != nil {
		if f.installer == "" {
			return "", err
		}
		// 失败同样等 ttl 后再查，更新源故障期间不会每个请求都去重试一遍
		logx.WithContext(ctx).Errorf("[ReleaseFeed] 查询更新源失败，继续使用 %s: %v", f.installer, err)
		return f.installer, nil
	}
	f.installer = installer
	return installer, nil
}

// fetch 下载 latest.yml 并解析出安装包地址
func (f *ReleaseFeed) fetch(ctx context.Context) (string, error) {
	latestURL := f.base.ResolveReference(&url.URL{Path: latestFileName}).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, latestURL, nil)
	if err != nil {
		return "", err
	}
	// latest.yml 每次发版都会被覆盖，要求途经的 CDN、代理回源校验，免得拿到旧版本
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求 %s 失败: %w", latestURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("请求 %s 返回 %d", latestURL, resp.StatusCode)
	}

	content, err := io.ReadAll(io.LimitReader(resp.Body, maxLatestFileSize))
	if err != nil {
		return "", fmt.Errorf("读取 %s 失败: %w", latestURL, err)
	}
	name, err := parseInstallerName(content)
	if err != nil {
		return "", fmt.Errorf("解析 %s 失败: %w", latestURL, err)
	}
	return f.resolve(name)
}

// resolve electron-builder 写的是相对更新源目录的文件名；写成绝对地址的原样使用
func (f *ReleaseFeed) resolve(name string) (string, error) {
	if u, err := url.Parse(name); err == nil && u.IsAbs() {
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", fmt.Errorf("安装包地址协议不支持: %q", name)
		}
		return u.String(), nil
	}
	// 按路径而不是 URL 拼接，文件名里的空格等字符交给 url 包转义
	return f.base.ResolveReference(&url.URL{Path: name}).String(), nil
}

// FileURL 更新源目录下文件 name 的完整地址，供 /auth/update 转发客户端的请求。
// 只接受单个文件名，且限于 feedFileExts 中的类型：不能借它跳到更新源目录以外，也不暴露目录里的其他文件
func (f *ReleaseFeed) FileURL(name string) (string, error) {
	if name == "" || name != path.Base(name) || strings.HasPrefix(name, ".") || strings.Contains(name, `\`) {
		return "", fmt.Errorf("文件名不合法: %q", name)
	}
	if !slices.Contains(feedFileExts, strings.ToLower(path.Ext(name))) {
		return "", fmt.Errorf("不是更新源里的文件: %q", name)
	}
	return f.base.ResolveReference(&url.URL{Path: name}).String(), nil
}

// latestInfo latest.yml 中用到的字段，格式由 electron-builder 生成
type latestInfo struct {
	Files []struct {
		URL string `yaml:"url"`
	} `yaml:"files"`
	// Path 旧格式字段，新版 electron-builder 仍会写，与 files 中的安装包相同
	Path string `yaml:"path"`
}

// parseInstallerName 取 files 中第一个 .exe（与 electron-updater 的 NsisUpdater 选法一致），没有时退回 path
func parseInstallerName(content []byte) (string, error) {
	var info latestInfo
	if err := yaml.Unmarshal(content, &info); err != nil {
		return "", err
	}
	for _, file := range info.Files {
		if strings.HasSuffix(strings.ToLower(file.URL), ".exe") {
			return file.URL, nil
		}
	}
	if info.Path != "" {
		return info.Path, nil
	}
	return "", errors.New("没有找到安装包")
}
