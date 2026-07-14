// HTTP 接口压测脚本
//
// 通用 HTTP 压测器：可对任意 HTTP(S) 接口按「固定并发 + 固定请求数」或
// 「固定并发 + 持续时长」两种模式施压，实时输出 QPS/错误数，结束后打印
// 成功率、状态码分布与延迟百分位（P50/P90/P95/P99）。
//
// 用法示例：
//
//	# 压测登录接口 30 秒，200 并发（account 为已注册的数字用户ID）
//	go run ./scripts/loadtest/http_bench \
//	    -url http://127.0.0.1:8888/auth/login -method POST \
//	    -body '{"account":10001,"password":"123456","device_id":"bench","remeber_me":false}' \
//	    -c 200 -d 30s
//
//	# 压测需要鉴权的接口，固定 5 万次请求
//	go run ./scripts/loadtest/http_bench \
//	    -url 'http://127.0.0.1:8889/user/info?id=10001' \
//	    -token "$AT" -c 100 -n 50000
//
//	# 请求体从文件读取
//	go run ./scripts/loadtest/http_bench -url http://127.0.0.1:8888/auth/login \
//	    -method POST -body @login.json -c 100 -d 20s
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// headerFlag 支持重复传入 -H "Key: Value"
type headerFlag []string

func (h *headerFlag) String() string { return strings.Join(*h, ", ") }
func (h *headerFlag) Set(v string) error {
	*h = append(*h, v)
	return nil
}

var (
	flagURL         = flag.String("url", "", "目标 URL（必填），如 http://127.0.0.1:8888/auth/login")
	flagMethod      = flag.String("method", "GET", "HTTP 方法：GET/POST/PUT/DELETE ...")
	flagBody        = flag.String("body", "", "请求体；以 @ 开头表示从文件读取，如 @body.json")
	flagConc        = flag.Int("c", 50, "并发数（worker 数量）")
	flagNum         = flag.Int64("n", 0, "总请求数；>0 时按固定请求数压测（与 -d 二选一）")
	flagDur         = flag.Duration("d", 0, "压测持续时间，如 30s；-n 为 0 时生效，默认 10s")
	flagTimeout     = flag.Duration("timeout", 10*time.Second, "单请求超时")
	flagToken       = flag.String("token", "", "Bearer Token，自动附加 Authorization 头")
	flagKeepAlive   = flag.Bool("keepalive", true, "是否复用连接（HTTP keep-alive）")
	flagInsecure    = flag.Bool("insecure", false, "跳过 TLS 证书校验")
	flagThink       = flag.Duration("think", 0, "每个 worker 相邻两次请求间的思考时间")
	flagContentType = flag.String("content-type", "application/json", "Content-Type 头（body 非空时生效）")
	headers         headerFlag
)

// workerResult 单个 worker 的统计结果，最后统一汇总，避免运行期锁竞争。
type workerResult struct {
	latencies []time.Duration
	status    map[int]int64 // HTTP 状态码 -> 次数
	netErrs   int64         // 网络层错误（连接失败、超时等）
	errKinds  map[string]int64
}

// 全局实时计数器（仅用于进度打印）
var (
	doneCount atomic.Int64
	errCount  atomic.Int64
)

func main() {
	flag.Var(&headers, "H", "自定义请求头，可重复，如 -H 'X-Trace: 1'")
	flag.Parse()

	if *flagURL == "" {
		fmt.Fprintln(os.Stderr, "错误：必须通过 -url 指定目标地址")
		flag.Usage()
		os.Exit(2)
	}

	body, err := loadBody(*flagBody)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取请求体失败: %v\n", err)
		os.Exit(1)
	}

	// 压测模式：优先按固定请求数；否则按时长，时长默认 10s。
	useNum := *flagNum > 0
	duration := *flagDur
	if !useNum && duration <= 0 {
		duration = 10 * time.Second
	}

	client := newHTTPClient()

	// 支持 Ctrl-C 提前结束并打印已有结果
	ctx, cancel := context.WithCancel(context.Background())
	if !useNum {
		var toCancel context.CancelFunc
		ctx, toCancel = context.WithTimeout(ctx, duration)
		defer toCancel()
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，正在收尾...")
		cancel()
	}()

	printHeader(useNum, duration)

	var remaining atomic.Int64
	remaining.Store(*flagNum)

	results := make([]workerResult, *flagConc)
	var wg sync.WaitGroup
	start := time.Now()

	// 实时进度打印
	stopProgress := make(chan struct{})
	go progressLoop(start, stopProgress)

	for i := 0; i < *flagConc; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = runWorker(ctx, client, body, useNum, &remaining)
		}(i)
	}

	wg.Wait()
	close(stopProgress)
	elapsed := time.Since(start)

	report(results, elapsed)
}

// runWorker 持续发起请求，直到达到请求数预算或 context 结束。
func runWorker(ctx context.Context, client *http.Client, body []byte, useNum bool, remaining *atomic.Int64) workerResult {
	res := workerResult{
		status:   make(map[int]int64),
		errKinds: make(map[string]int64),
	}
	for {
		if useNum {
			if remaining.Add(-1) < 0 {
				return res
			}
		} else {
			select {
			case <-ctx.Done():
				return res
			default:
			}
		}

		lat, code, err := doRequest(ctx, client, body)
		if err != nil {
			// context 取消导致的错误不计入业务错误
			if ctx.Err() != nil {
				return res
			}
			res.netErrs++
			res.errKinds[classifyErr(err)]++
			errCount.Add(1)
			doneCount.Add(1)
			continue
		}
		res.latencies = append(res.latencies, lat)
		res.status[code]++
		if code < 200 || code >= 400 {
			errCount.Add(1)
		}
		doneCount.Add(1)

		if *flagThink > 0 {
			select {
			case <-ctx.Done():
				return res
			case <-time.After(*flagThink):
			}
		}
	}
}

// doRequest 发起单次请求并返回耗时与状态码，响应体被读尽以复用连接。
func doRequest(ctx context.Context, client *http.Client, body []byte) (time.Duration, int, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(*flagMethod), *flagURL, reader)
	if err != nil {
		return 0, 0, err
	}
	if len(body) > 0 && *flagContentType != "" {
		req.Header.Set("Content-Type", *flagContentType)
	}
	if *flagToken != "" {
		req.Header.Set("Authorization", "Bearer "+*flagToken)
	}
	for _, h := range headers {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}

	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	// 读尽并关闭 body，才能复用底层 TCP 连接
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return time.Since(t0), resp.StatusCode, nil
}

func newHTTPClient() *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: *flagConc + 16,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   !*flagKeepAlive,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: *flagInsecure},
	}
	return &http.Client{Transport: tr, Timeout: *flagTimeout}
}

func loadBody(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if strings.HasPrefix(s, "@") {
		return os.ReadFile(s[1:])
	}
	return []byte(s), nil
}

func classifyErr(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "Timeout"), strings.Contains(msg, "i/o timeout"):
		return "timeout"
	case strings.Contains(msg, "refused"): // Unix: connection refused / Windows: actively refused
		return "connection refused"
	case strings.Contains(msg, "reset by peer"), strings.Contains(msg, "connection reset"), strings.Contains(msg, "forcibly closed"):
		return "connection reset"
	case strings.Contains(msg, "EOF"):
		return "EOF"
	case strings.Contains(msg, "no such host"):
		return "dns error"
	case strings.Contains(msg, "too many open files"):
		return "too many open files"
	default:
		return "other"
	}
}

func printHeader(useNum bool, duration time.Duration) {
	fmt.Println("========== HTTP 压测开始 ==========")
	fmt.Printf("目标   : %s %s\n", strings.ToUpper(*flagMethod), *flagURL)
	fmt.Printf("并发   : %d\n", *flagConc)
	if useNum {
		fmt.Printf("模式   : 固定请求数 n=%d\n", *flagNum)
	} else {
		fmt.Printf("模式   : 固定时长 d=%s\n", duration)
	}
	fmt.Printf("超时   : %s | keep-alive: %v\n", *flagTimeout, *flagKeepAlive)
	fmt.Println("-----------------------------------")
}

func progressLoop(start time.Time, stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			cur := doneCount.Load()
			elapsed := time.Since(start).Seconds()
			fmt.Printf("\r[%5.0fs] 已完成=%d  瞬时QPS=%d  累计QPS=%.0f  错误=%d      ",
				elapsed, cur, cur-last, float64(cur)/elapsed, errCount.Load())
			last = cur
		}
	}
}

func report(results []workerResult, elapsed time.Duration) {
	var (
		all      []time.Duration
		total    int64
		netErrs  int64
		status   = map[int]int64{}
		errKinds = map[string]int64{}
	)
	for _, r := range results {
		all = append(all, r.latencies...)
		netErrs += r.netErrs
		for c, n := range r.status {
			status[c] += n
			total += n
		}
		for k, n := range r.errKinds {
			errKinds[k] += n
		}
	}
	total += netErrs

	fmt.Printf("\n\n========== HTTP 压测结果 ==========\n")
	fmt.Printf("总耗时     : %.2fs\n", elapsed.Seconds())
	fmt.Printf("总请求     : %d\n", total)
	fmt.Printf("吞吐(QPS)  : %.1f\n", float64(total)/elapsed.Seconds())

	var ok2xx int64
	for c, n := range status {
		if c >= 200 && c < 300 {
			ok2xx += n
		}
	}
	fmt.Printf("成功(2xx)  : %d (%.2f%%)\n", ok2xx, pctOf(ok2xx, total))
	fmt.Printf("网络错误   : %d\n", netErrs)

	if len(status) > 0 {
		fmt.Println("\n--- 状态码分布 ---")
		codes := make([]int, 0, len(status))
		for c := range status {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		for _, c := range codes {
			fmt.Printf("  %d : %d (%.2f%%)\n", c, status[c], pctOf(status[c], total))
		}
	}
	if len(errKinds) > 0 {
		fmt.Println("\n--- 错误类型分布 ---")
		for k, n := range errKinds {
			fmt.Printf("  %-20s : %d\n", k, n)
		}
	}

	if len(all) > 0 {
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		var sum time.Duration
		for _, d := range all {
			sum += d
		}
		fmt.Println("\n--- 延迟分布（仅统计有响应的请求）---")
		fmt.Printf("  样本   : %d\n", len(all))
		fmt.Printf("  最小   : %s\n", all[0].Round(time.Microsecond))
		fmt.Printf("  平均   : %s\n", (sum / time.Duration(len(all))).Round(time.Microsecond))
		fmt.Printf("  P50    : %s\n", pctile(all, 50).Round(time.Microsecond))
		fmt.Printf("  P90    : %s\n", pctile(all, 90).Round(time.Microsecond))
		fmt.Printf("  P95    : %s\n", pctile(all, 95).Round(time.Microsecond))
		fmt.Printf("  P99    : %s\n", pctile(all, 99).Round(time.Microsecond))
		fmt.Printf("  最大   : %s\n", all[len(all)-1].Round(time.Microsecond))
	}
	fmt.Println("===================================")
}

func pctOf(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}

// pctile 线性插值百分位，sorted 必须已升序排序。
func pctile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := p / 100 * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + time.Duration(frac*float64(sorted[hi]-sorted[lo]))
}
