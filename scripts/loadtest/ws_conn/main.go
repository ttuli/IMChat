// WebSocket 连接数压测脚本
//
// 目标：测试网关（websocket-gateway）能同时承载多少条长连接。
// 流程：读取账号或 token -> 批量登录换取 token -> 按速率爬升建立 WS 连接 ->
// 保持一段时间（读循环消费服务端 ping/消息，维持连接）-> 输出建连成功/失败、
// 在线峰值、建连延迟分布。
//
// 关键约束：网关对同一 userID 的新连接会踢掉旧连接，且登录会递增会话 version
// 使旧 token 的 WS 鉴权失效。因此每条并发连接必须使用「不同账号」。请通过
// -accounts 提供足量的已注册账号，或用 -tokens 提供预先签发、互不冲突的 token。
//
// 用法示例：
//
//	# accounts.csv 每行： 账号(数字用户ID),密码[,设备ID]
//	go run ./scripts/loadtest/ws_conn \
//	    -host 127.0.0.1:8888 -path /ws \
//	    -login-url http://127.0.0.1:8888 \
//	    -accounts accounts.csv -n 5000 -rate 300 -hold 60s
//
//	# 直接使用 token 列表（每行一个 AT），跳过登录
//	go run ./scripts/loadtest/ws_conn -host 127.0.0.1:8888 -tokens tokens.txt -n 2000
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	flagHost     = flag.String("host", "127.0.0.1:8888", "网关地址 host:port")
	flagPath     = flag.String("path", "/ws", "WebSocket 路径")
	flagTLS      = flag.Bool("tls", false, "使用 wss（TLS）")
	flagInsecure = flag.Bool("insecure", false, "跳过 TLS 证书校验")
	flagLoginURL = flag.String("login-url", "", "登录服务基址，如 http://127.0.0.1:8888；用 -accounts 时必填")
	flagAccounts = flag.String("accounts", "", "账号文件：每行 账号,密码[,设备ID]")
	flagTokens   = flag.String("tokens", "", "token 文件：每行一个 AccessToken（与 -accounts 二选一）")
	flagNum      = flag.Int("n", 0, "目标连接数；0 表示使用全部账号/token")
	flagRate     = flag.Int("rate", 200, "建连爬升速率（条/秒）")
	flagHold     = flag.Duration("hold", 30*time.Second, "全部建连后保持时长")
	flagDialTO   = flag.Duration("dial-timeout", 10*time.Second, "单条连接握手超时")
	flagLoginTO  = flag.Duration("login-timeout", 10*time.Second, "单次登录超时")
	flagLoginC   = flag.Int("login-c", 50, "登录阶段并发数")
)

// 运行期计数器
var (
	established atomic.Int64 // 累计建连成功
	failed      atomic.Int64 // 累计建连失败
	active      atomic.Int64 // 当前在线
	peakActive  atomic.Int64 // 在线峰值

	failMu    sync.Mutex
	failKinds = map[string]int64{}

	connMu sync.Mutex
	conns  []*websocket.Conn // 保存以便结束时统一关闭

	dialMu   sync.Mutex
	dialLats []time.Duration
)

type credential struct {
	account  string
	password string
	deviceID string
	token    string // 直接提供 token 时填充
}

func main() {
	flag.Parse()

	creds, err := loadCredentials()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载账号/token 失败: %v\n", err)
		os.Exit(1)
	}
	if len(creds) == 0 {
		fmt.Fprintln(os.Stderr, "没有可用的账号或 token")
		os.Exit(1)
	}
	n := *flagNum
	if n <= 0 || n > len(creds) {
		n = len(creds)
	}
	creds = creds[:n]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，正在关闭所有连接...")
		cancel()
	}()

	// 阶段一：登录换 token（若已提供 token 则跳过）
	tokens := resolveTokens(ctx, creds)
	if len(tokens) == 0 {
		fmt.Fprintln(os.Stderr, "没有拿到任何有效 token，无法建连")
		os.Exit(1)
	}
	fmt.Printf("准备建连：目标 %d 条，有效 token %d 个\n", n, len(tokens))

	// 阶段二：按速率爬升建连
	fmt.Println("========== 开始建立 WS 连接 ==========")
	stopProgress := make(chan struct{})
	go progressLoop(stopProgress)

	var wg sync.WaitGroup
	rate := *flagRate
	if rate < 1 {
		rate = 1
	}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
loop:
	for _, tk := range tokens {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			dialOne(ctx, token)
		}(tk)
	}
	wg.Wait() // 等待全部拨号发起完成（读循环仍在后台维持连接）
	rampCost := time.Since(start)
	close(stopProgress)

	fmt.Printf("\n建连爬升完成：耗时 %.1fs，成功 %d，失败 %d，当前在线 %d\n",
		rampCost.Seconds(), established.Load(), failed.Load(), active.Load())

	// 阶段三：保持
	if active.Load() > 0 && ctx.Err() == nil {
		fmt.Printf("保持连接 %s（观察服务端是否踢连/掉线）...\n", *flagHold)
		holdCtx, holdCancel := context.WithTimeout(ctx, *flagHold)
		holdLoop(holdCtx)
		holdCancel()
	}

	// 阶段四：收尾
	closeAll()
	report(rampCost)
}

// dialOne 建立单条 WS 连接并启动读循环维持它。
func dialOne(ctx context.Context, token string) {
	scheme := "ws"
	if *flagTLS {
		scheme = "wss"
	}
	u := url.URL{Scheme: scheme, Host: *flagHost, Path: *flagPath, RawQuery: "token=" + url.QueryEscape(token)}

	dialer := websocket.Dialer{
		HandshakeTimeout: *flagDialTO,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: *flagInsecure},
	}

	t0 := time.Now()
	c, resp, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		failed.Add(1)
		recordFail(dialErrKind(err, resp))
		return
	}
	cost := time.Since(t0)
	dialMu.Lock()
	dialLats = append(dialLats, cost)
	dialMu.Unlock()

	established.Add(1)
	cur := active.Add(1)
	updatePeak(cur)

	connMu.Lock()
	conns = append(conns, c)
	connMu.Unlock()

	// 读循环：消费服务端消息与 ping（gorilla 默认自动回 pong），
	// 出错即认为连接断开。
	go func() {
		defer func() {
			active.Add(-1)
			c.Close()
		}()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				if ctx.Err() == nil { // 非主动关闭
					recordFail("read: " + dialErrKind(err, nil))
				}
				return
			}
		}
	}()
}

// holdLoop 保持阶段，仅打印在线数直到超时或被取消。
func holdLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\r在线=%d  峰值=%d  掉线=%d          ", active.Load(), peakActive.Load(), droppedCount())
		}
	}
}

func closeAll() {
	connMu.Lock()
	defer connMu.Unlock()
	for _, c := range conns {
		_ = c.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		c.Close()
	}
}

// ---------- token 获取 ----------

func resolveTokens(ctx context.Context, creds []credential) []string {
	// 已直接提供 token
	if creds[0].token != "" {
		out := make([]string, 0, len(creds))
		for _, c := range creds {
			if c.token != "" {
				out = append(out, c.token)
			}
		}
		return out
	}

	if *flagLoginURL == "" {
		fmt.Fprintln(os.Stderr, "使用 -accounts 时必须提供 -login-url")
		os.Exit(2)
	}

	fmt.Printf("========== 批量登录 %d 个账号 ==========\n", len(creds))
	client := &http.Client{Timeout: *flagLoginTO}
	tokens := make([]string, len(creds))
	var loginOK, loginFail atomic.Int64

	sem := make(chan struct{}, *flagLoginC)
	var wg sync.WaitGroup
	for i := range creds {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			tk, err := login(ctx, client, *flagLoginURL, creds[idx])
			if err != nil {
				loginFail.Add(1)
				recordFail("login: " + classifyLoginErr(err))
				return
			}
			tokens[idx] = tk
			loginOK.Add(1)
		}(i)
	}
	wg.Wait()
	fmt.Printf("登录完成：成功 %d，失败 %d\n", loginOK.Load(), loginFail.Load())

	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

type loginResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Token        string `json:"token"`
		RefreshToken string `json:"refresh_token"`
	} `json:"data"`
}

func login(ctx context.Context, client *http.Client, baseURL string, c credential) (string, error) {
	deviceID := c.deviceID
	if deviceID == "" {
		deviceID = "loadtest-" + c.account
	}
	// account 为数字用户ID，直接以数字形式写入 JSON
	body := fmt.Sprintf(`{"account":%s,"password":%q,"device_id":%q,"remeber_me":false}`,
		c.account, c.password, deviceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/auth/login", strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))

	var lr loginResponse
	if err := json.Unmarshal(raw, &lr); err != nil {
		return "", fmt.Errorf("解析登录响应失败(status=%d): %s", resp.StatusCode, truncate(string(raw), 120))
	}
	if lr.Data.Token == "" {
		return "", fmt.Errorf("登录未返回 token(code=%d msg=%s)", lr.Code, lr.Message)
	}
	return lr.Data.Token, nil
}

// ---------- 账号/token 文件加载 ----------

func loadCredentials() ([]credential, error) {
	if *flagTokens != "" {
		lines, err := readLines(*flagTokens)
		if err != nil {
			return nil, err
		}
		creds := make([]credential, 0, len(lines))
		for _, ln := range lines {
			creds = append(creds, credential{token: ln})
		}
		return creds, nil
	}
	if *flagAccounts == "" {
		return nil, fmt.Errorf("必须提供 -accounts 或 -tokens")
	}
	lines, err := readLines(*flagAccounts)
	if err != nil {
		return nil, err
	}
	creds := make([]credential, 0, len(lines))
	for _, ln := range lines {
		parts := strings.Split(ln, ",")
		if len(parts) < 2 {
			return nil, fmt.Errorf("账号行格式错误(应为 账号,密码[,设备ID]): %q", ln)
		}
		c := credential{
			account:  strings.TrimSpace(parts[0]),
			password: strings.TrimSpace(parts[1]),
		}
		if len(parts) >= 3 {
			c.deviceID = strings.TrimSpace(parts[2])
		}
		creds = append(creds, c)
	}
	return creds, nil
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		ln := strings.TrimSpace(sc.Text())
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		out = append(out, ln)
	}
	return out, sc.Err()
}

// ---------- 统计与输出 ----------

func updatePeak(cur int64) {
	for {
		old := peakActive.Load()
		if cur <= old || peakActive.CompareAndSwap(old, cur) {
			return
		}
	}
}

func droppedCount() int64 {
	// 已建连但当前不在线的数量（保持阶段掉线）
	d := established.Load() - active.Load()
	if d < 0 {
		return 0
	}
	return d
}

func recordFail(kind string) {
	failMu.Lock()
	failKinds[kind]++
	failMu.Unlock()
}

func progressLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fmt.Printf("\r建连中：成功=%d  在线=%d  失败=%d          ",
				established.Load(), active.Load(), failed.Load())
		}
	}
}

func report(rampCost time.Duration) {
	fmt.Printf("\n\n========== WS 连接数压测结果 ==========\n")
	fmt.Printf("建连成功   : %d\n", established.Load())
	fmt.Printf("建连失败   : %d\n", failed.Load())
	fmt.Printf("在线峰值   : %d\n", peakActive.Load())
	fmt.Printf("结束时在线 : %d\n", active.Load())
	if rampCost > 0 && established.Load() > 0 {
		fmt.Printf("平均建连速率: %.0f 条/秒\n", float64(established.Load())/rampCost.Seconds())
	}

	dialMu.Lock()
	lats := append([]time.Duration(nil), dialLats...)
	dialMu.Unlock()
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		fmt.Println("\n--- 握手延迟分布 ---")
		fmt.Printf("  P50 : %s\n", pctile(lats, 50).Round(time.Microsecond))
		fmt.Printf("  P90 : %s\n", pctile(lats, 90).Round(time.Microsecond))
		fmt.Printf("  P99 : %s\n", pctile(lats, 99).Round(time.Microsecond))
		fmt.Printf("  最大: %s\n", lats[len(lats)-1].Round(time.Microsecond))
	}

	failMu.Lock()
	defer failMu.Unlock()
	if len(failKinds) > 0 {
		fmt.Println("\n--- 失败/掉线原因 ---")
		type kv struct {
			k string
			v int64
		}
		items := make([]kv, 0, len(failKinds))
		for k, v := range failKinds {
			items = append(items, kv{k, v})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
		for _, it := range items {
			fmt.Printf("  %-28s : %d\n", it.k, it.v)
		}
	}
	fmt.Println("=======================================")
}

func dialErrKind(err error, resp *http.Response) string {
	if resp != nil {
		return fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "refused"): // Unix: connection refused / Windows: actively refused
		return "connection refused"
	case strings.Contains(msg, "reset by peer"), strings.Contains(msg, "connection reset"), strings.Contains(msg, "forcibly closed"):
		return "connection reset"
	case strings.Contains(msg, "too many open files"):
		return "too many open files (本机 fd 上限)"
	case strings.Contains(msg, "EOF"), strings.Contains(msg, "1000"), strings.Contains(msg, "1001"):
		return "closed by server"
	case strings.Contains(msg, "no such host"):
		return "dns error"
	default:
		return truncate(msg, 48)
	}
}

func classifyLoginErr(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "未返回 token"):
		return "鉴权失败(账号/密码错误)"
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"):
		return "timeout"
	case strings.Contains(msg, "refused"): // Unix: connection refused / Windows: actively refused
		return "connection refused"
	default:
		return truncate(msg, 48)
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

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
