// WebSocket 连接数压测脚本
//
// 目标：测试网关（websocket-gateway）能同时承载多少条长连接。
// 流程：自动生成批量虚拟账号和设备ID -> 直接调用 TokenManager 生成 Token 并注册 session 到 Redis ->
// 按速率爬升建立 WS 连接 -> 保持一段时间（读循环消费服务端 ping/消息，维持连接）-> 
// 输出建连成功/失败、在线峰值、建连延迟分布。
//
// 关键约束：网关对同一 userID 的新连接会踢掉旧连接。因此每条并发连接必须使用「不同账号」。
// 本脚本会通过配置自动递增生成 UserID 和对应的 Token，完美解决踢连问题并免去了注册账号的麻烦。
//
// 用法示例：
//
//	# 执行前请确保已配置好 config.yaml 并在项目根目录存在 .env
//	go run .
//
package main

import (
	"context"
	"crypto/tls"

	"fmt"
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
	"github.com/joho/godotenv"
	zredis "github.com/zeromicro/go-zero/core/stores/redis"
	"gopkg.in/yaml.v3"

	tokenmanager "IM2/pkg/tokenManager"
)

// ---------- 配置结构 ----------

type Config struct {
	Host     string `yaml:"host"`
	Path     string `yaml:"path"`
	TLS      bool   `yaml:"tls"`
	Insecure bool   `yaml:"insecure"`

	JWTExpire int64  `yaml:"jwt_expire"`

	Num         int    `yaml:"num"`
	StartUserID uint64 `yaml:"start_user_id"`
	Rate        int    `yaml:"rate"`
	Hold        string `yaml:"hold"`
	DialTimeout string `yaml:"dial_timeout"`

	// 解析后的 duration，不写入 YAML
	holdDur        time.Duration
	dialTimeoutDur time.Duration
}

func (c *Config) parseDurations() error {
	var err error
	if c.holdDur, err = time.ParseDuration(c.Hold); err != nil {
		return fmt.Errorf("hold 格式错误: %w", err)
	}
	if c.dialTimeoutDur, err = time.ParseDuration(c.DialTimeout); err != nil {
		return fmt.Errorf("dial_timeout 格式错误: %w", err)
	}
	return nil
}

func loadConfig() (Config, error) {
	var cfg Config
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return cfg, fmt.Errorf("读取配置文件 config.yaml 失败: %w", err)
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
	if cfg.Rate <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: rate (必须 > 0)")
	}
	if cfg.Hold == "" {
		return cfg, fmt.Errorf("配置项不能为空: hold")
	}
	if cfg.DialTimeout == "" {
		return cfg, fmt.Errorf("配置项不能为空: dial_timeout")
	}
	if cfg.JWTExpire <= 0 {
		return cfg, fmt.Errorf("配置项无效或为空: jwt_expire (必须 > 0)")
	}

	if err := cfg.parseDurations(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// ---------- 运行期计数器 ----------

var (
	established atomic.Int64
	failed      atomic.Int64
	active      atomic.Int64
	peakActive  atomic.Int64

	failMu    sync.Mutex
	failKinds = map[string]int64{}

	connMu sync.Mutex
	conns  []*websocket.Conn

	dialMu   sync.Mutex
	dialLats []time.Duration
)

// ---------- 主流程 ----------

func main() {

	// 1. 加载环境变量
	_ = godotenv.Load(".env") // 尝试加载当前目录的 .env
	_ = godotenv.Load("../../../.env") // 尝试加载项目根目录的 .env

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}
	if cfg.Num <= 0 {
		fmt.Fprintln(os.Stderr, "目标连接数(num)必须大于0")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，正在关闭所有连接...")
		cancel()
	}()

	// 2. 生成 Token 并写入 Redis
	tokens, tm := generateAndRegisterTokens(ctx, cfg)
	if len(tokens) == 0 {
		fmt.Fprintln(os.Stderr, "没有生成任何有效 token，无法建连")
		os.Exit(1)
	}
	fmt.Printf("准备建连：目标 %d 条，有效 token %d 个\n", cfg.Num, len(tokens))

	// 3. 按速率爬升建连
	fmt.Println("========== 开始建立 WS 连接 ==========")
	stopProgress := make(chan struct{})
	go progressLoop(stopProgress)

	var wg sync.WaitGroup
	rate := cfg.Rate
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
			dialOne(ctx, cfg, token)
		}(tk)
	}
	wg.Wait()
	rampCost := time.Since(start)
	close(stopProgress)

	fmt.Printf("\n建连爬升完成：耗时 %.1fs，成功 %d，失败 %d，当前在线 %d\n",
		rampCost.Seconds(), established.Load(), failed.Load(), active.Load())

	// 4. 保持
	if active.Load() > 0 && ctx.Err() == nil {
		fmt.Printf("保持连接 %s（观察服务端是否踢连/掉线）...\n", cfg.holdDur)
		holdCtx, holdCancel := context.WithTimeout(ctx, cfg.holdDur)
		holdLoop(holdCtx)
		holdCancel()
	}

	// 5. 收尾
	closeAll()
	cleanupTokens(context.Background(), tm, cfg)
	report(rampCost)
}

// ---------- Token 生成与清理 ----------

func generateAndRegisterTokens(ctx context.Context, cfg Config) ([]string, *tokenmanager.TokenManager) {
	redisHost := os.Getenv("REDIS_TOKEN_HOST")
	redisPass := os.Getenv("REDIS_TOKEN_PASS")
	jwtSecret := os.Getenv("JWT_SECRET")

	if redisHost == "" || jwtSecret == "" {
		fmt.Println("未读取到 REDIS_TOKEN_HOST 或 JWT_SECRET 环境变量，请确保 .env 文件存在并且已配置")
		os.Exit(1)
	}

	tmConfig := tokenmanager.TokenConfig{
		RedisConf: zredis.RedisConf{
			Host: redisHost,
			Pass: redisPass,
			Type: "node",
		},
	}
	tmConfig.JWTConfig.Secret = jwtSecret
	tmConfig.JWTConfig.Expire = cfg.JWTExpire
	tmConfig.JWTConfig.RefreshExpire = cfg.JWTExpire + 86400

	tm := tokenmanager.NewTokenManager(tmConfig)

	fmt.Printf("========== 批量生成并注册 %d 个账号的 Token ==========\n", cfg.Num)
	
	tokens := make([]string, cfg.Num)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 100) // 限制并发数为 100
	var successCount atomic.Int64

	for i := 0; i < cfg.Num; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			userID := cfg.StartUserID + uint64(idx)
			deviceID := fmt.Sprintf("device%02d", idx+1)

			// 调用 tokenmanager.OnLogin()：
			// 1. 写 session:{userID} 
			// 2. 生成带 version 的 AT
			accessToken, _, err := tm.OnLogin(ctx, userID, tokenmanager.LoginOptions{
				MachineID:  deviceID,
				RememberMe: false, // 压测时默认 false 即可
			})
			
			if err != nil {
				fmt.Fprintf(os.Stderr, "生成 Token 失败 (userID=%d): %v\n", userID, err)
				return
			}
			tokens[idx] = accessToken
			successCount.Add(1)
		}(i)
	}
	wg.Wait()

	// 过滤掉生成失败的空 token
	validTokens := make([]string, 0, successCount.Load())
	for _, tk := range tokens {
		if tk != "" {
			validTokens = append(validTokens, tk)
		}
	}
	
	fmt.Printf("Token 生成完毕：成功 %d 个\n", len(validTokens))
	return validTokens, tm
}

func cleanupTokens(ctx context.Context, tm *tokenmanager.TokenManager, cfg Config) {
	fmt.Printf("\n清理 Redis 测试数据中 (共 %d 个账号)...\n", cfg.Num)
	
	keys := make([]string, cfg.Num)
	for i := 0; i < cfg.Num; i++ {
		userID := cfg.StartUserID + uint64(i)
		keys[i] = fmt.Sprintf("session:%d", userID)
	}

	_, err := tm.DelCtx(ctx, keys...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "批量清理 Redis 数据失败: %v\n", err)
		return
	}
	fmt.Println("Redis 测试数据清理完毕。")
}

// ---------- 连接建立与维持 ----------

func dialOne(ctx context.Context, cfg Config, token string) {
	scheme := "ws"
	if cfg.TLS {
		scheme = "wss"
	}
	u := url.URL{Scheme: scheme, Host: cfg.Host, Path: cfg.Path, RawQuery: "token=" + url.QueryEscape(token)}

	dialer := websocket.Dialer{
		HandshakeTimeout: cfg.dialTimeoutDur,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: cfg.Insecure},
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

	go func() {
		defer func() {
			active.Add(-1)
			c.Close()
		}()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				if ctx.Err() == nil { 
					recordFail("read: " + dialErrKind(err, nil))
				}
				return
			}
		}
	}()
}

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

func updatePeak(cur int64) {
	for {
		old := peakActive.Load()
		if cur <= old || peakActive.CompareAndSwap(old, cur) {
			return
		}
	}
}

func droppedCount() int64 {
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
	case strings.Contains(msg, "refused"):
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
