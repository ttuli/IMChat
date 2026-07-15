// WebSocket 连接数压测脚本
//
// 目标：测试网关（websocket-gateway）能同时承载多少条长连接。
// 流程：自动生成批量虚拟账号和设备ID -> 直接调用 TokenManager 生成 Token 并注册 session 到 Redis ->
// 按速率爬升建立 WS 连接 -> 保持一段时间（读循环消费服务端 ping/消息，维持连接）->
// 输出建连成功/失败、在线峰值、掉线/被踢数、建连延迟分布。
//
// 关键约束：网关对同一 userID 的新连接会踢掉旧连接。因此每条并发连接必须使用「不同账号」。
// 本脚本会通过配置自动递增生成 UserID 和对应的 Token，完美解决踢连问题并免去了注册账号的麻烦。
//
// 用法示例：
//
//	# 方式一：进入脚本目录运行（读取当前目录 config.yaml）
//	cd scripts/loadtest/ws_conn && go run .
//
//	# 方式二：在项目根目录运行，用 -config 指定配置
//	go run ./scripts/loadtest/ws_conn -config scripts/loadtest/ws_conn/config.yaml
//
// 需在项目根目录存在 .env（提供 REDIS_TOKEN_HOST / REDIS_TOKEN_PASS / JWT_SECRET）。
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	zredis "github.com/zeromicro/go-zero/core/stores/redis"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"IM2/pkg/proto/transport"
	tokenmanager "IM2/pkg/tokenManager"
)

// ---------- 配置结构 ----------

type Config struct {
	Host     string `yaml:"host"`
	Path     string `yaml:"path"`
	TLS      bool   `yaml:"tls"`
	Insecure bool   `yaml:"insecure"`

	JWTExpire int64 `yaml:"jwt_expire"`

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

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
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
	established atomic.Int64 // 累计建连成功
	failed      atomic.Int64 // 累计建连失败（不含 Ctrl-C 取消的在途拨号）
	canceled    atomic.Int64 // Ctrl-C 中断时被取消的在途拨号（不算失败）
	active      atomic.Int64 // 当前在线
	peakActive  atomic.Int64 // 在线峰值
	dropped     atomic.Int64 // 建连成功后非主动关闭的掉线（含被踢）
	kicked      atomic.Int64 // 被服务端踢下线（USER_KICKOFF）

	// closing 置位后进入主动收尾阶段：读循环的断开不再计入掉线统计
	closing atomic.Bool

	// readWg 跟踪所有读循环，报告前等待其退出，保证计数器已稳定
	readWg sync.WaitGroup

	failMu    sync.Mutex
	failKinds = map[string]int64{}

	connMu sync.Mutex
	conns  []*websocket.Conn

	dialMu   sync.Mutex
	dialLats []time.Duration
)

type noOpLogger struct{}

func (n *noOpLogger) Printf(ctx context.Context, format string, v ...interface{}) {}

// ---------- 主流程 ----------

func main() {
	// 禁用 go-zero 内置的日志与系统统计输出，保持压测控制台界面整洁
	logx.Disable()
	logx.DisableStat()
	// 屏蔽底层 redis 驱动标准日志包的警告输出
	log.SetOutput(io.Discard)
	redis.SetLogger(&noOpLogger{})

	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	// 1. 加载环境变量
	_ = godotenv.Load(".env")          // 当前目录（从项目根目录运行时）
	_ = godotenv.Load("../../../.env") // 项目根目录（cd 进脚本目录运行时）

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
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

	var dialWg sync.WaitGroup
	interval := time.Second / time.Duration(cfg.Rate)
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
		dialWg.Add(1)
		go func(token string) {
			defer dialWg.Done()
			dialOne(ctx, cfg, token)
		}(tk)
	}
	dialWg.Wait()
	rampCost := time.Since(start)
	close(stopProgress)

	fmt.Printf("\n建连爬升完成：耗时 %.1fs，成功 %d，失败 %d，当前在线 %d\n",
		rampCost.Seconds(), established.Load(), failed.Load(), active.Load())
	if failed.Load() > 0 {
		fmt.Printf("主要失败原因: %s\n", topFailures(3))
	}

	// 4. 保持
	if active.Load() > 0 && ctx.Err() == nil {
		fmt.Printf("保持连接 %s（观察服务端是否踢连/掉线）...\n", cfg.holdDur)
		holdCtx, holdCancel := context.WithTimeout(ctx, cfg.holdDur)
		holdLoop(holdCtx)
		holdCancel()
	}

	// 5. 收尾：先快照「保持结束时在线」，再置收尾标志关闭连接，
	// 等读循环全部退出后计数器才稳定，避免报告读到中间值。
	onlineAtEnd := active.Load()
	closing.Store(true)
	closeAll()
	waitReadLoops(5 * time.Second)

	// 给网关一点时间各自处理断连（执行 UnregisterUser），随后再兜底清理路由键残留。
	// 非 Ctrl-C 场景才等待，中断时优先快速退出（清理仍会执行）。
	if ctx.Err() == nil {
		time.Sleep(2 * time.Second)
	}
	cleanupTokens(tm, cfg)
	report(rampCost, onlineAtEnd)
}

// waitReadLoops 等待所有读循环退出（带超时兜底，防止个别连接卡住阻塞报告）。
func waitReadLoops(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		readWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintf(os.Stderr, "警告：等待读循环退出超时(%s)，部分计数可能未归零\n", timeout)
	}
}

// ---------- Token 生成与清理 ----------

func generateAndRegisterTokens(ctx context.Context, cfg Config) ([]string, *tokenmanager.TokenManager) {
	redisHost := os.Getenv("REDIS_TOKEN_HOST")
	redisPass := os.Getenv("REDIS_TOKEN_PASS")
	jwtSecret := os.Getenv("JWT_ACCESS_SECRET")

	if redisHost == "" || jwtSecret == "" {
		fmt.Println("未读取到 REDIS_TOKEN_HOST 或 JWT_ACCESS_SECRET 环境变量，请确保 .env 文件存在并且已配置")
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

	tokens := make([]string, cfg.Num)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 100) // 限制并发数为 100
	var successCount atomic.Int64

	// 失败不逐条刷屏，只记录条数和第一个错误样本
	var genFailCount atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr error

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
				genFailCount.Add(1)
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
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

	if fails := genFailCount.Load(); fails > 0 {
		fmt.Printf("已生成 %d 个 Token（失败 %d 个，示例错误: %v）\n", len(validTokens), fails, firstErr)
	} else {
		fmt.Printf("已生成 %d 个 Token\n", len(validTokens))
	}
	return validTokens, tm
}

// cleanupTokens 删除本次压测写入的 session 键。
// 使用独立的带超时 context（主 ctx 可能已被 Ctrl-C 取消），并分批删除避免超大单命令。
func cleanupTokens(tm *tokenmanager.TokenManager, cfg Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const batchSize = 1000
	var deleted int
	for begin := 0; begin < cfg.Num; begin += batchSize {
		end := begin + batchSize
		if end > cfg.Num {
			end = cfg.Num
		}
		keys := make([]string, 0, (end-begin)*2)
		for i := begin; i < end; i++ {
			id := cfg.StartUserID + uint64(i)
			// 同时清理会话键与网关注册的用户路由键：
			//   session:{id}   —— 本脚本生成 Token 时写入
			//   ws:route:{id}  —— 网关在建连时注册、断连时 best-effort 删除，
			//                     高并发同时断连下常残留个位数，这里按账号区间兜底删除。
			keys = append(keys, fmt.Sprintf("session:%d", id), fmt.Sprintf("ws:route:%d", id))
		}
		n, err := tm.DelCtx(ctx, keys...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n清理 Redis 测试数据失败: %v\n", err)
			return
		}
		deleted += n
	}
	fmt.Printf("\n已清除所有 Redis 测试数据（session + 路由键，实际删除 %d 个 key）\n", deleted)
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
		// Ctrl-C 导致的在途拨号取消不是真实失败，单独计数
		if ctx.Err() != nil {
			canceled.Add(1)
			return
		}
		failed.Add(1)
		recordFail("建连: " + dialErrKind(err, resp))
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

	readWg.Add(1)
	go func() {
		defer readWg.Done()
		defer func() {
			active.Add(-1)
			c.Close()
		}()
		readLoop(ctx, c)
	}()
}

// readLoop 消费服务端下行帧维持连接：
//   - 解析 USER_KICKOFF / ERROR，区分「被踢」与普通掉线；
//   - 主动收尾（closing）或 Ctrl-C（ctx 取消）后的断开不计入掉线统计。
func readLoop(ctx context.Context, c *websocket.Conn) {
	wasKicked := false
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			if closing.Load() || ctx.Err() != nil {
				return // 主动关闭，不算掉线
			}
			dropped.Add(1)
			if wasKicked {
				recordFail("被服务端踢下线 (USER_KICKOFF)")
			} else {
				recordFail("掉线: " + dialErrKind(err, nil))
			}
			return
		}

		var ws transport.WSMessage
		if proto.Unmarshal(data, &ws) != nil {
			continue
		}
		switch ws.Type {
		case transport.MessageType_USER_KICKOFF:
			// 服务端踢连：先下发 KICKOFF 再关闭，标记后由读错误路径统一计数
			kicked.Add(1)
			wasKicked = true
		case transport.MessageType_ERROR:
			var em transport.ErrorMessage
			if proto.Unmarshal(ws.Payload, &em) == nil {
				recordFail(fmt.Sprintf("服务端错误 %d: %s", em.ErrorCode, truncate(em.ErrorMsg, 32)))
			}
		}
	}
}

func holdLoop(ctx context.Context) {
	deadline, _ := ctx.Deadline()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\r在线=%d  峰值=%d  掉线=%d  被踢=%d  剩余=%s      ",
				active.Load(), peakActive.Load(), dropped.Load(), kicked.Load(),
				time.Until(deadline).Round(time.Second))
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

func recordFail(kind string) {
	failMu.Lock()
	failKinds[kind]++
	failMu.Unlock()
}

// topFailures 返回出现次数最多的前 n 类失败原因摘要，如 "timeout=1200, connection refused=300"。
func topFailures(n int) string {
	failMu.Lock()
	defer failMu.Unlock()
	type kv struct {
		k string
		v int64
	}
	items := make([]kv, 0, len(failKinds))
	for k, v := range failKinds {
		items = append(items, kv{k, v})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
	if len(items) > n {
		items = items[:n]
	}
	parts := make([]string, 0, len(items))
	for _, it := range items {
		parts = append(parts, fmt.Sprintf("%s=%d", it.k, it.v))
	}
	return strings.Join(parts, ", ")
}

func progressLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			cur := established.Load()
			cause := ""
			if failed.Load() > 0 {
				// 实时暴露主导失败原因，便于判断「临近目标值失败飙升」的性质
				// （HTTP 503=入口/降载限流，timeout=accept 队列/CPU，reset=fd/LB 上限）
				cause = "  [" + topFailures(1) + "]"
			}
			fmt.Printf("\r建连中：成功=%d(+%d/s)  在线=%d  失败=%d%s          ",
				cur, cur-last, active.Load(), failed.Load(), cause)
			last = cur
		}
	}
}

func report(rampCost time.Duration, onlineAtEnd int64) {
	est := established.Load()
	fmt.Printf("\n\n========== WS 连接数压测结果 ==========\n")
	fmt.Printf("建连成功   : %d\n", est)
	fmt.Printf("建连失败   : %d\n", failed.Load())
	if n := canceled.Load(); n > 0 {
		fmt.Printf("中断取消   : %d（Ctrl-C 时在途拨号，不计入失败）\n", n)
	}
	fmt.Printf("在线峰值   : %d\n", peakActive.Load())
	fmt.Printf("保持结束在线: %d\n", onlineAtEnd)
	fmt.Printf("中途掉线   : %d（其中被踢 %d）\n", dropped.Load(), kicked.Load())
	if est > 0 && onlineAtEnd+dropped.Load() != est {
		// 校验不平通常意味着读循环未全部退出（见 waitReadLoops 超时警告）
		fmt.Printf("校验       : 成功(%d) != 结束在线(%d)+掉线(%d)，差 %d\n",
			est, onlineAtEnd, dropped.Load(), est-onlineAtEnd-dropped.Load())
	}
	if rampCost > 0 && est > 0 {
		fmt.Printf("平均建连速率: %.0f 条/秒\n", float64(est)/rampCost.Seconds())
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
	// 优先按类型识别 WS 关闭帧，避免对错误文本做脆弱的子串匹配
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway:
			return "closed by server"
		default:
			return fmt.Sprintf("close code %d", ce.Code)
		}
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
	case strings.Contains(msg, "EOF"):
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
