// WebSocket 消息并发压测脚本
//
// 目标：测试网关（websocket-gateway）在多连接并发下的消息摄入吞吐与 ACK 时延。
// 流程：批量登录 -> 建立 N 条 WS 连接 -> 每条连接并发发送 protobuf 编码的
// CHAT_TEXT 消息 -> 读循环按 ClientId 匹配服务端回发的 MSG_ACK，计算发送->ACK
// 往返时延，同时统计端到端投递（收到对端 CHAT_TEXT）的数量。
//
// 复用项目自身的 protobuf 类型（IM2/pkg/proto/...），保证与线上完全一致的报文格式。
//
// 指标说明：
//   - 发送吞吐：客户端成功写入 socket 的消息速率。
//   - ACK：网关将消息发布到 NATS 后立即回发的确认，衡量「网关摄入 + MQ 发布」时延；
//     与下游 Message 服务是否消费无关。ACK 少于发送量通常意味着网关侧背压
//     （发送缓冲区满导致 ACK 被丢弃）。
//   - 收到消息：需要 Message 服务消费 NATS 并回投，衡量端到端投递；仅网关在跑时为 0 属正常。
//
// 关键约束：每条连接必须使用不同账号（同一 userID 新连接会被踢、旧 token 会因
// 会话 version 递增而失效）。
//
// 用法示例：
//
//	# 200 条连接，每条 50 msg/s，持续 30s，两两互发（accounts.csv: 账号,密码[,设备ID]）
//	go run ./scripts/loadtest/ws_msg \
//	    -host 127.0.0.1:8888 -login-url http://127.0.0.1:8888 \
//	    -accounts accounts.csv -n 200 -rate 50 -d 30s
//
//	# 所有连接都向固定用户 10001 发送，每条各发 1000 条后停止
//	go run ./scripts/loadtest/ws_msg -host 127.0.0.1:8888 -login-url http://127.0.0.1:8888 \
//	    -accounts accounts.csv -n 100 -target 10001 -count 1000
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var (
	flagHost     = flag.String("host", "127.0.0.1:8888", "网关地址 host:port")
	flagPath     = flag.String("path", "/ws", "WebSocket 路径")
	flagTLS      = flag.Bool("tls", false, "使用 wss（TLS）")
	flagInsecure = flag.Bool("insecure", false, "跳过 TLS 证书校验")
	flagLoginURL = flag.String("login-url", "", "登录服务基址，如 http://127.0.0.1:8888；用 -accounts 时必填")
	flagAccounts = flag.String("accounts", "", "账号文件：每行 账号,密码[,设备ID]")
	flagTokens   = flag.String("tokens", "", "token 文件：每行一个 AccessToken（用它时须配合 -target）")

	flagNum     = flag.Int("n", 0, "发送连接数；0 表示使用全部账号/token")
	flagRate    = flag.Int("rate", 20, "每条连接的发送速率（条/秒）；0 表示不限速")
	flagCount   = flag.Int("count", 0, "每条连接发送的消息总数；0 表示不限，按 -d 时长")
	flagDur     = flag.Duration("d", 0, "压测时长（-count 为 0 时生效，默认 20s）")
	flagTarget  = flag.Uint64("target", 0, "固定目标用户ID；0 表示两两互发（需 -accounts）")
	flagContent = flag.String("content", "hello from loadtest", "文本内容")
	flagPadSize = flag.Int("payload-size", 0, "将文本填充到指定字节数（0 表示不填充）")

	flagDialTO  = flag.Duration("dial-timeout", 10*time.Second, "单条连接握手超时")
	flagLoginTO = flag.Duration("login-timeout", 10*time.Second, "单次登录超时")
	flagLoginC  = flag.Int("login-c", 50, "登录/建连阶段并发数")
)

// 全局实时计数器
var (
	totalSent    atomic.Int64
	totalSendErr atomic.Int64
	totalAckOK   atomic.Int64
	totalAckFail atomic.Int64
	totalAckDup  atomic.Int64
	totalRecv    atomic.Int64
	connActive   atomic.Int64
)

type sender struct {
	token      string
	self       uint64 // 自身用户ID（tokens 模式下为 0）
	target     uint64
	sessionKey string
	ackLats    []time.Duration // 由该连接的读循环独占写入，结束后合并
}

func main() {
	flag.Parse()

	senders, err := buildSenders()
	if err != nil {
		fmt.Fprintf(os.Stderr, "准备发送者失败: %v\n", err)
		os.Exit(1)
	}
	if len(senders) == 0 {
		fmt.Fprintln(os.Stderr, "没有可用的发送连接")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，正在收尾...")
		cancel()
	}()

	content := buildContent()

	// 发送时长控制
	sendCtx := ctx
	if *flagCount <= 0 {
		d := *flagDur
		if d <= 0 {
			d = 20 * time.Second
		}
		var toCancel context.CancelFunc
		sendCtx, toCancel = context.WithTimeout(ctx, d)
		defer toCancel()
	}

	// 建立所有连接
	fmt.Printf("========== 建立 %d 条发送连接 ==========\n", len(senders))
	conns := dialAll(ctx, senders)
	if connActive.Load() == 0 {
		fmt.Fprintln(os.Stderr, "没有任何连接建立成功")
		os.Exit(1)
	}
	fmt.Printf("已建立 %d 条连接，开始发送...\n", connActive.Load())

	// 实时进度
	stopProgress := make(chan struct{})
	go progressLoop(stopProgress)

	// 每条连接一个发送 goroutine（读循环已在 dial 时启动）
	fmt.Println("========== 开始并发发送 ==========")
	start := time.Now()
	var wg sync.WaitGroup
	for i := range senders {
		if conns[i] == nil {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sendLoop(sendCtx, conns[idx], senders[idx], content)
		}(i)
	}
	wg.Wait()
	sendDur := time.Since(start)
	close(stopProgress)

	// 给在途 ACK 一点收尾时间
	drain := 2 * time.Second
	if ctx.Err() == nil {
		fmt.Printf("\n发送结束，等待 %s 收尾在途 ACK...\n", drain)
		time.Sleep(drain)
	}

	// 关闭连接
	for _, c := range conns {
		if c != nil {
			_ = c.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
			c.Close()
		}
	}

	report(senders, sendDur)
}

// sendLoop 按速率/数量向单条连接持续发送 CHAT_TEXT。
func sendLoop(ctx context.Context, c *websocket.Conn, s *sender, content string) {
	var ticker *time.Ticker
	if *flagRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*flagRate))
		defer ticker.Stop()
	}

	for seq := 0; ; seq++ {
		if *flagCount > 0 && seq >= *flagCount {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if ticker != nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		now := time.Now()
		// ClientId 内嵌发送时刻纳秒，ACK 回发时原样带回，无需维护 in-flight 映射表。
		clientID := fmt.Sprintf("%d-%d-%d", s.self, seq, now.UnixNano())

		data, err := buildChatFrame(clientID, s, content, now)
		if err != nil {
			totalSendErr.Add(1)
			return
		}
		if err := c.WriteMessage(websocket.BinaryMessage, data); err != nil {
			totalSendErr.Add(1)
			return // 写失败通常意味着连接已断
		}
		totalSent.Add(1)
	}
}

// buildChatFrame 构造与线上一致的 WSMessage(CHAT_TEXT) 二进制帧。
func buildChatFrame(clientID string, s *sender, content string, now time.Time) ([]byte, error) {
	text := &message.TextMessage{
		Base: &message.BaseMessage{
			ClientId:   clientID,
			SessionKey: s.sessionKey,
			Target:     s.target,
			SendTime:   now.UnixMilli(),
			Status:     message.MessageStatus_MESSAGE_STATUS_SENDING,
		},
		Content: content,
	}
	payload, err := proto.Marshal(text)
	if err != nil {
		return nil, err
	}
	ws := &transport.WSMessage{
		Type:            transport.MessageType_CHAT_TEXT,
		Timestamp:       now.UnixMilli(),
		Payload:         payload,
		RouteTarget:     []uint64{s.target},
		RouteTargetType: transport.TargetType_USER,
	}
	return proto.Marshal(ws)
}

// readLoop 消费服务端下行：匹配 MSG_ACK 计算时延，统计收到的聊天消息。
func readLoop(c *websocket.Conn, s *sender) {
	defer func() {
		connActive.Add(-1)
		c.Close()
	}()
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var ws transport.WSMessage
		if proto.Unmarshal(data, &ws) != nil {
			continue
		}
		switch ws.Type {
		case transport.MessageType_MSG_ACK:
			var ack message.MessageAck
			if proto.Unmarshal(ws.Payload, &ack) != nil {
				continue
			}
			switch ack.Status {
			case message.AckStatus_ACK_STATUS_SUCCESS:
				totalAckOK.Add(1)
			case message.AckStatus_ACK_STATUS_DUPLICATE:
				totalAckDup.Add(1)
			default:
				totalAckFail.Add(1)
			}
			if lat, ok := latencyFromClientID(ack.ClientId); ok {
				s.ackLats = append(s.ackLats, lat) // 单读循环独占，无需加锁
			}
		case transport.MessageType_CHAT_TEXT, transport.MessageType_GROUP_TEXT:
			totalRecv.Add(1)
		}
	}
}

// latencyFromClientID 从 "self-seq-nanos" 解析发送时刻并算出往返时延。
func latencyFromClientID(clientID string) (time.Duration, bool) {
	i := strings.LastIndexByte(clientID, '-')
	if i < 0 {
		return 0, false
	}
	ns, err := strconv.ParseInt(clientID[i+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	d := time.Since(time.Unix(0, ns))
	if d < 0 {
		return 0, false
	}
	return d, true
}

// dialAll 并发建立所有连接并各自启动读循环，返回与 senders 对齐的连接切片。
func dialAll(ctx context.Context, senders []*sender) []*websocket.Conn {
	conns := make([]*websocket.Conn, len(senders))
	sem := make(chan struct{}, *flagLoginC)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := range senders {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := dialOne(ctx, senders[idx].token)
			if err != nil {
				failed.Add(1)
				return
			}
			conns[idx] = c
			connActive.Add(1)
			go readLoop(c, senders[idx])
		}(i)
	}
	wg.Wait()
	if f := failed.Load(); f > 0 {
		fmt.Printf("建连失败 %d 条\n", f)
	}
	return conns
}

func dialOne(ctx context.Context, token string) (*websocket.Conn, error) {
	scheme := "ws"
	if *flagTLS {
		scheme = "wss"
	}
	u := url.URL{Scheme: scheme, Host: *flagHost, Path: *flagPath, RawQuery: "token=" + url.QueryEscape(token)}
	dialer := websocket.Dialer{
		HandshakeTimeout: *flagDialTO,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: *flagInsecure},
	}
	c, _, err := dialer.DialContext(ctx, u.String(), nil)
	return c, err
}

// ---------- 构造发送者列表（登录 + 目标配对）----------

func buildSenders() ([]*sender, error) {
	creds, err := loadCredentials()
	if err != nil {
		return nil, err
	}
	n := *flagNum
	if n <= 0 || n > len(creds) {
		n = len(creds)
	}
	creds = creds[:n]

	// 换取 token
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	tokens, selves := resolveTokens(ctx, creds)

	// 收集有效发送者
	valid := make([]*sender, 0, len(tokens))
	for i := range tokens {
		if tokens[i] == "" {
			continue
		}
		valid = append(valid, &sender{token: tokens[i], self: selves[i]})
	}
	if len(valid) == 0 {
		return nil, fmt.Errorf("没有有效 token")
	}

	// 目标配对
	if *flagTarget > 0 {
		for _, s := range valid {
			s.target = *flagTarget
			s.sessionKey = privateSessionKey(s.self, s.target)
		}
	} else {
		// 两两互发：sender[i] -> sender[i+1]
		if valid[0].self == 0 {
			return nil, fmt.Errorf("两两互发需要账号ID，请改用 -accounts，或用 -target 指定固定目标")
		}
		m := len(valid)
		for i, s := range valid {
			s.target = valid[(i+1)%m].self
			s.sessionKey = privateSessionKey(s.self, s.target)
		}
	}
	return valid, nil
}

// privateSessionKey 与服务端私聊 session_key 约定一致：min_max。
func privateSessionKey(a, b uint64) string {
	if a == 0 {
		return fmt.Sprintf("bench_%d", b)
	}
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("%d_%d", a, b)
}

func buildContent() string {
	content := *flagContent
	if *flagPadSize > len(content) {
		content += strings.Repeat("x", *flagPadSize-len(content))
	}
	return content
}

// ---------- token 获取 ----------

func resolveTokens(ctx context.Context, creds []credential) (tokens []string, selves []uint64) {
	tokens = make([]string, len(creds))
	selves = make([]uint64, len(creds))

	if creds[0].token != "" { // 直接提供 token
		for i, c := range creds {
			tokens[i] = c.token
		}
		return tokens, selves
	}

	if *flagLoginURL == "" {
		fmt.Fprintln(os.Stderr, "使用 -accounts 时必须提供 -login-url")
		os.Exit(2)
	}
	fmt.Printf("========== 批量登录 %d 个账号 ==========\n", len(creds))
	client := &http.Client{Timeout: *flagLoginTO}
	var ok, fail atomic.Int64
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
				fail.Add(1)
				return
			}
			tokens[idx] = tk
			if id, e := strconv.ParseUint(creds[idx].account, 10, 64); e == nil {
				selves[idx] = id
			}
			ok.Add(1)
		}(i)
	}
	wg.Wait()
	fmt.Printf("登录完成：成功 %d，失败 %d\n", ok.Load(), fail.Load())
	return tokens, selves
}

type credential struct {
	account, password, deviceID, token string
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
		return "", fmt.Errorf("解析登录响应失败(status=%d)", resp.StatusCode)
	}
	if lr.Data.Token == "" {
		return "", fmt.Errorf("登录未返回 token(code=%d msg=%s)", lr.Code, lr.Message)
	}
	return lr.Data.Token, nil
}

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
		c := credential{account: strings.TrimSpace(parts[0]), password: strings.TrimSpace(parts[1])}
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

// ---------- 输出 ----------

func progressLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastSent, lastAck int64
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			sent := totalSent.Load()
			ack := totalAckOK.Load()
			inflight := sent - (totalAckOK.Load() + totalAckFail.Load() + totalAckDup.Load())
			fmt.Printf("\r发送=%d(+%d/s)  ACK=%d(+%d/s)  在途=%d  收到=%d  发送错误=%d      ",
				sent, sent-lastSent, ack, ack-lastAck, inflight, totalRecv.Load(), totalSendErr.Load())
			lastSent, lastAck = sent, ack
		}
	}
}

func report(senders []*sender, dur time.Duration) {
	var lats []time.Duration
	for _, s := range senders {
		lats = append(lats, s.ackLats...)
	}
	sent := totalSent.Load()
	ackOK := totalAckOK.Load()

	fmt.Printf("\n\n========== WS 消息并发压测结果 ==========\n")
	fmt.Printf("发送连接   : %d\n", len(senders))
	fmt.Printf("发送时长   : %.2fs\n", dur.Seconds())
	fmt.Printf("发送总数   : %d\n", sent)
	fmt.Printf("发送错误   : %d\n", totalSendErr.Load())
	if dur.Seconds() > 0 {
		fmt.Printf("发送吞吐   : %.0f msg/s\n", float64(sent)/dur.Seconds())
	}
	fmt.Printf("ACK 成功   : %d (%.2f%%)\n", ackOK, pctOf(ackOK, sent))
	fmt.Printf("ACK 失败   : %d\n", totalAckFail.Load())
	fmt.Printf("ACK 重复   : %d\n", totalAckDup.Load())
	fmt.Printf("收到消息   : %d（端到端投递，仅网关在跑时为 0 属正常）\n", totalRecv.Load())
	if miss := sent - (ackOK + totalAckFail.Load() + totalAckDup.Load()); miss > 0 {
		fmt.Printf("未收到 ACK : %d（可能为网关背压丢弃或收尾未回）\n", miss)
	}

	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		var sum time.Duration
		for _, d := range lats {
			sum += d
		}
		fmt.Println("\n--- ACK 往返时延 ---")
		fmt.Printf("  样本 : %d\n", len(lats))
		fmt.Printf("  最小 : %s\n", lats[0].Round(time.Microsecond))
		fmt.Printf("  平均 : %s\n", (sum / time.Duration(len(lats))).Round(time.Microsecond))
		fmt.Printf("  P50  : %s\n", pctile(lats, 50).Round(time.Microsecond))
		fmt.Printf("  P90  : %s\n", pctile(lats, 90).Round(time.Microsecond))
		fmt.Printf("  P95  : %s\n", pctile(lats, 95).Round(time.Microsecond))
		fmt.Printf("  P99  : %s\n", pctile(lats, 99).Round(time.Microsecond))
		fmt.Printf("  最大 : %s\n", lats[len(lats)-1].Round(time.Microsecond))
	}
	fmt.Println("=========================================")
}

func pctOf(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
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
