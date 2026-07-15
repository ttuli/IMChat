package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type noOpLogger struct{}

func (n *noOpLogger) Printf(ctx context.Context, format string, v ...interface{}) {}

func main() {
	// 禁用 go-zero 内置的日志与系统统计输出，保持压测控制台界面整洁
	logx.Disable()
	logx.DisableStat()
	// 屏蔽底层 redis 驱动标准日志包 of 警告输出
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
		fmt.Println("\n收到中断信号，正在收尾...")
		cancel()
	}()

	// 2. 生成 Token 并写入 Redis
	tokens, tm := generateAndRegisterTokens(ctx, &cfg)
	if len(tokens) == 0 {
		fmt.Fprintln(os.Stderr, "没有生成任何有效 token，无法建连")
		os.Exit(1)
	}

	// 3. 构建发送者列表
	senders, err := buildSenders(&cfg, tokens)
	if err != nil {
		fmt.Fprintf(os.Stderr, "准备发送者失败: %v\n", err)
		cleanupTokens(tm, &cfg)
		os.Exit(1)
	}

	content := buildContent(&cfg)

	// 发送时长控制
	sendCtx := ctx
	if cfg.SendCount <= 0 {
		var toCancel context.CancelFunc
		sendCtx, toCancel = context.WithTimeout(ctx, cfg.durationDur)
		defer toCancel()
	}

	// 4. 建立所有连接
	fmt.Printf("========== 建立 %d 条发送连接 ==========\n", len(senders))
	conns := dialAll(ctx, &cfg, senders)
	if connActive.Load() == 0 {
		fmt.Fprintln(os.Stderr, "没有任何连接建立成功")
		cleanupTokens(tm, &cfg)
		os.Exit(1)
	}
	fmt.Printf("已建立 %d 条连接，开始发送...\n", connActive.Load())

	// 实时进度
	stopProgress := make(chan struct{})
	go progressLoop(stopProgress)

	// 5. 每条连接一个发送 goroutine
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
			sendLoop(sendCtx, &cfg, conns[idx], senders[idx], content)
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

	// 等读循环全部退出后统计数据才稳定
	waitReadLoops(5 * time.Second)

	// 清理 Redis 注册的 Token
	cleanupTokens(tm, &cfg)

	// 6. 报告结果
	report(senders, sendDur)
}

func buildSenders(cfg *Config, tokens []string) ([]*sender, error) {
	valid := make([]*sender, 0, len(tokens))
	for idx, tk := range tokens {
		if tk == "" {
			continue
		}
		userID := cfg.StartUserID + uint64(idx)
		valid = append(valid, &sender{token: tk, self: userID})
	}
	if len(valid) == 0 {
		return nil, fmt.Errorf("没有有效 token")
	}

	// 目标配对
	if cfg.TargetUserID > 0 {
		for _, s := range valid {
			s.target = cfg.TargetUserID
			s.sessionKey = privateSessionKey(s.self, s.target)
		}
	} else {
		// 两两互发：sender[i] -> sender[i+1]
		m := len(valid)
		if m < 2 {
			return nil, fmt.Errorf("两两互发需要至少 2 个账号，当前有效账号数: %d", m)
		}
		for i, s := range valid {
			s.target = valid[(i+1)%m].self
			s.sessionKey = privateSessionKey(s.self, s.target)
		}
	}
	return valid, nil
}

// waitReadLoops 等待所有读循环退出。
func waitReadLoops(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		readWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintf(os.Stderr, "警告：等待读循环退出超时(%s)，部分统计可能不完整\n", timeout)
	}
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
