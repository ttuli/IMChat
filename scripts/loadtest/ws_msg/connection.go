package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	zredis "github.com/zeromicro/go-zero/core/stores/redis"

	tokenmanager "IM2/pkg/tokenManager"
)

// generateAndRegisterTokens generates virtual accounts and tokens, registers sessions in Redis.
func generateAndRegisterTokens(ctx context.Context, cfg *Config) ([]string, *tokenmanager.TokenManager) {
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
	sem := make(chan struct{}, 100) // Concurrency limit
	var successCount atomic.Int64

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
			deviceID := fmt.Sprintf("device_msg%02d", idx+1)

			accessToken, _, err := tm.OnLogin(ctx, userID, tokenmanager.LoginOptions{
				MachineID:  deviceID,
				RememberMe: false,
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

	// Filter out empty tokens
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

// cleanupTokens cleans up the session and routing keys from Redis on exit.
func cleanupTokens(tm *tokenmanager.TokenManager, cfg *Config) {
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

// dialAll dials all WebSockets using connection rate limit.
func dialAll(ctx context.Context, cfg *Config, senders []*sender) []*websocket.Conn {
	conns := make([]*websocket.Conn, len(senders))
	var wg sync.WaitGroup
	var failedCount atomic.Int64

	interval := time.Second / time.Duration(cfg.DialRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

dialLoop:
	for i := range senders {
		select {
		case <-ctx.Done():
			break dialLoop
		case <-ticker.C:
		}

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s := senders[idx]
			c, err := dialOne(ctx, cfg, s.token)
			if err != nil {
				failedCount.Add(1)
				return
			}
			conns[idx] = c
			connActive.Add(1)
			readWg.Add(1)
			go func() {
				defer readWg.Done()
				readLoop(c, s)
			}()
		}(i)
	}
	wg.Wait()

	if f := failedCount.Load(); f > 0 {
		fmt.Printf("建连失败 %d 条\n", f)
	}
	return conns
}

func dialOne(ctx context.Context, cfg *Config, token string) (*websocket.Conn, error) {
	scheme := "ws"
	if cfg.TLS {
		scheme = "wss"
	}
	u := url.URL{Scheme: scheme, Host: cfg.Host, Path: cfg.Path, RawQuery: "token=" + url.QueryEscape(token)}
	dialer := websocket.Dialer{
		HandshakeTimeout: cfg.dialTimeoutDur,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: cfg.Insecure},
	}
	c, _, err := dialer.DialContext(ctx, u.String(), nil)
	return c, err
}
