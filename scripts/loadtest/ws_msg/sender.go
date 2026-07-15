package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
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

	// readWg 跟踪所有读循环；报告前必须等它们退出，
	// 否则 report 读取 sender.ackLats 会与读循环的追加产生数据竞争。
	readWg sync.WaitGroup
)

type sender struct {
	token      string
	self       uint64 // 自身用户ID
	target     uint64
	sessionKey string
	ackLats    []time.Duration // 由该连接的读循环独占写入，结束后合并
}

// sendLoop 按速率/数量向单条连接持续发送 CHAT_TEXT。
func sendLoop(ctx context.Context, cfg *Config, c *websocket.Conn, s *sender, content string) {
	var ticker *time.Ticker
	if cfg.SendRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(cfg.SendRate))
		defer ticker.Stop()
	}

	for seq := 0; ; seq++ {
		if cfg.SendCount > 0 && seq >= cfg.SendCount {
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

func buildContent(cfg *Config) string {
	content := cfg.Content
	if cfg.PayloadSize > len(content) {
		content += strings.Repeat("x", cfg.PayloadSize-len(content))
	}
	return content
}
