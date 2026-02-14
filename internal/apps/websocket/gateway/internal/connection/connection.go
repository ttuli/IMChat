package connection

import (
	"context"
	"sync"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/xerr"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// 写入超时
	writeWait = 10 * time.Second
	// 读取超时
	pongWait = 60 * time.Second
	// 心跳间隔
	pingPeriod = (pongWait * 9) / 10
	// 最大消息大小
	maxMessageSize = 65536
)

// Connection WebSocket 连接
type Connection struct {
	UserID   uint64
	DeviceID string
	Platform string
	Conn     *websocket.Conn
	Codec    protocol.Codec

	sendChan  chan []byte
	closeChan chan struct{}
	closeOnce sync.Once

	mu sync.RWMutex
}

// NewConnection 创建新连接
func NewConnection(userID uint64, deviceID, platform string, conn *websocket.Conn, codec protocol.Codec) *Connection {
	return &Connection{
		UserID:    userID,
		DeviceID:  deviceID,
		Platform:  platform,
		Conn:      conn,
		Codec:     codec,
		sendChan:  make(chan []byte, 256),
		closeChan: make(chan struct{}),
	}
}

// Send 发送消息
func (c *Connection) Send(msg *protocol.Message) error {
	data, err := c.Codec.Encode(msg)
	if err != nil {
		return err
	}
	return c.SendRaw(data)
}

// SendRaw 发送原始数据
func (c *Connection) SendRaw(data []byte) error {
	select {
	case c.sendChan <- data:
		return nil
	case <-c.closeChan:
		return xerr.New(xerr.ErrInternalServer, "connection closed")
	default:
		return xerr.New(xerr.ErrInternalServer, "send buffer full")
	}
}

// Close 关闭连接
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.Conn.Close()
	})
}

// IsClosed 检查连接是否已关闭
func (c *Connection) IsClosed() bool {
	select {
	case <-c.closeChan:
		return true
	default:
		return false
	}
}

// ReadPump 读取消息循环
func (c *Connection) ReadPump(ctx context.Context, handler func(*protocol.Message) error) {
	defer c.Close()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeChan:
			return
		default:
		}

		_, data, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logx.Errorf("[Connection] user %d read error: %v", c.UserID, err)
			}
			return
		}

		msg, err := c.Codec.Decode(data)
		if err != nil {
			logx.Errorf("[Connection] user %d decode error: %v", c.UserID, err)
			continue
		}

		if err := handler(msg); err != nil {
			logx.Errorf("[Connection] user %d handle message error: %v", c.UserID, err)
		}
	}
}

// WritePump 写入消息循环
func (c *Connection) WritePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeChan:
			return
		case data, ok := <-c.sendChan:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, data); err != nil {
				logx.Errorf("[Connection] user %d write error: %v", c.UserID, err)
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
