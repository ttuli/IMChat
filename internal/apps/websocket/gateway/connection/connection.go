package connection

import (
	"context"
	"errors"
	"sync"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/user"

	"github.com/gogo/protobuf/proto"
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
	RemoveRT bool
	Conn     *websocket.Conn
	Codec    protocol.Codec
	Version  int32

	sendChan  chan []byte
	closeChan chan struct{}
	closeOnce sync.Once

	mu sync.RWMutex
}

// NewConnection 创建新连接
func NewConnection(userID uint64, deviceID string, removeRT bool, conn *websocket.Conn, codec protocol.Codec, version int32) *Connection {
	return &Connection{
		UserID:    userID,
		DeviceID:  deviceID,
		RemoveRT:  removeRT,
		Conn:      conn,
		Codec:     codec,
		Version:   version,
		sendChan:  make(chan []byte, 256),
		closeChan: make(chan struct{}),
	}
}

// Send 发送消息
func (c *Connection) Send(msg *transport.WSMessage) error {
	msg.Version = c.Version
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
		return errors.New("connection closed")
	default:
		return errors.New("send buffer full")
	}
}

// SendError 发送错误消息
func (c *Connection) SendError(msg *transport.ErrorMessage) error {
	wsMsg, _ := protocol.NewWSMessage(transport.MessageType_ERROR, msg)
	return c.Send(wsMsg)
}

func (c *Connection) Kick(reason string) {
	now := time.Now().UnixMilli()
	kick := &user.UserKickoff{
		UserId:    c.UserID,
		Reason:    reason,
		Timestamp: now,
	}
	data, _ := proto.Marshal(kick)
	ws := &transport.WSMessage{
		Type:      transport.MessageType_USER_KICKOFF,
		Timestamp: now,
		Payload:   data,
	}
	c.Send(ws)
	time.AfterFunc(time.Second, c.Close)
}

// Close 关闭连接
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		c.IsClosed()
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
func (c *Connection) ReadPump(ctx context.Context, handler func(*transport.WSMessage) error) {
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

		msg := &transport.WSMessage{}
		if err := c.Codec.Decode(data, msg); err != nil {
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
