package listener

import (
	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// NatsListener 监听 NATS 消息，将消息写入 MongoDB
type NatsListener struct {
	conn       *nats.Conn
	c          config.ListenerConfig
	messageDAO *dao.MessageDAO
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewNatsListener(c config.ListenerConfig) *NatsListener {
	conn, err := nats.Connect(c.Url)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &NatsListener{
		conn:       conn,
		c:          c,
		messageDAO: dao.NewMessageDAO(c.DBAddress),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (l *NatsListener) Listen() error {
	_, err := l.conn.Subscribe(l.c.DBSubject, l.handleMessage)
	return err
}

// Stop 停止监听并释放资源
func (l *NatsListener) Stop() error {
	l.cancel()
	if l.conn != nil {
		l.conn.Close()
	}
	return nil
}

func (l *NatsListener) handleMessage(msg *nats.Msg) {
	// TODO: 反序列化 msg.Data -> model.Message，调用 l.messageDAO.InsertMessage
	var wsMsg common.WSMessage
	if err := proto.Unmarshal(msg.Data, &wsMsg); err != nil {
		logger.Errorf("Failed to unmarshal NATS message: %v", err)
		return
	}

	// 解析出具体的业务消息
	var baseMsg *common.BaseMessage
	var content string
	var mediaURL string
	var extra map[string]any

	// 根据消息类型解析 Payload
	switch wsMsg.Type {
	case common.MessageType_CHAT_TEXT, common.MessageType_GROUP_TEXT:
		var textMsg common.TextMessage
		if err := proto.Unmarshal(wsMsg.Payload, &textMsg); err == nil {
			baseMsg = textMsg.Base
			content = textMsg.Content
		}
	case common.MessageType_CHAT_IMAGE, common.MessageType_GROUP_IMAGE:
		var imgMsg common.ImageMessage
		if err := proto.Unmarshal(wsMsg.Payload, &imgMsg); err == nil {
			baseMsg = imgMsg.Base
			mediaURL = imgMsg.Url
		}
	case common.MessageType_CHAT_VIDEO, common.MessageType_GROUP_VIDEO:
		var videoMsg common.VideoMessage
		if err := proto.Unmarshal(wsMsg.Payload, &videoMsg); err == nil {
			baseMsg = videoMsg.Base
			mediaURL = videoMsg.Url
		}
	case common.MessageType_CHAT_AUDIO, common.MessageType_GROUP_AUDIO:
		var audioMsg common.AudioMessage
		if err := proto.Unmarshal(wsMsg.Payload, &audioMsg); err == nil {
			baseMsg = audioMsg.Base
			mediaURL = audioMsg.Url
		}
	case common.MessageType_CHAT_FILE, common.MessageType_GROUP_FILE:
		var fileMsg common.FileMessage
		if err := proto.Unmarshal(wsMsg.Payload, &fileMsg); err == nil {
			baseMsg = fileMsg.Base
			mediaURL = fileMsg.Url
		}
	}

	if baseMsg == nil {
		logger.Errorf("Failed to extract BaseMessage or unsupported message type: %v", wsMsg.Type)
		return
	}

	// 转换为 MongoDB 模型
	dbMsg := &model.Message{
		MsgID:          baseMsg.MsgId,
		ClientID:       baseMsg.ClientId,
		ConversationID: baseMsg.SessionId,
		FromUserID:     baseMsg.FromUserId,
		MsgType:        int16(wsMsg.Type),
		Seq:            uint64(baseMsg.MsgSeq),
		Content:        content,
		MediaURL:       mediaURL,
		Extra:          extra,
		Status:         model.MsgStatusNormal,
		CreateTime:     time.UnixMilli(wsMsg.Timestamp),
	}

	ctx, cancel := context.WithTimeout(l.ctx, 3*time.Second)
	defer cancel()

	if err := l.messageDAO.InsertMessage(ctx, dbMsg); err != nil {
		logger.Errorf("Failed to insert message to MongoDB: %v", err)
	}
}
