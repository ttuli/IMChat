package listener

import (
	"context"
	"fmt"
	"time"

	"IM2/internal/apps/Message/rpc/config"
	"IM2/internal/apps/Message/rpc/internal/dao"
	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// NatsListener 监听 NATS 消息，将消息写入 MongoDB
type NatsListener struct {
	conn            *nats.Conn
	js              nats.JetStreamContext
	c               config.Config
	messageDAO      *dao.MessageDAO
	conversationDAO *dao.ConversationDAO
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewNatsListener(c config.Config) *NatsListener {
	conn, err := nats.Connect(c.Listener.Url)
	if err != nil {
		panic(err)
	}
	js, err := conn.JetStream()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &NatsListener{
		conn:            conn,
		js:              js,
		c:               c,
		messageDAO:      dao.NewMessageDAO(c.DAO.MessageDAO.Dbsource),
		conversationDAO: dao.NewConversationDAO(c.DAO.ConversationDAO.Dbsource, c.DAO.ConversationDAO.Redisx),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (l *NatsListener) Listen() error {
	_, err := l.js.Subscribe(l.c.Listener.DBSubject, func(msg *nats.Msg) {
		err := l.handleMessage(msg)
		if err != nil {
			msg.Nak()
		} else {
			msg.Ack()
		}
	})
	if err != nil {
		return err
	}

	_, err = l.js.Subscribe(l.c.Listener.BroadcastSubject, func(msg *nats.Msg) {
		err := l.handleBroadcastMsg(msg)
		if err != nil {
			msg.Nak()
		} else {
			msg.Ack()
		}
	})

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

func (l *NatsListener) handleBroadcastMsg(msg *nats.Msg) error {
	var wsMsg common.WSMessage
	if err := proto.Unmarshal(msg.Data, &wsMsg); err != nil {
		logger.Errorf("Failed to unmarshal NATS message: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(l.ctx, 3*time.Second)
	defer cancel()

	switch wsMsg.Type {
	case common.MessageType_GROUP_REQUEST:
		var groupRequest common.GroupApply
		if err := proto.Unmarshal(msg.Data, &groupRequest); err != nil {
			return err
		}
		if groupRequest.Status == common.GroupApplyStatus_GROUP_APPLY_STATUS_ACCEPTED {
			session := common.GenerateGroupSessionId(groupRequest.GroupId)
			if err := l.conversationDAO.InsertUserConversation(ctx, groupRequest.SenderId, session); err != nil {
				return err
			}
		}
	case common.MessageType_GROUP_JOIN:
		var groupJoin common.GroupNotification
		if err := proto.Unmarshal(msg.Data, &groupJoin); err != nil {
			return err
		}
		session := common.GenerateGroupSessionId(groupJoin.GroupId)
		if len(groupJoin.TargetIds) == 0 {
			logger.Error("handleBroadcastMsg: groupJoin.TargetIds is empty")
			return nil
		}

		if err := l.conversationDAO.InsertUserConversation(ctx, groupJoin.TargetIds[0], session); err != nil {
			return err
		}

	case common.MessageType_FRIEND_REQUEST:
		var friendApply common.FriendRequest
		if err := proto.Unmarshal(msg.Data, &friendApply); err != nil {
			return err
		}
		if friendApply.Status == common.ApplyStatus_APPLY_STATUS_AGREED {
			session := common.GenerateUserSessionId(friendApply.FromUserId, friendApply.ToUserId)
			userConvs := []*model.UserConversation{
				{
					UserID:         friendApply.FromUserId,
					ConversationID: session,
					IsTop:          false,
					IsDisturb:      false,
					IsMute:         false,
					LastReadSeq:    0,
				},
				{
					UserID:         friendApply.ToUserId,
					ConversationID: session,
					IsTop:          false,
					IsDisturb:      false,
					IsMute:         false,
					LastReadSeq:    0,
				},
			}

			if err := l.conversationDAO.BatchInsertUserConversations(ctx, userConvs); err != nil {
				return err
			}
		}
	case common.MessageType_FRIEND_ADD:
		var friendMsg common.Friend
		if err := proto.Unmarshal(msg.Data, &friendMsg); err != nil {
			return err
		}
		session := common.GenerateUserSessionId(friendMsg.FriendId, friendMsg.UserId)
		userConvs := []*model.UserConversation{
			{
				UserID:         friendMsg.FriendId,
				ConversationID: session,
				IsTop:          false,
				IsDisturb:      false,
				IsMute:         false,
				LastReadSeq:    0,
			},
			{
				UserID:         friendMsg.UserId,
				ConversationID: session,
				IsTop:          false,
				IsDisturb:      false,
				IsMute:         false,
				LastReadSeq:    0,
			},
		}

		if err := l.conversationDAO.BatchInsertUserConversations(ctx, userConvs); err != nil {
			return err
		}
	}
	return nil
}

func (l *NatsListener) handleMessage(msg *nats.Msg) error {
	// TODO: 反序列化 msg.Data -> model.Message，调用 l.messageDAO.InsertMessage
	var wsMsg common.WSMessage
	if err := proto.Unmarshal(msg.Data, &wsMsg); err != nil {
		logger.Errorf("Failed to unmarshal NATS message: %v", err)
		return err
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
		if err := proto.Unmarshal(wsMsg.Payload, &textMsg); err != nil {
			return err
		}
		baseMsg = textMsg.Base
		content = textMsg.Content
	case common.MessageType_CHAT_IMAGE, common.MessageType_GROUP_IMAGE:
		var imgMsg common.ImageMessage
		if err := proto.Unmarshal(wsMsg.Payload, &imgMsg); err != nil {
			return err
		}
		baseMsg = imgMsg.Base
		mediaURL = imgMsg.Url
	case common.MessageType_CHAT_VIDEO, common.MessageType_GROUP_VIDEO:
		var videoMsg common.VideoMessage
		if err := proto.Unmarshal(wsMsg.Payload, &videoMsg); err != nil {
			return err
		}
		baseMsg = videoMsg.Base
		mediaURL = videoMsg.Url
	case common.MessageType_CHAT_AUDIO, common.MessageType_GROUP_AUDIO:
		var audioMsg common.AudioMessage
		if err := proto.Unmarshal(wsMsg.Payload, &audioMsg); err != nil {
			return err
		}
		baseMsg = audioMsg.Base
		mediaURL = audioMsg.Url
	case common.MessageType_CHAT_FILE, common.MessageType_GROUP_FILE:
		var fileMsg common.FileMessage
		if err := proto.Unmarshal(wsMsg.Payload, &fileMsg); err != nil {
			return err
		}
		baseMsg = fileMsg.Base
		mediaURL = fileMsg.Url
	}

	if baseMsg == nil {
		return fmt.Errorf("Failed to extract BaseMessage or unsupported message type: %v", wsMsg.Type)
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
		Status:         int8(common.MessageStatus_MESSAGE_STATUS_SENT),
		CreateTime:     time.UnixMilli(wsMsg.Timestamp),
	}

	ctx, cancel := context.WithTimeout(l.ctx, 3*time.Second)
	defer cancel()

	if err := l.messageDAO.InsertMessage(ctx, dbMsg); err != nil {
		logger.Errorf("Failed to insert message to MongoDB: %v", err)
		return err
	}
	return nil
}
