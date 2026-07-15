package util

import (
	"errors"
	"strconv"
	"strings"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/proto/group"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/social"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"

	"google.golang.org/protobuf/proto"
)

func GetSessionType(sessionId string) message.SessionType {
	if IsGroupSession(sessionId) {
		return message.SessionType_SESSION_TYPE_GROUP
	}
	return message.SessionType_SESSION_TYPE_PRIVATE
}

func GenerateGroupSessionId(groupId uint64) string {
	return strconv.FormatUint(groupId, 10)
}

func IsGroupSession(sessionId string) bool {
	return !IsPrivateSession(sessionId)
}

func IsPrivateSession(sessionId string) bool {
	return strings.Contains(sessionId, "_")
}

// GetTargetIdFromSessionId 从会话ID中解析出目标ID（群ID或对方用户ID）
func GetTargetIdFromSessionId(sessionId string, currentUserId uint64) (uint64, error) {
	if IsGroupSession(sessionId) {
		return strconv.ParseUint(sessionId, 10, 64)
	} else if IsPrivateSession(sessionId) {
		parts := strings.Split(sessionId, "_")
		if len(parts) != 2 {
			return 0, errors.New("invalid private session id")
		}
		id1, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		id2, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, err
		}
		if id1 == currentUserId {
			return id2, nil
		}
		return id1, nil
	}
	return 0, errors.New("invalid session id")
}

func IsChatMessage(t transport.MessageType) bool {
	return t >= transport.MessageType_CHAT_TEXT && t <= transport.MessageType_MSG_OP_RECALL
}

func IsNotifyMessage(t transport.MessageType) bool {
	return t >= transport.MessageType_NOTIFICATION && t <= transport.MessageType_GROUP_REQUEST
}

// ConvertFriendApplyToWSMessage converts a model.FriendApply to a WSMessage
func ConvertFriendApplyToWSMessage(apply *model.FriendApply, targetID uint64) (*transport.WSMessage, error) {
	pbApply := &social.FriendRequest{
		Id:           apply.ID,
		FromUserId:   apply.FromUserID,
		ToUserId:     apply.ToUserID,
		ApplyMsg:     apply.ApplyMsg,
		Status:       social.ApplyStatus(int32(apply.Status)),
		Source:       social.ApplySource(int32(apply.Source)),
		RequestTime:  apply.CreateTime.UnixMilli(),
		HandleTime:   apply.HandleTime.UnixMilli(),
		RejectReason: apply.RejectReason,
	}

	payload, err := proto.Marshal(pbApply)
	if err != nil {
		return nil, err
	}

	return &transport.WSMessage{
		RouteTarget:     []uint64{targetID},
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       apply.HandleTime.UnixMilli(),
		Type:            transport.MessageType_FRIEND_REQUEST,
		Payload:         payload,
	}, nil
}

// ConvertGroupApplyToWSMessage converts a model.GroupApply to a WSMessage
func ConvertGroupApplyToWSMessage(apply *model.GroupApply, targetIDs []uint64) (*transport.WSMessage, error) {
	pbApply := &social.GroupApply{
		Id:          apply.ID,
		SenderId:    apply.FromUserID,
		GroupId:     apply.GroupID,
		ApplyMsg:    apply.ApplyMsg,
		Status:      social.GroupApplyStatus(apply.Status),
		HandlerId:   apply.HandlerID,
		RequestTime: apply.CreateTime.UnixMilli(),
		HandleTime:  apply.UpdateTime.UnixMilli(),
	}

	payload, err := proto.Marshal(pbApply)
	if err != nil {
		return nil, err
	}

	return &transport.WSMessage{
		RouteTarget:     targetIDs,
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       apply.UpdateTime.UnixMilli(),
		Type:            transport.MessageType_GROUP_REQUEST,
		Payload:         payload,
	}, nil
}

// NewRecallNotifyMsg 构造消息撤回通知的落库消息（统一 NotifyMessage 载体）。
// 发布到 DBSubject 后由 Message 服务分配 msg_id/seq 持久化再扇出，
// 离线客户端可按会话 seq 增量拉取感知撤回。
// sessionKey 用于会话形态判定与目标解析（群聊=群ID，单聊=对方用户ID）。
func NewRecallNotifyMsg(operator uint64, sessionKey string, msg *model.Message) (*svc.MessageSend, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	target, err := GetTargetIdFromSessionId(sessionKey, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()

	payload, err := proto.Marshal(&message.NotifyMessage{
		Base: &message.BaseMessage{
			SessionId:  msg.SessionID,
			SessionKey: sessionKey,
			FromUserId: operator,
			Target:     target,
			SendTime:   now,
		},
		Body: &message.NotifyMessage_Recall{Recall: &message.MessageRecall{
			MsgId:      msg.MsgID,
			RecallTime: now,
		}},
	})
	if err != nil {
		return nil, err
	}

	return &svc.MessageSend{
		SessionId:  msg.SessionID,
		SessionKey: sessionKey,
		Sender:     operator,
		Target:     target,
		MsgType:    int64(transport.MessageType_NOTIFICATION),
		Timestamp:  now,
		Preview:    "撤回了一条消息",
		Payload:    payload,
	}, nil
}

// NewGroupOperationMsg 构造群操作通知的落库消息（统一 NotifyMessage 载体）。
// 通知与聊天消息同链路：发布到 DBSubject 后由 Message 服务分配 msg_id/seq
// 落库，再按成员扇出投递；msg_id/session_id/msg_seq 由落库链路回填。
func NewGroupOperationMsg(opType message.GroupOperationType, groupId uint64, targetIDs []uint64, operator uint64, groupInfo *model.Group) *svc.MessageSend {
	now := time.Now().UnixMilli()
	sessionKey := GenerateGroupSessionId(groupId)
	notify := &message.GroupNotification{
		OpType:     opType,
		GroupId:    groupId,
		OperatorId: operator,
		TargetIds:  targetIDs,
		OpTime:     now,
	}

	if groupInfo != nil {
		notify.GroupInfo = &group.GroupInfo{
			Id:          groupInfo.ID,
			OwnerId:     groupInfo.OwnerID,
			Name:        groupInfo.Name,
			Avatar:      groupInfo.Avatar,
			Notice:      groupInfo.Notice,
			MemberCount: int32(groupInfo.MemberCount),
			CreateTime:  groupInfo.CreateTime.UnixMilli(),
			UpdateTime:  groupInfo.UpdateTime.UnixMilli(),
		}
	}

	payload, err := proto.Marshal(&message.NotifyMessage{
		Base: &message.BaseMessage{
			SessionKey: sessionKey,
			FromUserId: operator,
			Target:     groupId,
			SendTime:   now,
		},
		Body: &message.NotifyMessage_GroupNotify{GroupNotify: notify},
	})
	if err != nil {
		return nil
	}

	return &svc.MessageSend{
		SessionKey: sessionKey,
		Sender:     operator,
		Target:     groupId,
		MsgType:    int64(transport.MessageType_NOTIFICATION),
		Timestamp:  now,
		Preview:    GroupNotifyPreview(opType),
		Payload:    payload,
	}
}

// GroupNotifyPreview 群操作通知的会话列表摘要文案
func GroupNotifyPreview(opType message.GroupOperationType) string {
	switch opType {
	case message.GroupOperationType_GROUP_OP_CREATE:
		return "创建了群聊"
	case message.GroupOperationType_GROUP_OP_DISMISS:
		return "群聊已解散"
	case message.GroupOperationType_GROUP_OP_JOIN:
		return "加入了群聊"
	case message.GroupOperationType_GROUP_OP_LEAVE:
		return "退出了群聊"
	case message.GroupOperationType_GROUP_OP_KICK:
		return "被移出群聊"
	case message.GroupOperationType_GROUP_OP_INVITE:
		return "被邀请进群"
	case message.GroupOperationType_GROUP_OP_MUTE:
		return "被禁言"
	case message.GroupOperationType_GROUP_OP_UNMUTE:
		return "被解除禁言"
	case message.GroupOperationType_GROUP_OP_UPDATE_INFO,
		message.GroupOperationType_GROUP_OP_INFO_UPDATE_NAME,
		message.GroupOperationType_GROUP_OP_INFO_UPDATE_NOTICE:
		return "群信息已更新"
	default:
		return "群通知"
	}
}

func NewFriendUpdateMsg(msgType transport.MessageType, f *model.UserFriend, targetID uint64) (*transport.WSMessage, error) {
	pbFriend := &social.Friend{
		UserId:     f.UserID,
		FriendId:   f.FriendID,
		Remark:     f.Remark,
		Starred:    f.Starred,
		Blocked:    f.Blocked,
		Source:     social.FriendSource(f.Source),
		CreateTime: f.CreateTime.UnixMilli(),
		Extra:      f.Extra,
	}

	payload, err := proto.Marshal(pbFriend)
	if err != nil {
		return nil, err
	}

	return &transport.WSMessage{
		RouteTarget:     []uint64{targetID},
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       time.Now().UnixMilli(),
		Type:            msgType,
		Payload:         payload,
	}, nil
}
