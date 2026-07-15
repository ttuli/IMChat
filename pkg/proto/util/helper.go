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

func NewMessageOperationMsg(opType transport.MessageType, operator uint64, msg *model.Message) (*transport.WSMessage, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	targetId, err := GetTargetIdFromSessionId(msg.SessionID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()

	ws := &transport.WSMessage{
		Type:        opType,
		RouteTarget: []uint64{targetId},
		Timestamp:   now,
	}
	if IsGroupSession(msg.SessionID) {
		ws.RouteTargetType = transport.TargetType_GROUP
	} else {
		ws.RouteTargetType = transport.TargetType_USER
	}

	recall := &message.MessageRecall{
		MsgId:      msg.MsgID,
		SessionId:  msg.SessionID,
		UserId:     operator,
		RecallTime: now,
	}

	payload, err := proto.Marshal(recall)
	if err != nil {
		return nil, err
	}
	ws.Payload = payload
	return ws, nil
}

func NewGroupOperationMsg(opType social.GroupOperationType, groupId uint64, targetIDs []uint64, operator uint64, groupInfo *model.Group) *svc.MessageSend {
	notify := &social.GroupNotification{
		OpType:     opType,
		GroupId:    groupId,
		OperatorId: operator,
		TargetIds:  targetIDs,
		OpTime:     time.Now().UnixMilli(),
		SessionId:  GenerateGroupSessionId(groupId),
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
	payload, err := proto.Marshal(notify)
	if err != nil {
		return nil
	}

	return &svc.MessageSend{
		SessionKey: GenerateGroupSessionId(groupInfo.ID),
		MsgType: int64(transport.MessageType_NOTIFICATION),
		Payload: payload,
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
