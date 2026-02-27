package common

import (
	"strconv"
	"strings"
	"time"

	"IM2/internal/model"

	"google.golang.org/protobuf/proto"
)

func GetConversationType(sessionId string) ConversationType {
	if IsGroupSession(sessionId) {
		return ConversationType_CONVERSATION_TYPE_GROUP
	}
	return ConversationType_CONVERSATION_TYPE_PRIVATE
}

func GenerateUserSessionId(userId uint64, targetId uint64) string {
	if userId < targetId {
		return "private_" + strconv.FormatUint(userId, 10) + "_" + strconv.FormatUint(targetId, 10)
	} else {
		return "private_" + strconv.FormatUint(targetId, 10) + "_" + strconv.FormatUint(userId, 10)
	}
}

func GenerateGroupSessionId(groupId uint64) string {
	return "group_" + strconv.FormatUint(groupId, 10)
}

func IsGroupSession(sessionId string) bool {
	return strings.HasPrefix(sessionId, "group")
}

func IsPrivateSession(sessionId string) bool {
	return strings.HasPrefix(sessionId, "private")
}

func IsChatMessage(t MessageType) bool {
	return t >= MessageType_CHAT_TEXT && t <= MessageType_GROUP_NOTICE
}

func IsNotifyMessage(t MessageType) bool {
	return t >= MessageType_NOTIFICATION && t <= MessageType_APPLY_REJECT
}

// ConvertFriendToWSMessage converts a model.UserFriend to a WSMessage
func ConvertFriendToWSMessage(f *model.UserFriend, targetID uint64) (*WSMessage, error) {
	pbFriend := &Friend{
		UserId:     f.UserID,
		FriendId:   f.FriendID,
		Remark:     f.Remark,
		Starred:    f.Starred,
		Blocked:    f.Blocked,
		Source:     FriendSource(f.Source),
		CreateTime: f.CreateTime.UnixMilli(),
		Extra:      f.Extra,
	}

	payload, err := proto.Marshal(pbFriend)
	if err != nil {
		return nil, err
	}

	return &WSMessage{
		RouteTarget:     targetID,
		RouteTargetType: TargetType_USER,
		Timestamp:       time.Now().UnixMilli(),
		Type:            MessageType_FRIEND_ADD,
		Payload:         payload,
	}, nil
}

// ConvertFriendApplyToWSMessage converts a model.FriendApply to a WSMessage
func ConvertFriendApplyToWSMessage(apply *model.FriendApply, targetID uint64) (*WSMessage, error) {
	pbApply := &FriendRequest{
		Id:           apply.ID,
		FromUserId:   apply.FromUserID,
		ToUserId:     apply.ToUserID,
		ApplyMsg:     apply.ApplyMsg,
		Status:       ApplyStatus(int32(apply.Status)),
		Source:       ApplySource(int32(apply.Source)),
		RequestTime:  apply.CreateTime.UnixMilli(),
		HandleTime:   apply.HandleTime.UnixMilli(),
		RejectReason: apply.RejectReason,
	}

	payload, err := proto.Marshal(pbApply)
	if err != nil {
		return nil, err
	}

	return &WSMessage{
		RouteTarget:     targetID,
		RouteTargetType: TargetType_USER,
		Timestamp:       apply.HandleTime.UnixMilli(),
		Type:            MessageType_FRIEND_REQUEST,
		Payload:         payload,
	}, nil
}

// ConvertGroupApplyToWSMessage converts a model.GroupApply to a WSMessage
func ConvertGroupApplyToWSMessage(apply *model.GroupApply, groupID uint64) (*WSMessage, error) {
	pbApply := &GroupApply{
		Id:          apply.ID,
		SenderId:    apply.FromUserID,
		GroupId:     apply.GroupID,
		ApplyMsg:    apply.ApplyMsg,
		Status:      GroupApplyStatus(apply.Status),
		HandlerId:   apply.HandlerID,
		RequestTime: apply.CreateTime.UnixMilli(),
		HandleTime:  apply.UpdateTime.UnixMilli(),
	}

	payload, err := proto.Marshal(pbApply)
	if err != nil {
		return nil, err
	}

	return &WSMessage{
		RouteTarget:     groupID,
		RouteTargetType: TargetType_GROUP,
		Timestamp:       apply.UpdateTime.UnixMilli(),
		Type:            MessageType_GROUP_REQUEST,
		Payload:         payload,
	}, nil
}

func NewGroupCreateNotification(operator uint64, members []*model.GroupMember, group *model.Group) *WSMessage {
	if len(members) == 0 {
		return nil
	}
	gid := members[0].GroupID
	wmsg := &WSMessage{
		Type:            MessageType_GROUP_CREATE,
		RouteTargetType: TargetType_GROUP,
		Timestamp:       time.Now().UnixMilli(),
		RouteTarget:     gid,
	}
	targets := make([]uint64, 0, len(members))
	for _, member := range members {
		targets = append(targets, member.UserID)
	}
	notify := &GroupNotification{
		OpType:     GroupOperationType_GROUP_OP_CREATE,
		GroupId:    gid,
		OperatorId: operator,
		TargetIds:  targets,
		OpTime:     time.Now().UnixMilli(),
	}
	if group != nil {
		notify.GroupInfo = &GroupInfo{
			Id:          group.ID,
			OwnerId:     group.OwnerID,
			Name:        group.Name,
			Avatar:      group.Avatar,
			Notice:      group.Notice,
			MemberCount: int32(group.MemberCount),
			CreateTime:  group.CreateTime.UnixMilli(),
			UpdateTime:  group.UpdateTime.UnixMilli(),
		}
	}
	payload, err := proto.Marshal(notify)
	if err != nil {
		return nil
	}
	wmsg.Payload = payload
	return wmsg
}

func NewGroupOperationMsg(msgType MessageType, groupId uint64, targetID uint64, operator uint64, groupInfo *model.Group) *WSMessage {
	wmsg := &WSMessage{
		Type:            msgType,
		RouteTargetType: TargetType_GROUP,
		Timestamp:       time.Now().UnixMilli(),
		RouteTarget:     groupId,
	}
	notify := &GroupNotification{
		GroupId:    groupId,
		OperatorId: operator,
		TargetIds:  []uint64{targetID},
		OpTime:     time.Now().UnixMilli(),
	}

	if groupInfo != nil {
		notify.GroupInfo = &GroupInfo{
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

	switch msgType {
	case MessageType_GROUP_CREATE:
		notify.OpType = GroupOperationType_GROUP_OP_CREATE
	case MessageType_GROUP_DISMISS:
		notify.OpType = GroupOperationType_GROUP_OP_DISMISS
	case MessageType_GROUP_JOIN:
		notify.OpType = GroupOperationType_GROUP_OP_JOIN
	case MessageType_GROUP_LEAVE:
		notify.OpType = GroupOperationType_GROUP_OP_LEAVE
	case MessageType_GROUP_KICK:
		notify.OpType = GroupOperationType_GROUP_OP_KICK
	case MessageType_GROUP_INVITE:
		notify.OpType = GroupOperationType_GROUP_OP_INVITE
	case MessageType_GROUP_INFO_UPDATE:
		notify.OpType = GroupOperationType_GROUP_OP_UPDATE_INFO
	}

	payload, err := proto.Marshal(notify)
	if err != nil {
		return nil
	}
	wmsg.Payload = payload
	return wmsg
}
