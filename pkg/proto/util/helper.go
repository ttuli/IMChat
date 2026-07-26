package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/proto/call"
	"IM2/pkg/proto/group"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/social"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"

	"google.golang.org/protobuf/proto"
)

func GetSessionType(sessionKey string) message.SessionType {
	if IsGroupSession(sessionKey) {
		return message.SessionType_SESSION_TYPE_GROUP
	}
	return message.SessionType_SESSION_TYPE_PRIVATE
}

func GenerateGroupSessionId(groupId uint64) string {
	return strconv.FormatUint(groupId, 10)
}

func IsGroupSession(sessionKey string) bool {
	return !IsPrivateSession(sessionKey)
}

func IsPrivateSession(sessionKey string) bool {
	return strings.Contains(sessionKey, "_")
}

func JudgeSessionType(sessionKey string) int32 {
	if IsGroupSession(sessionKey) {
		return int32(message.SessionType_SESSION_TYPE_GROUP)
	}
	return int32(message.SessionType_SESSION_TYPE_PRIVATE)
}

// GetTargetIdFromSessionKey 从会话ID中解析出目标ID（群ID或对方用户ID）
func GetTargetIdFromSessionKey(sessionKey string, currentUserId uint64) (uint64, error) {
	if IsGroupSession(sessionKey) {
		return strconv.ParseUint(sessionKey, 10, 64)
	} else if IsPrivateSession(sessionKey) {
		parts := strings.Split(sessionKey, "_")
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
	return t >= transport.MessageType_FRIEND_REQUEST && t <= transport.MessageType_GROUP_REQUEST
}

// IsCallSignal 判断是否为通话信令（800-899）。
// 信令帧纯实时转发：不进 DBSubject、不落库、不分配 seq，
// 与走 IsChatMessage 分支的聊天消息是完全独立的两条路径。
func IsCallSignal(t transport.MessageType) bool {
	return t >= transport.MessageType_CALL_INVITE && t <= transport.MessageType_CALL_END
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

// ConvertGroupInviteToWSMessage 将 model.GroupInvite 转为定向投递给被邀请人的 WSMessage
// （与群申请同机制：经 UserNotifier 单播，客户端据此写入邀请收件箱）。
func ConvertGroupInviteToWSMessage(invite *model.GroupInvite, targetIDs []uint64) (*transport.WSMessage, error) {
	pbInvite := &social.GroupInvite{
		Id:         invite.ID,
		GroupId:    invite.GroupID,
		InviterId:  invite.InviterID,
		InviteeId:  invite.InviteeID,
		Status:     social.InviteStatus(invite.Status),
		InviteMsg:  invite.InviteMsg,
		CreateTime: invite.CreateTime.UnixMilli(),
		UpdateTime: invite.UpdateTime.UnixMilli(),
	}

	payload, err := proto.Marshal(pbInvite)
	if err != nil {
		return nil, err
	}

	return &transport.WSMessage{
		RouteTarget:     targetIDs,
		RouteTargetType: transport.TargetType_USER,
		Timestamp:       invite.CreateTime.UnixMilli(),
		Type:            transport.MessageType_GROUP_INVITE,
		Payload:         payload,
	}, nil
}

// NewRecallNotifyMsg 构造消息撤回通知的落库消息（统一 NotifyMessage 载体）。
// 发布到 DBSubject 后由 Message 服务分配 msg_id/seq 持久化再扇出，
// 离线客户端可按会话 seq 增量拉取感知撤回。
// sessionKey 用于会话形态判定与目标解析（群聊=群ID，单聊=对方用户ID）。
func NewRecallNotifyMsg(operator uint64, msg *model.Message) (*svc.MessageSend, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	now := time.Now().UnixMilli()
	target, err := GetTargetIdFromSessionKey(msg.SessionKey, operator)
	if err != nil {
		return nil, err
	}

	payload, err := proto.Marshal(&message.NotifyMessage{
		Base: &message.BaseMessage{
			SessionId:  msg.SessionID,
			SessionKey: msg.SessionKey,
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
		SessionKey: msg.SessionKey,
		Sender:     operator,
		Target:     target,
		MsgType:    int64(transport.MessageType_MSG_OP_RECALL),
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
		MsgType:    int64(transport.MessageType_GROUP_OP_NOTIFICATION),
		Timestamp:  now,
		Preview:    GroupNotifyPreview(opType),
		Payload:    payload,
	}
}

// NewCallRecordMsg 构造通话记录的落库消息（CHAT_CALL=106）。
//
// **一通电话只调用一次**：调用方必须是 callstate.Terminate 的 CAS 赢家
// （Terminate 返回非 nil 快照），否则并发的 hangup / reject / 超时收敛会写出多条记录。
//
// base.from_user_id 恒为主叫，客户端据此判断展示视角（我方「已取消」/ 对方「未接来电」）。
// duration 由服务端 connected_at → ended_at 计得，不采信客户端上报值。
func NewCallRecordMsg(
	callID string,
	callerID, calleeID uint64,
	sessionKey string,
	mediaType call.CallMediaType,
	reason call.CallEndReason,
	duration int32,
) (*svc.MessageSend, error) {
	if callID == "" || sessionKey == "" {
		return nil, errors.New("invalid call record")
	}
	now := time.Now().UnixMilli()

	payload, err := proto.Marshal(&message.CallMessage{
		Base: &message.BaseMessage{
			SessionKey: sessionKey,
			FromUserId: callerID,
			Target:     calleeID,
			SendTime:   now,
		},
		CallId:    callID,
		MediaType: mediaType,
		EndReason: reason,
		Duration:  duration,
	})
	if err != nil {
		return nil, err
	}

	return &svc.MessageSend{
		SessionKey: sessionKey,
		Sender:     callerID,
		Target:     calleeID,
		MsgType:    int64(transport.MessageType_CHAT_CALL),
		Timestamp:  now,
		Preview:    CallPreview(reason, mediaType, duration),
		Payload:    payload,
	}, nil
}

// CallPreview 通话记录的会话列表摘要文案（无主语，与 GroupNotifyPreview 同契约）。
// 主被叫视角差异（「已取消」vs「未接来电」）由客户端按 from_user_id 重算，
// 服务端只给中性文案。
func CallPreview(reason call.CallEndReason, mediaType call.CallMediaType, duration int32) string {
	prefix := "语音通话"
	if mediaType == call.CallMediaType_CALL_MEDIA_TYPE_VIDEO {
		prefix = "视频通话"
	}

	switch reason {
	case call.CallEndReason_CALL_END_REASON_COMPLETED:
		return fmt.Sprintf("%s %s", prefix, formatDuration(duration))
	case call.CallEndReason_CALL_END_REASON_CANCELED:
		return prefix + " 已取消"
	case call.CallEndReason_CALL_END_REASON_REJECTED:
		return prefix + " 已拒绝"
	case call.CallEndReason_CALL_END_REASON_BUSY:
		return prefix + " 对方忙线"
	case call.CallEndReason_CALL_END_REASON_MISSED,
		call.CallEndReason_CALL_END_REASON_PEER_OFFLINE:
		return prefix + " 未接听"
	default:
		return prefix + " 已结束"
	}
}

// formatDuration 把秒数格式化为 mm:ss / hh:mm:ss
func formatDuration(sec int32) string {
	if sec < 0 {
		sec = 0
	}
	h, m, s := sec/3600, (sec%3600)/60, sec%60
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

// GroupNotifyPreview 群操作通知的会话列表摘要文案（无主语）。
// 操作人显示名因查看者而异（备注/群昵称/"你"），服务端无法预渲染，
// 由客户端在收到消息 / 离线补拉时按实际消息（含 operator/target）重算摘要。
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
