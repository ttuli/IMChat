package service

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"IM2/internal/apps/Message/rpc/internal/dao"
	model "IM2/internal/model"
	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/protobuf/proto"
)

// extraKey 以 MessageExtraKey 的数字值（而非枚举名字面量）作为 Extra map 的 key，
// 例如 WIDTH=10 → "10"。前端据同一枚举按数字索引读取，无需匹配枚举名。
func extraKey(k message.MessageExtraKey) string {
	return strconv.Itoa(int(k))
}

// GetHistory 获取历史消息（基于 Seq 区间分页）
// startSeq/endSeq 负数表示无界，limit 兜底最大 100。
func (s *MessageService) GetHistory(ctx context.Context, conversationID string, startSeq, endSeq int64, limit int) ([]*model.Message, error) {
	const maxLimit = 100
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}
	messages, err := s.svcCtx.MessageDAO.FindByConversation(ctx, conversationID, startSeq, endSeq, limit)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询历史消息失败")
	}
	return messages, nil
}

// PersistMessage 消费NATS消息、生成序号、广播事件、同步落库。
// streamSeq 是该消息的 JetStream stream sequence，作为 Lamport 时间戳源，
// 保证多实例并发消费同一会话时 seq 顺序与消息进入 stream 的顺序一致
// （不受实例间物理时钟倾斜影响）。
func (s *MessageService) PersistMessage(ctx context.Context, msg *svc.MessageSend, streamSeq uint64) (*model.Message, error) {
	// 0. 幂等：JetStream 是至少一次投递，落库成功后若进程崩溃 / Ack 丢失会重投同一条消息。
	// msg_id 是本函数现场生成的雪花号，重投时必然是新号，据此判重无效；
	// 真正稳定的幂等键是客户端带来的 (from_user_id, client_id)——网关发布到 JetStream 时
	// 用的也是这个键（nats.MsgId）。
	//
	// 命中已落库消息时直接复用它返回：上层据此回的 PersistAck 仍是**原** msg_id/seq，
	// 客户端本地 client_id → msg_id 的映射不会被同一条消息的第二个 msg_id 冲掉；
	// 重复投递给接收方的那份也因 msg_id 相同而能被客户端识别为重复。
	//
	// 服务端铸造的消息（撤回通知/群操作通知/通话记录）没有 client_id，跳过判重——
	// 它们由发布侧保证只发一次（如通话记录只由 callstate.Terminate 的 CAS 赢家发布）。
	if msg.ClientId != "" {
		existing, err := s.svcCtx.MessageDAO.FindBySenderAndClient(ctx, msg.Sender, msg.ClientId)
		switch {
		case err == nil:
			logger.Infof("[MessageService] duplicate delivery skipped: client_id=%s reuse msg_id=%s seq=%d",
				msg.ClientId, existing.MsgID, existing.Seq)
			return existing, nil
		case err != mongo.ErrNoDocuments:
			logger.Errorf("[MessageService] idempotency check failed for client_id %s: %v", msg.ClientId, err)
			return nil, err
		}
	}

	// 1. 分配 Lamport Seq（本地生成，不依赖 Redis）。
	// 进程首次遇到该会话时先从 MongoDB 播种已持久化的最大 seq，
	// 防止进程重启 + stream 重建导致新消息 seq 落后于历史消息。
	if !s.svcCtx.SeqAllocator.Known(msg.SessionId) {
		if maxSeq, err := s.svcCtx.MessageDAO.MaxSeq(ctx, msg.SessionId); err != nil {
			logger.Errorf("Failed to seed seq allocator for session %s: %v", msg.SessionId, err)
			return nil, err
		} else {
			s.svcCtx.SeqAllocator.Observe(msg.SessionId, maxSeq)
		}
	}
	seq := s.svcCtx.SeqAllocator.Alloc(msg.SessionId, streamSeq)

	// 通知类消息（群操作 606 / 撤回 605）：占用 seq 但不是用户可读的"最后一条消息"，
	// 不改写会话摘要，preview 保持上一条聊天消息
	isNotify := msg.MsgType == int64(transport.MessageType_GROUP_OP_NOTIFICATION) ||
		msg.MsgType == int64(transport.MessageType_MSG_OP_RECALL)

	// 2. 将完整会话状态推送到 SeqSyncer（异步批量刷 MySQL actual_seq + Redis 完整快照）。
	// 会话形态与目标显式传入：sessionId 是雪花 ID，SeqSyncer 无法再按前缀推断。
	s.svcCtx.SessionDAO.PushSeqUpdate(dao.SeqUpdate{
		SessionID:   msg.SessionId,
		SessionKey:  msg.SessionKey,
		SessionType: int(util.JudgeSessionType(msg.SessionKey)),
		Seq:         seq,
		LastContent: msg.Preview,
		LastSender:  msg.Sender,
		UpdateTime:  msg.Timestamp,
	})

	msgid := s.GenerateMsgId()

	// 4. 构建 db model 并落库
	dbMsg := &model.Message{
		MsgID:      msgid,
		ClientID:   msg.ClientId,
		SessionID:  msg.SessionId,
		SessionKey: msg.SessionKey,
		FromUserID: msg.Sender,
		MsgType:    int16(msg.MsgType),
		Seq:        seq,
		Status:     int8(message.MessageStatus_MESSAGE_STATUS_DELIVERED),
		Content:    msg.Preview,
		CreateTime: time.UnixMilli(msg.Timestamp),
		Extra:      make(map[string]any),
	}

	if msg.MsgType == int64(transport.MessageType_CHAT_IMAGE) ||
		msg.MsgType == int64(transport.MessageType_GROUP_IMAGE) ||
		msg.MsgType == int64(transport.MessageType_CHAT_VIDEO) ||
		msg.MsgType == int64(transport.MessageType_GROUP_VIDEO) ||
		msg.MsgType == int64(transport.MessageType_CHAT_FILE) ||
		msg.MsgType == int64(transport.MessageType_GROUP_FILE) {
		switch msg.MsgType {
		case int64(transport.MessageType_CHAT_IMAGE), int64(transport.MessageType_GROUP_IMAGE):
			imageMsg := &message.ImageMessage{}
			if err := proto.Unmarshal(msg.Payload, imageMsg); err != nil {
				logger.Errorf("Failed to unmarshal image message: %v", err)
				return nil, err
			}
			dbMsg.MediaURL = imageMsg.Url
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_SIZE)] = imageMsg.Size
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_NAME)] = imageMsg.FileName
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_FORMAT)] = imageMsg.Format

			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_WIDTH)] = imageMsg.Width
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_HEIGHT)] = imageMsg.Height
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_THUMB_WIDE)] = imageMsg.ThumbnailWidth
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_THUMB_HEIGHT)] = imageMsg.ThumbnailHeight
		case int64(transport.MessageType_CHAT_VIDEO), int64(transport.MessageType_GROUP_VIDEO):
			videoMsg := &message.VideoMessage{}
			if err := proto.Unmarshal(msg.Payload, videoMsg); err != nil {
				logger.Errorf("Failed to unmarshal video message: %v", err)
				return nil, err
			}
			dbMsg.MediaURL = videoMsg.Url
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_SIZE)] = videoMsg.Size
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_NAME)] = videoMsg.FileName
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_DURATION)] = videoMsg.Duration
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_FORMAT)] = videoMsg.Format

			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_WIDTH)] = videoMsg.Width
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_HEIGHT)] = videoMsg.Height
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_THUMB_WIDE)] = videoMsg.ThumbnailWidth
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_THUMB_HEIGHT)] = videoMsg.ThumbnailHeight
		case int64(transport.MessageType_CHAT_FILE), int64(transport.MessageType_GROUP_FILE):
			fileMsg := &message.FileMessage{}
			if err := proto.Unmarshal(msg.Payload, fileMsg); err != nil {
				logger.Errorf("Failed to unmarshal file message: %v", err)
				return nil, err
			}
			dbMsg.MediaURL = fileMsg.Url
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_SIZE)] = fileMsg.Size
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_NAME)] = fileMsg.FileName
			dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_FORMAT)] = fileMsg.Format
		}
	}

	// 通话记录：终态与类型在 CallMessage payload 内，而历史接口只返回
	// content / media_url / extra，不落 Extra 的话客户端翻历史时无法还原
	// 「未接来电」还是「通话时长 03:21」，只剩服务端下发的中性 Content 文案。
	if msg.MsgType == int64(transport.MessageType_CHAT_CALL) {
		callMsg := &message.CallMessage{}
		if err := proto.Unmarshal(msg.Payload, callMsg); err != nil {
			logger.Errorf("Failed to unmarshal call message: %v", err)
			return nil, err
		}
		dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_CALL_ID)] = callMsg.CallId
		dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_CALL_MEDIA_TYPE)] = int32(callMsg.MediaType)
		dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_CALL_END_REASON)] = int32(callMsg.EndReason)
		dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_DURATION)] = callMsg.Duration
	}

	// 通知类消息（群操作/撤回）与聊天消息同样落库：Content 为预览文案，
	// 完整结构化载荷存 extra.payload（十六进制），供历史拉取时重建通知内容。
	if isNotify {
		dbMsg.Extra[extraKey(message.MessageExtraKey_MESSAGE_EXTRA_KEY_NOTIFY_PAYLOAD)] = hex.EncodeToString(msg.Payload)
	}

	if err := s.svcCtx.MessageDAO.AppendMessages(ctx, dbMsg.SessionID, []*model.Message{dbMsg}); err != nil {
		logger.Errorf("Failed to persist message %s: %v", msgid, err)
		return nil, err
	}

	return dbMsg, nil
}

// RecallMessage 撤回消息
// 1. 校验消息是否存在
// 2. 校验是否为发送者本人
// 3. 校验是否在 2 分钟内
// 4. 更新消息状态为已撤回
func (s *MessageService) RecallMessage(ctx context.Context, userID uint64, msgID string, sessionID string) error {
	const recallWindowSeconds = 120 // 撤回时间窗口：2分钟

	// 1. 查询消息
	msg, err := s.svcCtx.MessageDAO.FindByMsgID(ctx, msgID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return xerr.Wrap(err, transport.ErrorCode_ERR_NOT_FOUND, "消息不存在")
		}
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询消息失败")
	}

	// 2. 校验发送者
	if msg.FromUserID != userID {
		return xerr.Wrap(nil, transport.ErrorCode_ERR_FORBIDDEN, "只能撤回自己发送的消息")
	}

	// 3. 校验撤回时间窗口
	if time.Since(msg.CreateTime).Seconds() > recallWindowSeconds {
		return xerr.Wrap(nil, transport.ErrorCode_ERR_FORBIDDEN, "超过撤回时间限制")
	}

	// 4. 原子置为已撤回（CAS：仅当尚未撤回时更新）。
	// 旧实现"先查 IsRecalled 再更新"存在竞态窗口：并发重复撤回会各自通过检查、
	// 各发一条撤回通知；CAS 保证只有一个请求完成变更并独占发布通知。
	recalledNow, err := s.svcCtx.MessageDAO.UpdateMessageStatusCAS(
		ctx, msgID, int8(message.MessageStatus_MESSAGE_STATUS_RECALLED))
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "撤回消息失败")
	}
	if !recalledNow {
		return xerr.Wrap(nil, transport.ErrorCode_ERR_FORBIDDEN, "该消息已被撤回或删除")
	}

	// 6. 撤回事件作为统一通知消息发布到落库队列：由消费链路分配 msg_id/seq
	// 持久化后按会话扇出，离线端也能按 seq 增量拉取感知撤回。
	// 撤回状态已落库（步骤 5），通知发布失败不影响撤回结果，客户端可拉取对齐。
	
	notifyMsg, err := util.NewRecallNotifyMsg(userID, msg)
	if err != nil {
		logger.Errorf("Failed to build recall notify for msg %s: %v", msgID, err)
		return nil
	}
	data, err := proto.Marshal(notifyMsg)
	if err != nil {
		logger.Errorf("Failed to marshal recall notify for msg %s: %v", msgID, err)
		return nil
	}
	if _, err := s.svcCtx.Nats.JetStream().Publish(nats_util.DBSubject, data); err != nil {
		logger.Errorf("Failed to publish recall notify for msg %s: %v", msgID, err)
	}

	return nil
}

// BulkPersistMessages 批量持久化消息，由 NATS Listener 消费后调用。
// 消息桶按会话组织，跨会话的一批必须先按 session_id 分组再各自追加。
func (s *MessageService) BulkPersistMessages(ctx context.Context, msgs []*model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	bySession := make(map[string][]*model.Message)
	for _, msg := range msgs {
		bySession[msg.SessionID] = append(bySession[msg.SessionID], msg)
	}
	for sessionID, group := range bySession {
		if err := s.svcCtx.MessageDAO.AppendMessages(ctx, sessionID, group); err != nil {
			logger.Errorf("[MessageService] BulkPersistMessages failed (session=%s, batch=%d): %v",
				sessionID, len(group), err)
			return err
		}
	}
	logger.Infof("[MessageService] BulkPersistMessages ok: %d messages persisted across %d sessions",
		len(msgs), len(bySession))
	return nil
}
