package service

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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

// ErrPoison 标记「重试多少次都不会成功」的毒消息（如载荷反序列化失败）。
// listener 据此跳过退避重投直接转入 DLQ，DLQ 重放时也应跳过这类消息。
// 用 errors.Is 判断，包装链上任意一层带上它即可。
var ErrPoison = errors.New("poison message")

// Delivery 描述一次 JetStream 投递的元数据，由 listener 从 nats.Msg 提取后传入。
// 元数据缺失（如 DLQ 重放）时为零值，各字段都退化为最保守的处理路径。
type Delivery struct {
	// StreamSeq 该消息的 stream sequence，作为 Lamport 时间戳源；
	// 0 表示缺失，seq 分配退化为本地逻辑时钟。
	StreamSeq uint64
	// NumDelivered 该消息被**当前 consumer** 投递的次数，首投为 1；0 表示缺失。
	NumDelivered uint64
	// PredatesConsumer 该消息写入 stream 的时间早于当前 consumer 的创建时间。
	PredatesConsumer bool
}

// MaybeDuplicate 报告本次投递是否**可能**已被处理过，决定是否需要付一次判重查询。
//
// NumDelivered == 1 且消息晚于当前 consumer 创建时，重复在两条来源上都不可能：
//   - 消费侧重投（Nak / AckWait 超时 / 落库后崩溃未 Ack）必然带来 NumDelivered >= 2；
//   - 客户端未收到 ACK 的重发在发布侧就被 nats.MsgId 去重窗口折叠，进不了 stream
//     （见 nats_util.streamDuplicates —— 该窗口是本判断成立的前提）。
//
// PredatesConsumer 覆盖的是 consumer 被删除重建：投递计数是 consumer 级状态，
// 新 consumer 会把 stream 里的存量消息（MaxAge 内）从头再投一遍且计数归零——
// 这些消息可能已被前一个 consumer 落过库，只看 NumDelivered 会全部跳过判重、
// 批量落成重复消息。写入早于 consumer 创建的消息一律判重即可堵住：
// 晚于创建的消息，前一个 consumer 当时已不存在，不可能处理过它。
//
// NumDelivered == 0 表示元数据缺失，无从判断，一律按可能重复处理。
func (d Delivery) MaybeDuplicate() bool {
	return d.NumDelivered != 1 || d.PredatesConsumer
}

// PersistMessage 消费NATS消息、生成序号、广播事件、同步落库。
// d 携带 JetStream 投递元数据：stream sequence 作为 Lamport 时间戳源，
// 保证多实例并发消费同一会话时 seq 顺序与消息进入 stream 的顺序一致
// （不受实例间物理时钟倾斜影响）；投递次数用于跳过不必要的判重查询。
func (s *MessageService) PersistMessage(ctx context.Context, msg *svc.MessageSend, d Delivery) (*model.Message, error) {
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
	//
	// MaybeDuplicate 把这次查询限制在真正可能重复的投递上：正常系统的重投率远低于
	// 千分之一，等于免掉了热路径上一次带索引的 Mongo 往返。
	if msg.ClientId != "" && d.MaybeDuplicate() {
		existing, err := s.svcCtx.MessageDAO.FindBySenderAndClient(ctx, msg.Sender, msg.ClientId)
		switch {
		case err == nil:
			logger.Infof("[MessageService] duplicate delivery skipped: client_id=%s reuse msg_id=%s seq=%d",
				msg.ClientId, existing.MsgID, existing.Seq)
			// 补推一次摘要：上一次投递可能在落库后、SeqSyncer 刷盘前崩溃（摘要在进程内
			// channel 里随进程丢失），重投命中判重时是唯一的补救时机。
			// SeqSyncer 按 seq 条件写，摘要已生效时这里是无害的空操作。
			s.pushSessionSummary(msg, existing.Seq)
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
	seq := s.svcCtx.SeqAllocator.Alloc(msg.SessionId, d.StreamSeq)

	// 通知类消息（群操作 606 / 撤回 605）：占用 seq 但不是用户可读的"最后一条消息"，
	// 不改写会话摘要，preview 保持上一条聊天消息
	isNotify := msg.MsgType == int64(transport.MessageType_GROUP_OP_NOTIFICATION) ||
		msg.MsgType == int64(transport.MessageType_MSG_OP_RECALL)

	msgid := s.GenerateMsgId()

	// 2. 构建 db model 并落库
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
				return nil, fmt.Errorf("%w: unmarshal image payload: %v", ErrPoison, err)
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
				return nil, fmt.Errorf("%w: unmarshal video payload: %v", ErrPoison, err)
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
				return nil, fmt.Errorf("%w: unmarshal file payload: %v", ErrPoison, err)
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
			return nil, fmt.Errorf("%w: unmarshal call payload: %v", ErrPoison, err)
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

	// 3. 落库成功后才推进会话摘要。
	// 必须在 AppendMessages 之后：摘要一旦指向某个 seq，会话列表就会展示这条预览、
	// actual_seq 也会前进。先推后写的话，写失败的消息会在会话列表留下一条点进去
	// 不存在的「最后一条消息」，且每次重试都分配新 seq 再推一次——进了 DLQ 的消息
	// 会永久留下这个幽灵预览。
	s.pushSessionSummary(msg, seq)

	return dbMsg, nil
}

// pushSessionSummary 把一条已落库消息推进到会话摘要（SeqSyncer 异步批量刷
// MySQL actual_seq + Redis 完整快照）。SeqSyncer 按 seq 条件写，重复推送同一
// seq 或更旧的 seq 都不会回退摘要，因此可安全地在重投路径上补推。
// 会话形态与目标显式传入：sessionId 是雪花 ID，SeqSyncer 无法再按前缀推断。
func (s *MessageService) pushSessionSummary(msg *svc.MessageSend, seq uint64) {
	s.svcCtx.SessionDAO.PushSeqUpdate(dao.SeqUpdate{
		SessionID:   msg.SessionId,
		SessionKey:  msg.SessionKey,
		SessionType: int(util.JudgeSessionType(msg.SessionKey)),
		Seq:         seq,
		LastContent: msg.Preview,
		LastSender:  msg.Sender,
		UpdateTime:  msg.Timestamp,
	})
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
