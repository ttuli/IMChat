package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type JoinGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *JoinGroupLogic {
	return &JoinGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 群申请管理
func (l *JoinGroupLogic) JoinGroup(in *group.JoinGroupReq) (*group.JoinGroupResp, error) {
	apply, member, err := l.svcCtx.GroupService.JoinGroup(l.ctx, in.GroupId, in.FromUserId, in.ApplyMsg)
	if err != nil {
		return nil, err
	}

	resp := &group.JoinGroupResp{}
	if apply != nil {
		resp.Data = &group.GroupRequest{
			Id:          apply.ID,
			FromUserId:  apply.FromUserID,
			GroupId:     apply.GroupID,
			ApplyMsg:    apply.ApplyMsg,
			Status:      group.ApplyStatus(apply.Status),
			HandlerId:   apply.HandlerID,
			RequestTime: apply.CreateTime.UnixMilli(),
			HandleTime:  apply.UpdateTime.UnixMilli(),
		}
	} else if member != nil {
		resp.Member = &group.GroupMember{
			GroupId:  member.GroupID,
			UserId:   member.UserID,
			Role:     group.GroupRole(member.Role),
			Nickname: member.Nickname,
			JoinedAt: member.JoinedAt.UnixMilli(),
		}
	}

	return resp, nil
}
