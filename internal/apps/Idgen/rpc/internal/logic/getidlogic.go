package logic

import (
	"context"

	"IM2/internal/apps/Idgen/rpc/idgen"
	"IM2/internal/apps/Idgen/rpc/svc"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetIdLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetIdLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetIdLogic {
	return &GetIdLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetId 获取单个或多个ID
func (l *GetIdLogic) GetId(in *idgen.GetIdReq) (*idgen.GetIdResp, error) {
	// 参数校验
	if in == nil {
		return nil, xerr.New(xerr.ErrInvalidParams, "请求参数不能为空")
	}

	// 调用 service 层获取ID
	ids, err := l.svcCtx.IDService.GetIDs(l.ctx, in.GetIdType(), in.GetCount())
	if err != nil {
		return nil, err
	}

	return &idgen.GetIdResp{
		Ids: ids,
	}, nil
}
