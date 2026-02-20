package callback

import (
	"context"
	"fmt"

	file "IM2/internal/apps/File"
	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"
	"IM2/internal/apps/User/rpc/user"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
)

type UploadCallbackLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 上传回调
func NewUploadCallbackLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UploadCallbackLogic {
	return &UploadCallbackLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UploadCallbackLogic) UploadCallback(req *types.CallbackData) error {
	fmt.Println(*req)
	if req.FileName == "" {
		return xerr.New(xerr.ErrInvalidParams, "empty filename")
	}
	if req.Id == 0 {
		return xerr.New(xerr.ErrInvalidParams, "empty id")
	}

	switch file.FileType(req.FileType) {
	case file.FileTypeAvatar:
		region := l.svcCtx.Config.Oss.Avatar.Region
		bucketName := l.svcCtx.Config.Oss.Avatar.BucketName
		avatar := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com/%s", bucketName, region, req.FileName)
		_, err := l.svcCtx.UpdateInfo(l.ctx, &user.UpdateInfoReq{
			UserId: req.Id,
			Avatar: avatar,
		})
		if err != nil {
			return err
		}
	case file.FileTypeChatImage:
	case file.FileTypeChatFile:
	}

	return nil
}
