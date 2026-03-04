package fileupload

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"
	"IM2/pkg/signer"
	"IM2/pkg/xerr"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	stscredentials "github.com/aliyun/credentials-go/credentials"
)

type GetAccessUrlLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetAccessUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAccessUrlLogic {
	return &GetAccessUrlLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetAccessUrlLogic) GetAccessUrlLogic(req *types.GetAccessUrlReq) (*types.GetAccessUrlResp, error) {
	// 1. 根据 file_type 选择对应的 Bucket 配置
	var region, bucketName, product string
	switch req.FileType {
	case types.FileType_FileTypeChatImage:
		region = l.svcCtx.Config.Oss.ChatImage.Region
		bucketName = l.svcCtx.Config.Oss.ChatImage.BucketName
		product = l.svcCtx.Config.Oss.ChatImage.Product
	case types.FileType_FileTypeChatFile:
		region = l.svcCtx.Config.Oss.ChatFile.Region
		bucketName = l.svcCtx.Config.Oss.ChatFile.BucketName
		product = l.svcCtx.Config.Oss.ChatFile.Product
	default:
		return nil, xerr.New(xerr.ErrInvalidParams, "文件类型不合法")
	}

	// 2. 获取 STS 临时凭证
	config := new(stscredentials.Config).
		SetType("ram_role_arn").
		SetAccessKeyId(os.Getenv("OSS_ACCESS_KEY_ID")).
		SetAccessKeySecret(os.Getenv("OSS_ACCESS_KEY_SECRET")).
		SetRoleArn(os.Getenv("OSS_STS_ROLE_ARN")).
		SetRoleSessionName("Role_Session_Name").
		SetPolicy("").
		SetRoleSessionExpiration(3600)

	provider, err := stscredentials.NewCredential(config)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInternalServer, "获取凭证失败")
	}

	cred, err := provider.GetCredential()
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInternalServer, "获取凭证失败")
	}

	method:="GET"
	switch req.Method {
		case types.GetMethod_MethodHead:
			method="HEAD"
		default:
			method="GET"
	}
	fmt.Println(method)

	// 3. 构造目标文件的 GET 请求
	host := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bucketName, region)
	ossURL := fmt.Sprintf("%s/%s", host, req.FileKey)
	httpReq, err := http.NewRequest(method, ossURL, nil)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInternalServer, "构造请求失败")
	}

	// 3.5 如果有 OSS 图片处理参数，在签名前注入（必须在签名前加入，否则签名校验失败）
	if req.OssProcess != "" {
		q := httpReq.URL.Query()
		q.Set("x-oss-process", req.OssProcess)
		httpReq.URL.RawQuery = q.Encode()
	}

	// 4. 使用 SignerV4 进行 URL 签名 (AuthMethodQuery 模式)
	signerV4 := &signer.SignerV4{}
	signingCtx := &signer.SigningContext{
		Product: &product,
		Region:  &region,
		Bucket:  &bucketName,
		Key:     &req.FileKey,
		Request: httpReq,
		Credentials: &credentials.Credentials{
			AccessKeyID:     *cred.AccessKeyId,
			AccessKeySecret: *cred.AccessKeySecret,
			SecurityToken:   *cred.SecurityToken,
		},
		AuthMethodQuery: true,
		Time:            time.Now().UTC().Add(15 * time.Minute), // URL 15 分钟后过期
	}

	if err := signerV4.Sign(l.ctx, signingCtx); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInternalServer, "签名失败")
	}

	// 5. 返回预签名 URL
	return &types.GetAccessUrlResp{
		AccessUrl: httpReq.URL.String(),
	}, nil
}
