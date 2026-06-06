package fileupload

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"IM2/internal/apps/File/api/svc"
	"IM2/internal/apps/File/api/types"
	"IM2/pkg/proto/transport"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/aliyun/credentials-go/credentials"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetPostSignatureLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取签名
func NewGetPostSignatureLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPostSignatureLogic {
	return &GetPostSignatureLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetPostSignatureLogic) GetPostSignature(req *types.GetPostSignatureReq) (resp *types.PolicyToken, err error) {
	id := tokenmanager.ExtractIDFromCtx(l.ctx)

	var region, bucketName, dir, callbackUrl, product string
	switch req.FileType {
	case int32(types.FileType_FileTypeAvatar):
		region = l.svcCtx.Config.Oss.Avatar.Region
		bucketName = l.svcCtx.Config.Oss.Avatar.BucketName
		dir = l.svcCtx.Config.Oss.Avatar.Dir
		callbackUrl = l.svcCtx.Config.Oss.Avatar.CallbackURL
		product = l.svcCtx.Config.Oss.Avatar.Product
	case int32(types.FileType_FileTypeChatImage):
		region = l.svcCtx.Config.Oss.ChatImage.Region
		bucketName = l.svcCtx.Config.Oss.ChatImage.BucketName
		dir = l.svcCtx.Config.Oss.ChatImage.Dir
		callbackUrl = l.svcCtx.Config.Oss.ChatImage.CallbackURL
		product = l.svcCtx.Config.Oss.ChatImage.Product
	case int32(types.FileType_FileTypeChatFile):
		region = l.svcCtx.Config.Oss.ChatFile.Region
		bucketName = l.svcCtx.Config.Oss.ChatFile.BucketName
		dir = l.svcCtx.Config.Oss.ChatFile.Dir
		callbackUrl = l.svcCtx.Config.Oss.ChatFile.CallbackURL
		product = l.svcCtx.Config.Oss.ChatFile.Product
	default:
		return nil, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "文件类型不合法")
	}

	host := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bucketName, region)

	config := new(credentials.Config).
		SetType("ram_role_arn").
		SetAccessKeyId(os.Getenv("OSS_ACCESS_KEY_ID")).
		SetAccessKeySecret(os.Getenv("OSS_ACCESS_KEY_SECRET")).
		SetRoleArn(os.Getenv("OSS_STS_ROLE_ARN")).
		SetRoleSessionName("Role_Session_Name").
		SetPolicy("").
		SetRoleSessionExpiration(3600)

	// 根据配置创建凭证提供器
	provider, err := credentials.NewCredential(config)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_INTERNAL_SERVER, "获取签名失败")
	}

	// 从凭证提供器获取凭证
	cred, err := provider.GetCredential()
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_INTERNAL_SERVER, "获取签名失败")
	}

	// 构建policy
	utcTime := time.Now().UTC()
	date := utcTime.Format("20060102")
	expiration := utcTime.Add(1 * time.Hour)
	policyMap := map[string]any{
		"expiration": expiration.Format("2006-01-02T15:04:05.000Z"),
		"conditions": []any{
			map[string]string{"bucket": bucketName},
			map[string]string{"x-oss-signature-version": "OSS4-HMAC-SHA256"},
			map[string]string{"x-oss-credential": fmt.Sprintf("%v/%v/%v/%v/aliyun_v4_request", *cred.AccessKeyId, date, region, product)},
			map[string]string{"x-oss-date": utcTime.Format("20060102T150405Z")},
			map[string]string{"x-oss-security-token": *cred.SecurityToken},
		},
	}

	// 将policy转换为 JSON 格式
	policy, err := json.Marshal(policyMap)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_INTERNAL_SERVER, "获取签名失败")
	}

	// 构造待签名字符串（StringToSign）
	stringToSign := base64.StdEncoding.EncodeToString([]byte(policy))

	hmacHash := func() hash.Hash { return sha256.New() }
	// 构建signing key
	signingKey := "aliyun_v4" + *cred.AccessKeySecret
	h1 := hmac.New(hmacHash, []byte(signingKey))
	io.WriteString(h1, date)
	h1Key := h1.Sum(nil)

	h2 := hmac.New(hmacHash, h1Key)
	io.WriteString(h2, region)
	h2Key := h2.Sum(nil)

	h3 := hmac.New(hmacHash, h2Key)
	io.WriteString(h3, product)
	h3Key := h3.Sum(nil)

	h4 := hmac.New(hmacHash, h3Key)
	io.WriteString(h4, "aliyun_v4_request")
	h4Key := h4.Sum(nil)
	// 生成签名
	h := hmac.New(hmacHash, h4Key)
	io.WriteString(h, stringToSign)
	signature := hex.EncodeToString(h.Sum(nil))
	var callbackParam types.CallbackParam
	callbackParam.CallbackUrl = callbackUrl
	callbackParam.CallbackBody = fmt.Sprintf("file_name=${object}&size=${size}&mime_type=${mimeType}&height=${imageInfo.height}&width=${imageInfo.width}&id=%v&file_type=%v", id, req.FileType)
	callbackParam.CallbackBodyType = "application/x-www-form-urlencoded"
	callback_str, err := json.Marshal(callbackParam)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_INTERNAL_SERVER, "获取签名失败")
	}
	callbackBase64 := base64.StdEncoding.EncodeToString(callback_str)
	// 构建返回给前端的表单
	policyToken := &types.PolicyToken{
		Policy:               stringToSign,
		SecurityToken:        *cred.SecurityToken,
		XOssSignatureVersion: "OSS4-HMAC-SHA256",
		XOssCredential:       fmt.Sprintf("%v/%v/%v/%v/aliyun_v4_request", *cred.AccessKeyId, date, region, product),
		XOssDate:             utcTime.UTC().Format("20060102T150405Z"),
		Signature:            signature,
		Host:                 host,           // 返回 OSS 上传地址
		Dir:                  dir,            // 返回上传目录
		Callback:             callbackBase64, // 返回上传回调参数
	}
	return policyToken, nil
}
