package manager

import (
	"IM2/internal/apps/File/api/config"
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

type OssManager struct {
	cfg config.OssConfig
	cli *oss.Client
}

func NewOssManager(cfg config.OssConfig) *OssManager {
	cliCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(cfg.Region)

	cli := oss.NewClient(cliCfg)
	return &OssManager{
		cfg: cfg,
		cli: cli,
	}
}

func (o *OssManager) IsObjectExist(ctx context.Context, key string) (bool, error) {
	return o.cli.IsObjectExist(ctx, o.cfg.BucketName, key)
}
