package service

import (
	"context"
	"errors"
	"time"

	"IM2/internal/apps/Idgen/rpc/config"
	"IM2/internal/apps/Idgen/rpc/idgen"
	repository "IM2/internal/apps/Idgen/rpc/internal/dao"
	"IM2/internal/model"
	"IM2/pkg/xerr"
)

// IDService ID生成服务接口
type IDService interface {
	// GetIDs 获取指定类型的ID
	GetIDs(ctx context.Context, idType idgen.IDType, count int32) ([]int64, error)
	// InitBizTags 初始化业务标签
	InitBizTags(ctx context.Context) error
	// SaveCacheState 保存缓存状态（用于优雅关闭）
	SaveCacheState(ctx context.Context) error
}

// idService ID服务实现
type idService struct {
	c     config.Config
	idDAO *repository.IdDAO
}

// NewIDService 创建ID服务
func NewIDService(c config.Config) IDService {
	idDAO := repository.NewIdDAO(c.IdDAO.Database)
	s := &idService{
		idDAO: idDAO,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := s.InitBizTags(ctx); err != nil {
		panic(err)
	}
	return s
}

// GetIDs 获取指定类型的ID
func (s *idService) GetIDs(ctx context.Context, idType idgen.IDType, count int32) ([]int64, error) {
	if count <= 0 {
		count = 1
	}

	bizTag, err := idTypeToBizTag(idType)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInvalidIDType, "获取ID失败")
	}

	ids, err := s.idDAO.GetNextIDs(ctx, bizTag, count)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "获取ID失败")
	}
	return ids, nil
}

// InitBizTags 初始化业务标签
func (s *idService) InitBizTags(ctx context.Context) error {
	err := s.idDAO.InitBizTags(ctx)
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "初始化业务标签失败")
	}
	return nil
}

// SaveCacheState 保存缓存状态
func (s *idService) SaveCacheState(ctx context.Context) error {
	err := s.idDAO.SaveCacheState(ctx)
	if err != nil {
		return xerr.Wrap(err, xerr.ErrCache, "保存缓存状态失败")
	}
	return nil
}

// idTypeToBizTag 将IDType枚举转换为业务标签
func idTypeToBizTag(idType idgen.IDType) (string, error) {
	switch idType {
	case idgen.IDType_ID_TYPE_USER:
		return model.BizTagUser, nil
	case idgen.IDType_ID_TYPE_GROUP:
		return model.BizTagGroup, nil
	case idgen.IDType_ID_TYPE_MESSAGE:
		return model.BizTagMessage, nil
	default:
		return "", errors.New("invaild id type")
	}
}
