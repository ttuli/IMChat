package service

import (
	"context"

	model "IM2/internal/Entity"
	"IM2/internal/apps/Idgen/rpc/idgen"
	"IM2/pkg/encrypt"
	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

func (s *UserService) CreateUser(ctx context.Context, info *model.UserInfo) (uint64, error) {
	resp, err := s.idGenerator.GetId(ctx, &idgen.GetIdReq{
		IdType: idgen.IDType_ID_TYPE_USER,
		Count:  1,
	})
	if err != nil {
		return 0, xerr.Wrap(err, transport.ErrorCode_ERR_RPC, "调用ID生成服务失败")
	}
	if len(resp.Ids) == 0 {
		return 0, xerr.New(transport.ErrorCode_ERR_DATABASE, "生成ID失败")
	}
	info.UserID = uint64(resp.Ids[0])
	err = s.userDAO.InsertUser(ctx, info)
	if err != nil {
		return 0, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "插入用户失败")
	}
	return info.UserID, nil
}

func (s *UserService) VerifyPassword(ctx context.Context, userId uint64, password string) (bool, error) {
	user, err := s.userDAO.FindOneByID(ctx, userId)
	if err == gorm.ErrRecordNotFound {
		return false, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "用户不存在")
	}
	if err != nil {
		return false, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户失败")
	}
	return encrypt.ValidatePasswordHash(password, user.Password), nil
}

// GetUsersByIDs 根据ID列表获取用户
func (s *UserService) GetUsersByIDs(ctx context.Context, ids []uint64) ([]*model.UserInfo, error) {
	users, err := s.userDAO.FindByIDs(ctx, ids)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "批量查询用户失败")
	}
	return users, nil
}

// GetUserByPhone 根据手机号获取用户
func (s *UserService) GetUserByPhone(ctx context.Context, phone string) (*model.UserInfo, error) {
	user, err := s.userDAO.FindOneByPhone(ctx, phone)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "用户不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户失败")
	}
	return user, nil
}

// GetUsersByName 根据名字模糊查询用户
func (s *UserService) GetUsersByName(ctx context.Context, name string, limit, offset int32) ([]*model.UserInfo, error) {
	users, err := s.userDAO.FindByName(ctx, name, limit, offset)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "按名字查询用户失败")
	}
	return users, nil
}

// UpdateUserInfo 更新用户信息
func (s *UserService) UpdateUserInfo(ctx context.Context, id uint64, name, avatar string, gender, joinType uint8, personalSignature string) error {
	// 1. 查找用户
	user, err := s.userDAO.FindOneByID(ctx, id)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "用户不存在")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户失败")
	}

	// 2. 更新字段
	if name != "" {
		user.UserName = name
	}
	if avatar != "" {
		user.Avatar = avatar
	}
	if gender != 0 {
		user.Gender = gender
	}
	if joinType != 0 {
		user.JoinType = joinType
	}
	if personalSignature != "" {
		user.PersonalSignature = personalSignature
	}

	// 3. 保存
	err = s.userDAO.UpdateUser(ctx, user)
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新用户失败")
	}
	return nil
}
