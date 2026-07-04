package dao

import (
	"context"

	model "IM2/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type AuthDAO struct {
	db *gorm.DB
}

func NewAuthDAO(dbSource string) *AuthDAO {
	db, err := gorm.Open(mysql.Open(dbSource), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return &AuthDAO{
		db: db,
	}
}

// InsertUser 写入新用户记录（注册场景）
func (d *AuthDAO) InsertUser(ctx context.Context, u *model.UserInfo) error {
	return d.db.WithContext(ctx).Create(u).Error
}

// FindOneByPhone 按手机号查找用户（注册校验场景）
func (d *AuthDAO) FindOneByPhone(ctx context.Context, phone string) (*model.UserInfo, error) {
	var u model.UserInfo
	if err := d.db.WithContext(ctx).Where("phone = ?", phone).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

// FindOneByID 按用户ID查找用户（登录场景）
func (d *AuthDAO) FindOneByID(ctx context.Context, id uint64) (*model.UserInfo, error) {
	var u model.UserInfo
	if err := d.db.WithContext(ctx).Where("user_id = ?", id).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}
