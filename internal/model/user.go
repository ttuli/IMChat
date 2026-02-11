package model

import "time"

type UserInfo struct {
	UserID            uint64    `gorm:"column:user_id;primaryKey;autoIncrement;comment:用户ID (主键)" json:"user_id"`
	UserName          string    `gorm:"column:user_name;type:varchar(64);not null;default:'';comment:用户昵称" json:"user_name"`
	Avatar            string    `gorm:"column:avatar;type:varchar(512);not null;default:'';comment:头像URL" json:"avatar"`
	Gender            uint8     `gorm:"column:gender;type:tinyint unsigned;not null;default:3;comment:性别: 3-未知, 1-男, 2-女" json:"gender"`
	Phone             string    `gorm:"column:phone;type:varchar(20);not null;uniqueIndex:uni_phone;comment:手机号" json:"phone"`
	JoinType          uint8     `gorm:"column:join_type;type:tinyint unsigned;not null;default:1;comment:添加方式: 1-需要验证, 2-直接同意" json:"join_type"`
	Password          string    `gorm:"column:password;type:varchar(255);not null;default:'';comment:加密后的密码" json:"password"`
	PersonalSignature string    `gorm:"column:personal_signature;type:varchar(255);not null;default:'';comment:个性签名" json:"personal_signature"`
	Status            uint8     `gorm:"column:status;type:tinyint unsigned;not null;default:1;comment:状态: 0-冻结, 1-正常" json:"status"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

// TableName 指定表名
func (UserInfo) TableName() string {
	return "user_info"
}

// 性别常量
const (
	GenderMale    uint8 = 1 // 男
	GenderFemale  uint8 = 2 // 女
	GenderUnknown uint8 = 3 // 未知
)

// 注册方式常量
const (
	JoinTypeVerify  uint8 = 1 // 需要验证
	JoinTypeDirect  uint8 = 2 // 直接同意
)

// 用户状态常量
const (
	UserStatusFrozen uint8 = 0 // 冻结
	UserStatusNormal uint8 = 1 // 正常
)