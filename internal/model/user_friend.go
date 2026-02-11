package model

import "time"

// UserFriend 好友关系表
type UserFriend struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	UserID     uint64    `gorm:"column:user_id;type:bigint unsigned;not null;uniqueIndex:uni_user_friend;comment:用户ID (我)" json:"user_id"`
	FriendID   uint64    `gorm:"column:friend_id;type:bigint unsigned;not null;uniqueIndex:uni_user_friend;index:idx_friend_id;comment:好友ID (对方)" json:"friend_id"`
	Remark     string    `gorm:"column:remark;type:varchar(64);not null;default:'';comment:好友备注名" json:"remark"`
	Stared     bool      `gorm:"column:stared;type:tinyint(1);not null;default:0;comment:是否星标好友" json:"stared"`
	Blocked    bool      `gorm:"column:blocked;type:tinyint(1);not null;default:0;comment:是否拉黑好友" json:"blocked"`
	Source     uint8     `gorm:"column:source;type:tinyint unsigned;not null;default:1;comment:来源" json:"source"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:成为好友时间" json:"create_time"`
}

func (UserFriend) TableName() string {
	return "user_friend"
}

// 好友来源常量
const (
	FriendSourceSearch    uint8 = 1 // 搜索
	FriendSourceGroup     uint8 = 2 // 群聊
	FriendSourceRecommend uint8 = 3 // 推荐
)
