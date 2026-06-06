package model

import "time"

// UserFriend 好友关系表
type UserFriend struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	UserID     uint64    `gorm:"column:user_id;type:bigint unsigned;not null;uniqueIndex:uni_user_friend;comment:用户ID (我)" json:"user_id"`
	FriendID   uint64    `gorm:"column:friend_id;type:bigint unsigned;not null;uniqueIndex:uni_user_friend;index:idx_friend_id;comment:好友ID (对方)" json:"friend_id"`
	Remark     string    `gorm:"column:remark;type:varchar(64);not null;default:'';comment:好友备注名" json:"remark"`
	Starred    bool      `gorm:"column:starred;type:tinyint(1);not null;default:0;comment:是否星标好友" json:"starred"`
	Blocked    bool      `gorm:"column:blocked;type:tinyint(1);not null;default:0;comment:是否拉黑好友" json:"blocked"`
	Source     uint8     `gorm:"column:source;type:tinyint unsigned;not null;default:1;comment:来源" json:"source"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:成为好友时间" json:"create_time"`
	Extra      string    `gorm:"column:extra;type:varchar(1000);comment:扩展字段(背景图、备注等)" json:"extra"`
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

// ==================== 领域方法 ====================

// NewUserFriend 创建好友关系
func NewUserFriend(userID, friendID uint64, source uint8) *UserFriend {
	return &UserFriend{
		UserID:     userID,
		FriendID:   friendID,
		Source:     source,
		CreateTime: time.Now(),
	}
}

// SetRemark 设置备注
func (u *UserFriend) SetRemark(remark string) {
	u.Remark = remark
}

// SetStarred 设置星标
func (u *UserFriend) SetStarred(starred bool) {
	u.Starred = starred
}

// SetBlocked 设置拉黑
func (u *UserFriend) SetBlocked(blocked bool) {
	u.Blocked = blocked
}

// IsBlocked 是否已拉黑
func (u *UserFriend) IsBlocked() bool {
	return u.Blocked
}
