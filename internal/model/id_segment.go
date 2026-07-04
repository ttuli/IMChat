package model

import "time"

// IdSegment 号段表，用于号段模式生成ID
type IdSegment struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:主键ID" json:"id"`
	BizTag     string    `gorm:"column:biz_tag;type:varchar(32);not null;uniqueIndex:uni_biz_tag;comment:业务标签 (user, group, message等)" json:"biz_tag"`
	MaxID      int64     `gorm:"column:max_id;type:bigint;not null;default:0;comment:当前号段的最大ID" json:"max_id"`
	Step       int64     `gorm:"column:step;type:bigint;not null;default:1000;comment:号段步长，每次获取的ID数量" json:"step"`
	Version    int64     `gorm:"column:version;type:bigint;not null;default:0;comment:版本号，用于乐观锁" json:"version"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);comment:创建时间" json:"create_time"`
	UpdateTime time.Time `gorm:"column:update_time;type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoUpdateTime;comment:更新时间" json:"update_time"`
}

// TableName 指定表名
func (IdSegment) TableName() string {
	return "id_segment"
}

// 业务标签常量（对应 proto 中的 IDType）
const (
	BizTagUser    string = "user"    // 用户ID
	BizTagGroup   string = "group"   // 群组ID
	BizTagMessage string = "message" // 消息ID
)

// 默认步长
const (
	DefaultStep int64 = 1000 // 默认每次获取1000个ID
)

// ID长度配置
const (
	UserIDLength    int = 10 // 用户ID固定10位
	GroupIDLength   int = 8  // 群ID固定8位
	MessageIDLength int = 0  // 消息ID暂时搁置
)

// GetIDLength 获取指定业务标签的ID长度
func GetIDLength(bizTag string) int {
	switch bizTag {
	case BizTagUser:
		return UserIDLength
	case BizTagGroup:
		return GroupIDLength
	case BizTagMessage:
		return MessageIDLength
	default:
		return 0
	}
}

// GetIDRange 获取指定业务标签的ID范围
func GetIDRange(bizTag string) (minID, maxID int64) {
	length := GetIDLength(bizTag)
	if length <= 0 {
		return 0, 0
	}

	// 计算最小ID：10^(length-1)，例如10位是1000000000
	minID = 1
	for i := 0; i < length-1; i++ {
		minID *= 10
	}

	// 计算最大ID：10^length - 1，例如10位是9999999999
	maxID = minID*10 - 1

	return minID, maxID
}
