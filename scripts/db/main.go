// 数据库迁移脚本 - 将 IM2/internal/model 下的所有结构体导入到数据库
package main

import (
	"fmt"
	"log"

	"IM2/internal/model"
	"IM2/pkg/env"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func initDB() *gorm.DB {
	if err := env.LoadEnv(); err != nil {
		panic(fmt.Sprintf("加载 .env 文件失败: %v", err))
	}

	dsn := env.GetString("DB_ADDRESS_Friend", "")

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(fmt.Sprintf("数据库连接失败: %v", err))
	}
	return db
}

func main() {
	db := initDB()

	// 所有需要迁移的模型
	models := []interface{}{
		&model.UserFriend{},
		// &model.FriendApply{},
		// &model.Group{},
		// &model.Conversation{},
		// &model.UserConversation{},
		// &model.GroupMember{},
	}

	// 使用 AutoMigrate 批量迁移所有模型
	err := db.AutoMigrate(models...)
	if err != nil {
		log.Fatalf("数据库迁移失败: %v", err)
	}

	fmt.Println("数据库迁移完成！已创建以下表：")
	fmt.Println("  - user_info (用户信息表)")
	fmt.Println("  - user_friend (好友关系表)")
	fmt.Println("  - friend_apply (好友申请表)")
	fmt.Println("  - id_segment (ID号段表)")
	fmt.Println("  - group (群组表)")
	fmt.Println("  - group_apply (群组申请表)")
	fmt.Println("  - group_member (群成员表)")
}
