// 数据库迁移
package main

import (
	"fmt"

	"IM2/internal/model"
	"IM2/pkg/env"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func initDB() *gorm.DB {
	if err := env.LoadEnv(); err != nil {
		panic(fmt.Sprintf("加载 .env 文件失败: %v", err))
	}
	// 从环境变量读取数据库配置
	dbUser := env.GetString("DB_USER", "im")
	dbPassword := env.GetString("DB_PASSWORD", "123456") // 密码必须设置
	dbHost := env.GetString("DB_HOST", "127.0.0.1")
	dbPort := env.GetString("DB_PORT", "3306")
	dbName := env.GetString("DB_NAME", "im")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	d, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		panic(fmt.Sprintf("数据库连接失败: %v", err))
	}
	return d
}

func main() {
	d := initDB()
	err := d.AutoMigrate(model.UserInfo{})
	if err != nil {
		fmt.Println(err)
	}
	// err = d.AutoMigrate(model.UserFriend{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = d.AutoMigrate(model.FriendApply{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = d.AutoMigrate(model.IdSegment{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = d.AutoMigrate(model.Group{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = d.AutoMigrate(model.GroupApply{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = d.AutoMigrate(model.GroupMember{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	err = d.AutoMigrate(model.IdSegment{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("done")
}
