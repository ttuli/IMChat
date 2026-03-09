-- init-im.sql
-- 初始化 'im' 数据库 (用于 User, Id, Friend, FriendApply, Group, GroupApply 等微服务)
CREATE DATABASE IF NOT EXISTS `im` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建操作该库的用户并设置密码
CREATE USER IF NOT EXISTS 'im'@'%' IDENTIFIED BY '123456';

-- 授予该用户操作 'im' 数据库的所有权限
GRANT ALL PRIVILEGES ON `im`.* TO 'im'@'%';

-- 刷新权限
FLUSH PRIVILEGES;
