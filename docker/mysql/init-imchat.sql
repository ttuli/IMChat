-- init-imchat.sql
-- 初始化 'imchat' 数据库 (用于 Message, Contact 等微服务)
CREATE DATABASE IF NOT EXISTS `imchat` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建操作该库的用户并设置密码
CREATE USER IF NOT EXISTS 'imchat'@'%' IDENTIFIED BY 'zihang623';

-- 授予该用户操作 'imchat' 数据库的所有权限
GRANT ALL PRIVILEGES ON `imchat`.* TO 'imchat'@'%';

-- 刷新权限
FLUSH PRIVILEGES;
