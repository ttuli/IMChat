-- init-imchat.sql
-- 初始化 'imchat' 数据库 (用于 Message, Contact 等微服务)
CREATE DATABASE IF NOT EXISTS `imchat` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建操作该库的用户并设置密码
CREATE USER IF NOT EXISTS 'imchat'@'%' IDENTIFIED BY 'zihang623';

-- 授予该用户操作 'imchat' 数据库的所有权限
GRANT ALL PRIVILEGES ON `imchat`.* TO 'imchat'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 切换到 imchat 数据库
USE `imchat`;

-- 创建 conversation 表
CREATE TABLE `conversation` (
    `conversation_id` varchar(128) NOT NULL COMMENT '唯一会话标识',
    `type` tinyint NOT NULL COMMENT '会话类型: 1-单聊 2-群聊',
    `last_content` varchar(1024) NOT NULL DEFAULT '' COMMENT '最后一条消息内容',
    `last_sender` bigint unsigned NOT NULL DEFAULT 0 COMMENT '最后发送者ID',
    `max_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT '最大消息序号',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    UNIQUE KEY `uni_conv_id` (`conversation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='会话表';

-- 创建 user_conversation 表
CREATE TABLE `user_conversation` (
    `user_id` bigint unsigned NOT NULL COMMENT '用户ID',
    `conversation_id` varchar(128) NOT NULL COMMENT '会话标识',
    `is_top` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否置顶',
    `is_disturb` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否免打扰',
    `is_mute` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否静音',
    `last_read_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT '最后已读消息序号',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    UNIQUE KEY `uni_user_conv` (`user_id`, `conversation_id`),
    KEY `idx_conv_id` (`conversation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户会话关系表';
