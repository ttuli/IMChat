-- init-im.sql
-- 初始化 'im' 数据库 (用于 User, Id, Friend, FriendApply, Group, GroupApply 等微服务)
CREATE DATABASE IF NOT EXISTS `im` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建操作该库的用户并设置密码
CREATE USER IF NOT EXISTS 'im'@'%' IDENTIFIED BY '123456';

-- 授予该用户操作 'im' 数据库的所有权限
GRANT ALL PRIVILEGES ON `im`.* TO 'im'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 切换到 im 数据库
USE `im`;

-- 创建 user_info 表
CREATE TABLE `user_info` (
    `user_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '用户ID (主键)',
    `user_name` varchar(64) NOT NULL DEFAULT '' COMMENT '用户昵称',
    `avatar` varchar(512) NOT NULL DEFAULT '' COMMENT '头像URL',
    `gender` tinyint unsigned NOT NULL DEFAULT 3 COMMENT '性别: 3-未知, 1-男, 2-女',
    `phone` varchar(20) NOT NULL COMMENT '手机号',
    `join_type` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '添加方式: 1-需要验证, 2-直接同意',
    `password` varchar(255) NOT NULL DEFAULT '' COMMENT '加密后的密码',
    `personal_signature` varchar(255) NOT NULL DEFAULT '' COMMENT '个性签名',
    `status` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '状态: 0-冻结, 1-正常',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uni_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户信息表';

-- 创建 friend_apply 表
CREATE TABLE `friend_apply` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `from_user_id` bigint unsigned NOT NULL COMMENT '申请发起人ID',
    `to_user_id` bigint unsigned NOT NULL COMMENT '申请接收人ID',
    `apply_msg` varchar(255) NOT NULL DEFAULT '' COMMENT '申请理由/验证消息',
    `status` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '状态: 1-待处理, 2-已同意, 3-已拒绝, 4-已忽略',
    `source` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '来源',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '申请时间',
    `handle_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '处理时间',
    `reject_reason` varchar(255) NOT NULL DEFAULT '' COMMENT '拒绝原因',
    PRIMARY KEY (`id`),
    KEY `idx_from_user` (`from_user_id`),
    KEY `idx_to_user_status` (`to_user_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='好友申请记录表';

-- 创建 group 表
CREATE TABLE `group` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `owner_id` bigint unsigned NOT NULL COMMENT '群主ID',
    `name` varchar(255) NOT NULL COMMENT '群名称',
    `avatar` varchar(255) NOT NULL COMMENT '群头像',
    `notice` varchar(1000) NULL COMMENT '群公告',
    `member_count` int NOT NULL DEFAULT 1 COMMENT '群成员数',
    `join_type` int NOT NULL DEFAULT 1 COMMENT '加群方式',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='群组表';

-- 创建 group_apply 表
CREATE TABLE `group_apply` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `from_user_id` bigint unsigned NOT NULL COMMENT '申请发起人ID',
    `group_id` bigint unsigned NOT NULL COMMENT '群组ID',
    `apply_msg` varchar(255) NOT NULL DEFAULT '' COMMENT '申请理由/验证消息',
    `status` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '状态',
    `handler_id` bigint unsigned NULL COMMENT '处理人ID（群主或管理员）',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '申请时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '处理时间',
    PRIMARY KEY (`id`),
    KEY `idx_from_user` (`from_user_id`),
    KEY `idx_group_status` (`group_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='群组申请表';

-- 创建 group_invite 表
CREATE TABLE `group_invite` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `group_id` bigint unsigned NOT NULL COMMENT '群组ID',
    `inviter_id` bigint unsigned NOT NULL COMMENT '邀请人ID(群成员)',
    `invitee_id` bigint unsigned NOT NULL COMMENT '被邀请人ID',
    `status` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '状态(1:待处理 2:已接受 3:已拒绝)',
    `invite_msg` varchar(255) NOT NULL DEFAULT '' COMMENT '邀请语',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_group_invite_status` (`group_id`, `status`),
    KEY `idx_invitee_invite_status` (`invitee_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='群组邀请表';

-- 创建 group_member 表
CREATE TABLE `group_member` (
    `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `group_id` bigint unsigned NOT NULL COMMENT '群组ID',
    `user_id` bigint unsigned NOT NULL COMMENT '用户ID',
    `role` tinyint NOT NULL DEFAULT 3 COMMENT '角色: 1-群主, 2-管理员, 3-普通成员',
    `nickname` varchar(50) NOT NULL DEFAULT '' COMMENT '群内昵称',
    `mute_until` bigint NOT NULL DEFAULT 0 COMMENT '禁言截止时间（Unix时间戳，0表示未禁言）',
    `joined_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '加入时间',
    `extra` varchar(1000) NULL COMMENT '扩展字段(背景图、备注等)',
    PRIMARY KEY (`id`),
    KEY `idx_group_user` (`group_id`, `user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='群成员表';

-- 创建 id_segment 表
CREATE TABLE `id_segment` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `biz_tag` varchar(32) NOT NULL COMMENT '业务标签 (user, group, message等)',
    `max_id` bigint NOT NULL DEFAULT 0 COMMENT '当前号段的最大ID',
    `step` bigint NOT NULL DEFAULT 1000 COMMENT '号段步长，每次获取的ID数量',
    `version` bigint NOT NULL DEFAULT 0 COMMENT '版本号，用于乐观锁',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uni_biz_tag` (`biz_tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='号段表，用于号段模式生成ID';

-- 创建 user_friend 表
CREATE TABLE `user_friend` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `user_id` bigint unsigned NOT NULL COMMENT '用户ID (我)',
    `friend_id` bigint unsigned NOT NULL COMMENT '好友ID (对方)',
    `remark` varchar(64) NOT NULL DEFAULT '' COMMENT '好友备注名',
    `starred` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否星标好友',
    `blocked` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否拉黑好友',
    `source` tinyint unsigned NOT NULL DEFAULT 1 COMMENT '来源',
    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '成为好友时间',
    `extra` varchar(1000) NULL COMMENT '扩展字段(背景图、备注等)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uni_user_friend` (`user_id`, `friend_id`),
    KEY `idx_friend_id` (`friend_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='好友关系表';
