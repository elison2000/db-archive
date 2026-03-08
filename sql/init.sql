CREATE TABLE `data_sources`
(
    `id`         bigint       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name`       varchar(100) NOT NULL COMMENT '数据源名称',
    `role`       varchar(20)  NOT NULL COMMENT '角色，source / sink',
    `db_type`    varchar(64)  NOT NULL COMMENT '数据库类型，如 mysql / doris / oracle 等',
    `host`       varchar(128) NOT NULL COMMENT '主机地址',
    `port`       int          NOT NULL COMMENT '端口',
    `user`       varchar(64)  NOT NULL COMMENT '用户名',
    `password`   varchar(128) NOT NULL COMMENT '密码',
    `extra`      text COMMENT '扩展配置(JSON)，如 Doris Stream Load 等',
    `is_enabled` tinyint      DEFAULT '1' COMMENT '是否启用，1启用，0禁用',
    `remark`     varchar(200) DEFAULT NULL COMMENT '备注',
    `created_at` datetime     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY          `idx_role` (`role`),
    KEY          `idx_db_type` (`db_type`),
    KEY          `idx_is_enabled` (`is_enabled`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='数据源定义表（Source / Sink 通用）';



CREATE TABLE `archive_jobs`
(
    `id`                bigint       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name`              varchar(100) NOT NULL COMMENT '任务名称',
    `source_id`         bigint       NOT NULL COMMENT '源数据源ID',
    `source_db`         varchar(64)  NOT NULL COMMENT '源数据库名称',
    `source_table`      varchar(128) NOT NULL COMMENT '源表名称',
    `sink_id`           bigint       NOT NULL COMMENT '目标数据源ID',
    `sink_db`           varchar(64)   DEFAULT '' COMMENT '目标数据库名称',
    `sink_table`        varchar(128)  DEFAULT '' COMMENT '目标表名称',
    `archive_mode`      varchar(50)   DEFAULT 'copy_only' COMMENT '归档模式',
    `write_mode`        varchar(50)   DEFAULT 'insert' COMMENT '写入模式',
    `archive_condition` varchar(1000) DEFAULT '' COMMENT '归档条件',
    `interval_day`      int           DEFAULT '1' COMMENT '执行间隔天数',
    `time_window`       varchar(1000) DEFAULT '00:00-06:00' COMMENT '执行时间窗口',
    `priority`          tinyint       DEFAULT '1' COMMENT '优先级',
    `split_column`      varchar(128)  DEFAULT '' COMMENT '分批字段',
    `split_size`        int           DEFAULT '10000' COMMENT '分批大小',
    `batch_size`        int           DEFAULT '1000' COMMENT '每批写入/删除行数',
    `concurrency`       int           DEFAULT '1' COMMENT '并发数',
    `write_rate_limit`  int           DEFAULT '10000' COMMENT '写入限速 rows/sec',
    `delete_rate_limit` int           DEFAULT '10000' COMMENT '删除限速 rows/sec',
    `is_enabled`        tinyint       DEFAULT '1' COMMENT '是否启用',
    `is_deleted`        tinyint       DEFAULT '0' COMMENT '是否已删除',
    `remark`            varchar(200)  DEFAULT NULL COMMENT '备注',
    `created_at`        datetime      DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at`        datetime      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                 `idx_source` (`source_id`),
    KEY                 `idx_sink` (`sink_id`),
    KEY                 `idx_enabled` (`is_enabled`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='归档任务配置表';


CREATE TABLE `archive_tasks`
(
    `id`                bigint       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name`              varchar(100) NOT NULL COMMENT '任务名称',
    `job_id`            bigint       NOT NULL COMMENT '归档任务ID',
    `source_id`         bigint       NOT NULL COMMENT '源数据源ID',
    `source_db`         varchar(64)  NOT NULL COMMENT '源数据库名称',
    `source_table`      varchar(128) NOT NULL COMMENT '源表名称',
    `sink_id`           bigint       NOT NULL COMMENT '目标数据源ID',
    `sink_db`           varchar(64)           DEFAULT '' COMMENT '目标数据库名称',
    `sink_table`        varchar(128)          DEFAULT '' COMMENT '目标表名称',
    `archive_mode`      varchar(50)           DEFAULT 'copy_only' COMMENT '归档模式',
    `write_mode`        varchar(50)           DEFAULT 'insert' COMMENT '写入模式',
    `archive_condition` varchar(1000)         DEFAULT '' COMMENT '归档条件',
    `time_window`       varchar(1000)         DEFAULT '00:00-06:00' COMMENT '执行时间窗口',
    `priority`          tinyint               DEFAULT '1' COMMENT '优先级',
    `split_column`      varchar(128)          DEFAULT '' COMMENT '分批字段',
    `split_size`        int                   DEFAULT '10000' COMMENT '分批大小',
    `batch_size`        int                   DEFAULT '1000' COMMENT '每批写入/删除行数',
    `concurrency`       int                   DEFAULT '1' COMMENT '并发数',
    `write_rate_limit`  int                   DEFAULT '10000' COMMENT '写入限速',
    `delete_rate_limit` int                   DEFAULT '10000' COMMENT '删除限速',
    `prepare_phase`     varchar(32)  NOT NULL DEFAULT 'init' COMMENT '准备阶段状态',
    `exec_phase`        varchar(32)  NOT NULL DEFAULT 'init' COMMENT '执行阶段状态',
    `exec_start`        datetime              DEFAULT NULL COMMENT '执行开始时间',
    `exec_end`          datetime              DEFAULT NULL COMMENT '执行结束时间',
    `exec_seconds`      int                   DEFAULT NULL COMMENT '执行耗时秒',
    `read_rows`         bigint                DEFAULT '0' COMMENT '读取行数',
    `inserted_rows`     bigint                DEFAULT '0' COMMENT '插入行数',
    `deleted_rows`      bigint                DEFAULT '0' COMMENT '删除行数',
    `msg`               longtext COMMENT '任务执行消息',
    `is_enabled`        tinyint               DEFAULT '1' COMMENT '是否启用',
    `is_deleted`        tinyint               DEFAULT '0' COMMENT '是否已删除',
    `created_at`        datetime              DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at`        datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                 `idx_job` (`job_id`),
    KEY                 `idx_exec_phase` (`exec_phase`),
    KEY                 `idx_job_id_created_at` (`job_id`,`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='归档任务实例表';


CREATE TABLE `archive_sub_tasks`
(
    `id`             bigint        NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `task_id`        bigint        NOT NULL COMMENT '归档任务ID',
    `split_column`   varchar(128)           DEFAULT '' COMMENT '分批字段',
    `start_value`    varchar(128)  NOT NULL COMMENT '分片起始值',
    `end_value`      varchar(128)  NOT NULL COMMENT '分片结束值',
    `full_condition` varchar(1000) NOT NULL COMMENT '最终执行条件',
    `exec_phase`     varchar(32)   NOT NULL DEFAULT 'init' COMMENT '执行阶段状态',
    `exec_start`     datetime               DEFAULT NULL COMMENT '执行开始时间',
    `exec_end`       datetime               DEFAULT NULL COMMENT '执行结束时间',
    `exec_seconds`   int                    DEFAULT NULL COMMENT '执行耗时秒',
    `read_rows`      bigint                 DEFAULT '0' COMMENT '读取行数',
    `inserted_rows`  bigint                 DEFAULT '0' COMMENT '插入行数',
    `deleted_rows`   bigint                 DEFAULT '0' COMMENT '删除行数',
    `msg`            longtext COMMENT '子任务执行消息',
    `created_at`     datetime               DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at`     datetime               DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY              `idx_task` (`task_id`),
    KEY              `idx_exec_phase` (`exec_phase`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='归档子任务表';

CREATE TABLE `secret_keys`
(
    `id`         bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name`       varchar(100) NOT NULL COMMENT '秘钥名称/用途说明',
    `access_key` varchar(128) NOT NULL COMMENT '访问标识（可公开）',
    `secret_key` varchar(255) NOT NULL COMMENT '秘钥（敏感信息）',
    `is_enabled` tinyint      NOT NULL DEFAULT '1' COMMENT '状态：1-启用，0-禁用',
    `expired_at` datetime              DEFAULT NULL COMMENT '过期时间',
    `remark`     varchar(255)          DEFAULT NULL COMMENT '备注',
    `created_at` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_access_key` (`access_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='系统秘钥表';

insert into secret_keys(name, access_key, secret_key, is_enabled, expired_at, remark)
values ('default', 'default', 'RXJqDlO6HK0wfx5vTgF9ySt15mWZ8GI2', 1, '2099-09-09 00:00:00', '系统默认');