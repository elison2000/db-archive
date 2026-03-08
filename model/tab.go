package model

import (
	"db-archive/config"
	"db-archive/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
	"time"
)

type PreparePhase string

const (
	PrepareInit     PreparePhase = "init"
	PrepareRunning  PreparePhase = "preparing"
	PrepareFailed   PreparePhase = "failed"
	PreparePrepared PreparePhase = "prepared"
)

type ExecPhase string

const (
	ExecInit      ExecPhase = "init"      // 已创建，未就绪
	ExecQueueing  ExecPhase = "queueing"  // 已进入执行队列
	ExecRunning   ExecPhase = "running"   // 执行中
	ExecCompleted ExecPhase = "completed" // 已完成
	ExecFailed    ExecPhase = "failed"    // 执行失败
	ExecPaused    ExecPhase = "paused"    // 已暂停
	ExecStopped   ExecPhase = "stopped"   // 已停止(用户发起的)
	ExecResuming  ExecPhase = "resuming"  // 恢复中
)

type ArchiveMode string

const (
	ArchiveModeCopyOnly   ArchiveMode = "copy_only"   // 仅同步，不删除源端
	ArchiveModeMove       ArchiveMode = "move"        // 同步后删除源端
	ArchiveModeDeleteOnly ArchiveMode = "delete_only" // 仅删除源端，不同步
)

type WriteMode string

const (
	WriteInsert WriteMode = "insert" // 严格插入，冲突失败
	WriteUpsert WriteMode = "upsert" // 冲突覆盖（由 DBType 决定实现）
)

// DataSource 数据源定义表（Source / Sink 通用）
type DataSource struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`                  // 主键ID
	Name      string    `gorm:"column:name;size:100;not null" json:"name"`                     // 数据源名称
	Role      string    `gorm:"column:role;size:20;not null" json:"role"`                      // 角色，source / sink
	DBType    string    `gorm:"column:db_type;size:64;not null" json:"db_type"`                // 数据库类型，如 mysql / doris / oracle 等
	Host      string    `gorm:"column:host;size:128;not null" json:"host"`                     // 主机地址
	Port      int       `gorm:"column:port;not null" json:"port"`                              // 端口
	User      string    `gorm:"column:user;size:64;not null" json:"user"`                      // 用户名
	Password  string    `gorm:"column:password;size:128;not null" json:"password"`             // 密码
	Extra     string    `gorm:"column:extra" json:"extra"`                                     // 扩展配置，JSON格式，用于存储如 Doris Stream Load 等额外配置
	IsEnabled int8      `gorm:"column:is_enabled;default:1" json:"is_enabled"`                 // 是否启用，1启用，0禁用
	Remark    string    `gorm:"column:remark;size:200" json:"remark"`                          // 备注
	CreatedAt time.Time `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"` // 创建时间
	UpdatedAt time.Time `gorm:"column:updated_at;default:CURRENT_TIMESTAMP" json:"updated_at"` // 更新时间
}

func (*DataSource) TableName() string {
	return "data_sources"
}

func (d DataSource) String() string {
	var b strings.Builder
	b.Grow(256) // 预分配，避免扩容

	fmt.Fprintf(&b,
		"DataSource{ID:%d, Name:%s, Role:%s, DBType:%s, Host:%s, Port:%d, User:%s, Password:%s, Extra:%s}",
		d.ID,
		d.Name,
		d.Role,
		d.DBType,
		d.Host,
		d.Port,
		d.User,
		"***",   // 密码脱敏
		d.Extra, // Extra 允许打印
	)

	return b.String()
}
func (ds *DataSource) BeforeSave(_ *gorm.DB) error {
	if ds.Password != "" {
		ds.Password = util.AESEncrypt(ds.Password, config.Global.SecretKey)
	}
	return nil
}

func (ds *DataSource) AfterFind(_ *gorm.DB) error {
	if ds.Password != "" {
		pwd, err := util.AESDecrypt(ds.Password, config.Global.SecretKey)
		if err != nil {
			return nil
		}
		ds.Password = pwd
	}
	return nil
}

// ArchiveJob 归档配置表
type ArchiveJob struct {
	ID               int64       `gorm:"column:id;primaryKey;autoIncrement" json:"id"` // 主键ID
	Name             string      `gorm:"column:name;size:100;not null" json:"name"`
	SourceID         int64       `gorm:"column:source_id;not null" json:"source_id"`                        // 源数据源ID
	SourceDB         string      `gorm:"column:source_db;size:64;not null" json:"source_db"`                // 源数据库名称
	SourceTable      string      `gorm:"column:source_table;size:128;not null" json:"source_table"`         // 源表名称
	SinkID           int64       `gorm:"column:sink_id;not null" json:"sink_id"`                            // 目标数据源ID
	SinkDB           string      `gorm:"column:sink_db;size:64;default:''" json:"sink_db"`                  // 目标数据库名称
	SinkTable        string      `gorm:"column:sink_table;size:128;default:''" json:"sink_table"`           // 目标表名称
	ArchiveMode      ArchiveMode `gorm:"column:archive_mode;size:50;default:'archive'" json:"archive_mode"` // 归档模式
	WriteMode        WriteMode   `gorm:"column:write_mode;size:50;default:'insert'" json:"write_mode"`
	ArchiveCondition string      `gorm:"column:archive_condition;size:1000;default:''" json:"archive_condition"` // 归档条件
	IntervalDay      int         `gorm:"column:interval_day;default:1" json:"interval_day"`                      // 执行间隔天数
	TimeWindow       string      `gorm:"column:time_window;size:1000;default:'00:00-06:00'" json:"time_window"`  // 执行时间窗口
	Priority         int8        `gorm:"column:priority;default:1" json:"priority"`                              // 优先级
	SplitColumn      string      `gorm:"column:split_column;size:128;default:''" json:"split_column"`            // 分批字段
	SplitSize        int         `gorm:"column:split_size;default:10000" json:"split_size"`                      // 分批大小
	BatchSize        int         `gorm:"column:batch_size;default:1000" json:"batch_size"`                       // 每批写入/删除的行数
	Concurrency      int         `gorm:"column:concurrency;default:1" json:"concurrency"`                        // 并发数
	WriteRateLimit   int         `gorm:"column:write_rate_limit;default:10000" json:"write_rate_limit"`          // 写入速度限制（rows/sec）
	DeleteRateLimit  int         `gorm:"column:delete_rate_limit;default:10000" json:"delete_rate_limit"`
	IsEnabled        int8        `gorm:"column:is_enabled;default:1" json:"is_enabled"`                 // 是否启用，1启用，0禁用
	IsDeleted        int8        `gorm:"column:is_deleted;default:0" json:"is_deleted"`                 // 是否已删除，0否，1是
	Remark           string      `gorm:"column:remark;size:200" json:"remark"`                          // 备注
	CreatedAt        time.Time   `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"` // 创建时间
	UpdatedAt        time.Time   `gorm:"column:updated_at;default:CURRENT_TIMESTAMP" json:"updated_at"` // 更新时间
}

func (ArchiveJob) TableName() string {
	return "archive_jobs"
}

type ArchiveTask struct {
	ID               int64        `gorm:"column:id;primaryKey;autoIncrement" json:"id"` // 主键ID
	Name             string       `gorm:"column:name;size:100;not null" json:"name"`
	JobID            int64        `gorm:"column:job_id;not null" json:"job_id"`       // 归档任务ID
	SourceID         int64        `gorm:"column:source_id;not null" json:"source_id"` // 源数据源ID
	SourceDataSource DataSource   `gorm:"foreignKey:SourceID;references:ID"`
	SourceDB         string       `gorm:"column:source_db;size:64;not null" json:"source_db"`        // 源数据库名称
	SourceTable      string       `gorm:"column:source_table;size:128;not null" json:"source_table"` // 源表名称
	SinkID           int64        `gorm:"column:sink_id;not null" json:"sink_id"`                    // 目标数据源ID
	SinkDataSource   DataSource   `gorm:"foreignKey:SinkID;references:ID"`
	SinkDB           string       `gorm:"column:sink_db;size:64;default:''" json:"sink_db"`                  // 目标数据库名称
	SinkTable        string       `gorm:"column:sink_table;size:128;default:''" json:"sink_table"`           // 目标表名称
	ArchiveMode      ArchiveMode  `gorm:"column:archive_mode;size:50;default:'archive'" json:"archive_mode"` // 归档模式
	WriteMode        WriteMode    `gorm:"column:write_mode;size:50;default:'insert'" json:"write_mode"`
	ArchiveCondition string       `gorm:"column:archive_condition;size:1000;default:''" json:"archive_condition"` // 归档条件
	TimeWindow       string       `gorm:"column:time_window;size:1000;default:'00:00-06:00'" json:"time_window"`  // 执行时间窗口
	Priority         int8         `gorm:"column:priority;default:1" json:"priority"`                              // 优先级
	SplitColumn      string       `gorm:"column:split_column;size:128;default:''" json:"split_column"`            // 分批字段
	SplitSize        int          `gorm:"column:split_size;default:10000" json:"split_size"`                      // 分批大小
	BatchSize        int          `gorm:"column:batch_size;default:1000" json:"batch_size"`                       // 每批写入/删除的行数
	Concurrency      int          `gorm:"column:concurrency;default:1" json:"concurrency"`                        // 并发数
	WriteRateLimit   int          `gorm:"column:write_rate_limit;default:10000" json:"write_rate_limit"`          // 写入速度限制（rows/sec）
	DeleteRateLimit  int          `gorm:"column:delete_rate_limit;default:10000" json:"delete_rate_limit"`        // 删除速度限制（rows/sec）
	PreparePhase     PreparePhase `gorm:"column:prepare_phase;type:varchar(32);not null;default:'init'" json:"prepare_phase"`
	ExecPhase        ExecPhase    `gorm:"column:exec_phase;type:varchar(32);not null;default:'init'" json:"exec_phase"`
	ExecStart        *time.Time   `gorm:"column:exec_start" json:"exec_start"`                           // 执行开始时间
	ExecEnd          *time.Time   `gorm:"column:exec_end" json:"exec_end"`                               // 执行结束时间
	ExecSeconds      int          `gorm:"column:exec_seconds" json:"exec_seconds"`                       // 执行耗时秒数
	ReadRows         int64        `gorm:"column:read_rows;default:0" json:"read_rows"`                   // 读取行数
	InsertedRows     int64        `gorm:"column:inserted_rows;default:0" json:"inserted_rows"`           // 插入行数
	DeletedRows      int64        `gorm:"column:deleted_rows;default:0" json:"deleted_rows"`             // 删除行数
	Msg              string       `gorm:"column:msg;type:longtext" json:"msg"`                           // 任务执行消息
	IsEnabled        int8         `gorm:"column:is_enabled;default:1" json:"is_enabled"`                 // 是否启用，1启用，0禁用
	IsDeleted        int8         `gorm:"column:is_deleted;default:0" json:"is_deleted"`                 // 是否已删除，0否，1是
	CreatedAt        time.Time    `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"` // 创建时间
	UpdatedAt        time.Time    `gorm:"column:updated_at;default:CURRENT_TIMESTAMP" json:"updated_at"` // 更新时间
}

func (ArchiveTask) TableName() string {
	return "archive_tasks"
}

func (t ArchiveTask) String() string {
	var b strings.Builder
	b.Grow(512) // 预分配，避免扩容

	// 关键字段输出
	fmt.Fprintf(&b,
		"ArchiveTask{Name:%s, SourceID:%d, Source:%s.%s, SinkID:%d, Sink:%s.%s, "+
			"ArchiveMode:%s, WriteMode:%s, Condition:%s, "+
			"SplitColumn:%s, SplitSize:%d, Concurrency:%d, "+
			"WriteRate:%d, DeleteRate:%d}",
		t.Name,
		t.SourceID,
		t.SourceDB,
		t.SourceTable,
		t.SinkID,
		t.SinkDB,
		t.SinkTable,
		t.ArchiveMode,
		t.WriteMode,
		t.ArchiveCondition,
		t.SplitColumn,
		t.SplitSize,
		t.Concurrency,
		t.WriteRateLimit,
		t.DeleteRateLimit,
	)

	return b.String()
}

// ArchiveSubTask 归档子任务表
type ArchiveSubTask struct {
	ID            int64      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`                   // 主键ID
	TaskID        int64      `gorm:"column:task_id;not null" json:"task_id"`                         // 归档任务ID
	SplitColumn   string     `gorm:"column:split_column;size:128;default:''" json:"split_column"`    // 分批字段
	StartValue    string     `gorm:"column:start_value;size:128;not null" json:"start_value"`        // 分片起始值
	EndValue      string     `gorm:"column:end_value;size:128;not null" json:"end_value"`            // 分片结束值
	FullCondition string     `gorm:"column:full_condition;size:1000;not null" json:"full_condition"` // 最终执行的 SQL 条件
	ExecPhase     ExecPhase  `gorm:"column:exec_phase;type:varchar(32);not null;default:'init'" json:"exec_phase"`
	ExecStart     *time.Time `gorm:"column:exec_start" json:"exec_start"`                           // 执行开始时间
	ExecEnd       *time.Time `gorm:"column:exec_end" json:"exec_end"`                               // 执行结束时间
	ExecSeconds   int        `gorm:"column:exec_seconds" json:"exec_seconds"`                       // 执行耗时秒数
	ReadRows      int64      `gorm:"column:read_rows;default:0" json:"read_rows"`                   // 读取行数
	InsertedRows  int64      `gorm:"column:inserted_rows;default:0" json:"inserted_rows"`           // 插入行数
	DeletedRows   int64      `gorm:"column:deleted_rows;default:0" json:"deleted_rows"`             // 删除行数
	Msg           string     `gorm:"column:msg;type:longtext" json:"msg"`                           // 子任务执行消息
	CreatedAt     time.Time  `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"` // 创建时间
	UpdatedAt     time.Time  `gorm:"column:updated_at;default:CURRENT_TIMESTAMP" json:"updated_at"` // 更新时间
}

func (ArchiveSubTask) TableName() string {
	return "archive_sub_tasks"
}
