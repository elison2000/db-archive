package main

import (
	"context"
	"db-archive/archive"
	"db-archive/config"
	"db-archive/executor"
	"db-archive/http"
	"db-archive/model"
	"db-archive/util"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/gorm"
)

type job struct {
	model.ArchiveJob
	LastCreatedAt *time.Time      `gorm:"column:last_created_at" json:"last_created_at"`
	LastExecPhase model.ExecPhase `gorm:"column:last_exec_phase" json:"last_exec_phase"`
}

func GenerateTasksFromJobs(db *gorm.DB) {
	query := `with b as (select * from (SELECT job_id, created_at, exec_phase,ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY created_at DESC) as rn from archive_tasks) tmp where rn = 1)
select a.*,b.created_at as last_created_at,exec_phase last_exec_phase from archive_jobs a left join b on a.id=b.job_id where is_enabled = 1 AND is_deleted = 0`
	var jobs []*job
	err := db.Raw(query).Find(&jobs).Error
	if err != nil {
		Logger.Error("获取作业详情失败", "err", err)
		return
	}

	var tasks []*model.ArchiveTask
	for _, j := range jobs {
		if j.LastCreatedAt == nil { //以前从未运行
			t := GenerateTaskInstance(j)
			tasks = append(tasks, &t)
			continue
		}

		nextRunTime := j.LastCreatedAt.Truncate(time.Hour).Add(time.Duration(j.IntervalDay) * 24 * time.Hour)
		if time.Now().After(nextRunTime) {
			if j.LastExecPhase == model.ExecCompleted || j.LastExecPhase == model.ExecFailed || j.LastExecPhase == model.ExecStopped {
				t := GenerateTaskInstance(j)
				tasks = append(tasks, &t)
			} else {
				Logger.Error("上一次执行未结束", "jobID", j.ID, "lastExecPhase", j.LastExecPhase)
			}
		}

	}

	err = db.CreateInBatches(tasks, 100).Error
	if err != nil {
		Logger.Error("保存任务失败", "err", err)
		return
	}

}

func GenerateTaskInstance(j *job) model.ArchiveTask {
	return model.ArchiveTask{
		Name:             fmt.Sprintf("%s_%s", j.Name, time.Now().Format("20060102")),
		JobID:            j.ID,
		SourceID:         j.SourceID,
		SourceDB:         j.SourceDB,
		SourceTable:      j.SourceTable,
		SinkID:           j.SinkID,
		SinkDB:           j.SinkDB,
		SinkTable:        j.SinkTable,
		ArchiveMode:      j.ArchiveMode,
		WriteMode:        j.WriteMode,
		ArchiveCondition: util.ParseDateMacros(j.ArchiveCondition),
		TimeWindow:       j.TimeWindow,
		Priority:         j.Priority,
		SplitColumn:      j.SplitColumn,
		SplitSize:        j.SplitSize,
		BatchSize:        j.BatchSize,
		Concurrency:      j.Concurrency,
		WriteRateLimit:   j.WriteRateLimit,
		DeleteRateLimit:  j.DeleteRateLimit,
		PreparePhase:     "init", // 初始阶段
		ExecPhase:        "init",
		IsEnabled:        1,
		IsDeleted:        0,
	}
}

type Planner struct {
	metaDB   *gorm.DB
	executor *executor.Executor
	logger   *slog.Logger
}

func NewPlanner(metaDB *gorm.DB, executor *executor.Executor, logger *slog.Logger) *Planner {
	return &Planner{
		metaDB:   metaDB,
		executor: executor,
		logger:   logger,
	}
}

func (p *Planner) Plan(ctx context.Context) error {
	// 获取作业列表
	var tasks []*model.ArchiveTask
	var err error

	if p.executor.Remaining() <= 0 {
		p.logger.Info("任务队列已满")
		return nil
	}

	err = p.metaDB.Preload("SourceDataSource").Preload("SinkDataSource").
		Where("is_enabled = 1 AND is_deleted = 0 and created_at>=date_add(curdate(),interval -7 day) and exec_phase IN ?",
			[]model.ExecPhase{model.ExecInit, model.ExecPaused, model.ExecResuming}).Find(&tasks).Order("priority").Error
	if err != nil {
		return fmt.Errorf("Plan:Find-> %w", err)
	}

	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("Plan:ctx.Err-> %w", err)
		}

		if p.executor.Remaining() <= 0 {
			p.logger.Info("任务队列已满")
			return nil
		}

		if task.SourceDataSource.Host == "" || task.SinkDataSource.Host == "" {
			p.logger.Error("更新任务状态失败", "taskID", task.ID, "err", "source和sink数据源不能为空")
			continue
		}

		arch := archive.NewArchiver(p.metaDB, task)
		//p.logger.Info("任务配置", "taskID", task.ID, "taskConfig", task)

		ok, err := arch.InTimeWindow()
		if err != nil {
			p.logger.Error("检查时间窗口失败", "taskID", task.ID, "err", err)
			continue
		}
		if !ok {
			continue
		}

		if arch.Task.ExecPhase == model.ExecInit || arch.Task.ExecPhase == model.ExecPaused || arch.Task.ExecPhase == model.ExecResuming {
			arch.Task.ExecPhase = model.ExecQueueing
		} else {
			p.logger.Error("未知任务状态", "taskID", arch.Task.ID, "status", arch.Task.ExecPhase)
			continue
		}

		err = p.metaDB.Model(&model.ArchiveTask{}).
			Where("id = ?", arch.Task.ID).
			Update("exec_phase", arch.Task.ExecPhase).Error
		if err != nil {
			p.logger.Error("更新任务状态失败", "taskID", arch.Task.ID, "err", err)
			continue
		}

		p.logger.Info("新任务加入执行队列", "taskID", arch.Task.ID, "status", arch.Task.ExecPhase)
		p.executor.Submit(ctx, arch)
	}

	return nil
}

var MetaDB *gorm.DB
var Logger *slog.Logger
var Executor *executor.Executor

func main() {

	util.EnterWorkDir()
	//创建日志目录
	os.MkdirAll("logs", 0755)

	//获取参数
	config.Init()

	gLogF, err := os.OpenFile("gorm.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	MetaDB, err = util.NewMysqlORM(config.Global.DBConfig, gLogF)
	if err != nil {
		panic(err)
	}
	defer func() {
		Logger.Info("关闭数据库连接")
		db, _ := MetaDB.DB()
		_ = db.Close()
	}()

	err = MetaDB.Raw("select secret_key from secret_keys where access_key='default'").First(&config.Global.SecretKey).Error
	if err != nil {
		panic(err)
	}

	Logger = util.NewLogger()
	go func() {
		next := time.Now().Truncate(time.Hour).Add(time.Hour)
		time.Sleep(time.Until(next))

		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for t := range ticker.C {
			if t.Hour() == 0 {
				// 每天 00:00 整点，只会进一次
				GenerateTasksFromJobs(MetaDB)
			}
		}
	}()

	Executor = executor.NewExecutor(config.Global.Concurrency, 100, Logger)
	Executor.Start()

	planner := NewPlanner(MetaDB, Executor, Logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		Logger.Info("Planner启动")
		defer cancel()

		now := time.Now()
		next := now.Truncate(time.Minute).Add(time.Minute)
		sleep := time.Until(next)
		time.Sleep(sleep)
		ticker := time.NewTicker(60 * time.Second) //计时开始时刻对齐到00秒
		defer ticker.Stop()
		err = planner.Plan(ctx)
		if err != nil {
			Logger.Error("生成任务失败", "err", err)
		}
		for range ticker.C {
			if err := ctx.Err(); err != nil {
				break
			}
			Logger.Info("获取任务")
			err = planner.Plan(ctx)
			if err != nil {
				Logger.Error("生成任务失败", "err", err)
			}
		}

		Logger.Info("Planner退出")
	}()

	//启动http服务
	go func() {
		http.StartService(MetaDB, config.Global.HttpPort, Executor)
	}()

	// 监听 SIGTERM / SIGINT
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	<-sig
	Logger.Info("收到kill信号，准备退出")

	cancel()         //停止生成任务
	Executor.Stop()  // 触发 context cancel
	Executor.Close() // 停止接收新任务（可选）
	Executor.Wait()  // 等 worker 退出

	Logger.Info("修复正在排队的任务的状态", "err", err)
	err = MetaDB.Model(&model.ArchiveTask{}).Where("exec_phase = ?", model.ExecQueueing).Update("exec_phase", model.ExecPaused).Error
	if err != nil {
		Logger.Error("修复正在排队的任务的状态失败", "err", err)
	}

	Logger.Info("程序已退出")

}
