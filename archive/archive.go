package archive

import (
	"context"
	"db-archive/model"
	"db-archive/util"
	"fmt"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"gorm.io/gorm"
	"log/slog"
	"os"
	"strings"
	"time"
)

type Archiver struct {
	MetaDB      *gorm.DB
	Task        *model.ArchiveTask
	Source      model.Source
	Sink        model.Sink
	logFile     *os.File
	Logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	innerCtx    context.Context
	innerCancel context.CancelFunc
}

func NewArchiver(metaDB *gorm.DB, task *model.ArchiveTask) *Archiver {
	return &Archiver{
		MetaDB: metaDB,
		Task:   task,
	}
}

func (a *Archiver) Init() (err error) {
	// 初始化数据库连接
	fileName := fmt.Sprintf("logs/%d.log", a.Task.ID)
	logFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	a.logFile = logFile

	baseLogger := util.NewFileLogger(logFile)
	logger := baseLogger.With("taskID", a.Task.ID)
	a.Logger = logger

	a.Logger.Info("初始化任务")
	a.Logger.Info("配置", "taskConfig", a.Task)
	a.Logger.Info("配置", "sourceConfig", a.Task.SourceDataSource)
	a.Logger.Info("配置", "sinkConfig", a.Task.SinkDataSource)

	a.Source, err = NewSource(a.Task)
	if err != nil {
		return fmt.Errorf("NewSource-> %w", err)
	}
	err = a.Source.Init()
	if err != nil {
		return fmt.Errorf("Source:Init-> %w", err)
	}

	a.Sink, err = NewSink(a.Task)
	if err != nil {
		return fmt.Errorf("NewSink-> %w", err)
	}

	sourceColumns := a.Source.GetColumns()
	if sourceColumns == nil || len(sourceColumns) == 0 {
		return fmt.Errorf("SourceColumns is empty")
	}

	err = a.Sink.Init(sourceColumns)
	if err != nil {
		return fmt.Errorf("Sink:Init-> %w", err)
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.innerCtx, a.innerCancel = context.WithCancel(context.Background())

	return nil
}

func (a *Archiver) InTimeWindow() (bool, error) {
	// 多个时间窗口
	now := time.Now()
	tws := strings.Split(a.Task.TimeWindow, ",")
	for _, tw := range tws {
		ok, err := util.InTimeWindow(now, tw)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (a *Archiver) Close() {

	//初始化的资源太多，有些成功有些失败，关闭前需要判断是否为空
	if a.logFile != nil {
		a.logFile.Close()
	}

	if a.Source != nil {
		a.Source.Close()
	}

	if a.Sink != nil {
		a.Sink.Close()
	}
}

func (a *Archiver) Cancel() {
	a.cancel() //优雅停止，可以恢复
}

func (a *Archiver) Terminate() {
	a.innerCancel() //终止，可能需要修复数据
}

func (a *Archiver) SaveTask() error {
	err := a.MetaDB.Save(a.Task).Error
	if err != nil {
		return fmt.Errorf("SaveTask-> %w", err)
	}
	return nil
}

func (a *Archiver) SaveSubTask(t *model.ArchiveSubTask) error {
	err := a.MetaDB.Save(t).Error
	if err != nil {
		return fmt.Errorf("SaveSubTask-> %w", err)
	}
	return nil
}

func (a *Archiver) CompareCols() (code int, err error) {
	/* code 返回值说明
	-1 未知
	0 一致
	1 字段类型或长度不一致
	2 字段个数不一致
	*/

	sourceCols, err := a.Source.GetColumnTypes()
	if err != nil {
		code = -1
		return
	}

	sinkCols, err := a.Sink.GetColumnTypes()
	if err != nil {
		code = -1
		return
	}

	missingInSinkCols := make([]string, 0)
	extraInSinkCols := make([]string, 0)
	diffCols := make([]string, 0)

	for sourceCol, sourceType := range sourceCols {
		if targetType, ok := sinkCols[sourceCol]; !ok {
			missingInSinkCols = append(missingInSinkCols, sourceCol)
		} else if targetType != sourceType {
			diffCols = append(diffCols, fmt.Sprintf("列 %s 类型不一致: %s -> %s", sourceCol, sourceType, targetType))
		}
	}

	for sinkCol, _ := range sinkCols {
		if _, ok := sourceCols[sinkCol]; !ok {
			extraInSinkCols = append(extraInSinkCols, sinkCol)
		}
	}

	if len(missingInSinkCols) > 0 {
		code = 2
		err = fmt.Errorf("sink缺少字段: %s\n", strings.Join(missingInSinkCols, ", "))
		return
	}

	if len(extraInSinkCols) > 0 {
		code = 2
		err = fmt.Errorf("sink多出字段: %s\n", strings.Join(extraInSinkCols, ", "))
		return
	}

	if len(diffCols) > 0 {
		code = 1
		err = fmt.Errorf("字段类型不一致: %s\n", strings.Join(diffCols, "\n"))
	}

	return
}

func (a *Archiver) getCount(sourceCondition, sinkCondition string) (sourceRowCount, sinkRowCount int64, err error) {

	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		var e error
		sourceRowCount, e = a.Source.GetCount(sourceCondition)
		return e
	})

	g.Go(func() error {
		var e error
		sinkRowCount, e = a.Sink.GetCount(sinkCondition)
		return e
	})

	err = g.Wait()
	return sourceRowCount, sinkRowCount, err
}

func (a *Archiver) archiveSubTask(limiter *rate.Limiter, subTask *model.ArchiveSubTask) (rowsRead, rowsInserted int64, err error) {

	logger := a.Logger.With("subTaskID", subTask.ID)
	logger.Info("开始归档")
	startTime := time.Now()

	defer func() {
		execSeconds := int(time.Since(startTime).Seconds())
		if err != nil {
			logger.Error("归档失败", "rowsRead", rowsRead, "rowsInserted", rowsInserted, "execSeconds", execSeconds, "err", err)
		} else {
			logger.Info("归档完成", "rowsRead", rowsRead, "rowsInserted", rowsInserted, "execSeconds", execSeconds)
		}
	}()

	dataChan := make(chan []any, 1000)

	g, ctx := errgroup.WithContext(a.innerCtx)

	// 读取
	g.Go(func() error {
		defer close(dataChan)
		n, err := a.Source.FetchBatch(ctx, dataChan, subTask.FullCondition)
		if err != nil {
			return err
		}
		rowsRead = n
		return nil
	})

	// 写入
	g.Go(func() error {
		n, err := a.Sink.WriteBatch(ctx, limiter, dataChan)
		if err != nil {
			return err
		}
		rowsInserted = n
		return nil
	})

	if err = g.Wait(); err != nil {
		err = fmt.Errorf("archiveSubTask-> %w", err)
		return
	}

	if rowsRead != rowsInserted {
		err = fmt.Errorf("读取行数和写入行数不相等")
		return
	}

	return
}

func (a *Archiver) deleteSubTask(limiter *rate.Limiter, subTask *model.ArchiveSubTask) (rowsDeleted int64, err error) {
	logger := a.Logger.With("subTaskID", subTask.ID)
	logger.Info("开始删除")
	startTime := time.Now()
	defer func() {
		if err != nil {
			logger.Error("删除失败", "rowsDeleted", rowsDeleted, "execSeconds", int(time.Since(startTime).Seconds()), "err", err)
		} else {
			logger.Info("删除完成", "rowsDeleted", rowsDeleted, "execSeconds", int(time.Since(startTime).Seconds()))
		}
	}()

	sourceCondition := subTask.FullCondition
	oldStr := " " + a.Source.GetDBName() + "."
	newStr := " " + a.Sink.GetDBName() + "."

	/*
	   where条件中存在子查询时,如：where pid in (select id from source_schema.xxx where ...)
	   需要把归档侧的子查询的source_schema替换成sink_schema
	*/
	sinkCondition := strings.Replace(subTask.FullCondition, oldStr, newStr, -1)

	logger.Info("预检查条件", "sourceCondition", sourceCondition, "sinkCondition", sinkCondition)

	sourceRowCount, sinkRowCount, err := a.getCount(sourceCondition, sinkCondition)
	if err != nil {
		return 0, fmt.Errorf("deleteSubTask-> %w", err)
	}

	if sourceRowCount != sinkRowCount {
		return 0, fmt.Errorf("source行数:%d, sink行数:%d, 预检查失败", sourceRowCount, sinkRowCount)
	}

	rowsDeleted, err = a.Source.DeleteBatch(a.innerCtx, limiter, sourceCondition)
	if err != nil {
		return rowsDeleted, fmt.Errorf("deleteSubTask:DeleteBatch-> %w", err)
	}

	if rowsDeleted != sourceRowCount {
		return rowsDeleted, fmt.Errorf("预计删除行数:%d, 实际删除行数:%d, 不符合预期", sourceRowCount, rowsDeleted)
	}

	return rowsDeleted, nil

}

func (a *Archiver) ExecuteSubTask(writeLimiter, deleteLimiter *rate.Limiter, subTask *model.ArchiveSubTask) (rowsRead, rowsInserted, rowsDeleted int64, err error) {
	logger := a.Logger.With("subTaskID", subTask.ID)
	logger.Info("开始执行子任务", "fullCondition", subTask.FullCondition)
	startTime := time.Now()
	defer func() {
		if err != nil {
			logger.Error("执行子任务失败", "execSeconds", int(time.Since(startTime).Seconds()), "err", err)
		} else {
			logger.Info("执行子任务完成", "execSeconds", int(time.Since(startTime).Seconds()))
		}
	}()

	subTask.ExecPhase = model.ExecRunning
	subTask.ExecStart = &startTime
	err = a.SaveSubTask(subTask)
	if err != nil {
		return rowsRead, rowsInserted, rowsDeleted, fmt.Errorf("ExecuteSubTask-> %w", err)
	}

	defer func() {
		endTime := time.Now()
		subTask.ReadRows = rowsRead
		subTask.InsertedRows = rowsInserted
		subTask.DeletedRows = rowsDeleted
		subTask.ExecStart = &startTime
		subTask.ExecEnd = &endTime
		subTask.ExecSeconds = int(time.Since(startTime).Seconds())
		if err != nil {
			subTask.ExecPhase = model.ExecFailed
			subTask.Msg = err.Error()
		} else {
			subTask.ExecPhase = model.ExecCompleted
		}

		// 使用err1,防止污染业务逻辑导致的错误，不能终止子任务
		if err1 := a.SaveSubTask(subTask); err1 != nil {
			logger.Error("保存子任务状态失败", "execSeconds", int(time.Since(startTime).Seconds()), "err", err1)
		}
	}()

	if a.Task.ArchiveMode == model.ArchiveModeCopyOnly || a.Task.ArchiveMode == model.ArchiveModeMove {
		rowsRead, rowsInserted, err = a.archiveSubTask(writeLimiter, subTask)
		if err != nil {
			return rowsRead, rowsInserted, rowsDeleted, fmt.Errorf("ExecuteSubTask-> %w", err)
		}

		if rowsRead != rowsInserted {
			return rowsRead, rowsInserted, rowsDeleted, fmt.Errorf("读取行数和写入行数不一致")
		}

	}

	if a.Task.ArchiveMode == model.ArchiveModeDeleteOnly || a.Task.ArchiveMode == model.ArchiveModeMove {
		rowsDeleted, err = a.deleteSubTask(deleteLimiter, subTask)
		if err != nil {
			return rowsRead, rowsInserted, rowsDeleted, fmt.Errorf("ExecuteSubTask-> %w", err)
		}

		if a.Task.ArchiveMode == model.ArchiveModeMove && rowsInserted != rowsDeleted {
			return rowsRead, rowsInserted, rowsDeleted, fmt.Errorf("写入行数和删除行数不一致")
		}
	}

	return rowsRead, rowsInserted, rowsDeleted, nil
}

func (a *Archiver) GetSubTask() (subTasks []*model.ArchiveSubTask, err error) {

	vals, err := a.Source.GetSplitValues()
	if err != nil {
		return nil, fmt.Errorf("GetSubTask-> %w", err)
	}

	if len(vals) == 0 {
		return
	}

	splitCol := a.Task.SplitColumn

	if len(vals) > 1 {
		// 生成子任务
		for i := 1; i < len(vals); i++ {
			where := fmt.Sprintf("%s AND %s >= %s AND %s < %s", a.Task.ArchiveCondition, splitCol, vals[i-1], splitCol, vals[i])
			subTask := &model.ArchiveSubTask{
				TaskID:        a.Task.ID,
				SplitColumn:   splitCol,
				StartValue:    vals[i-1],
				EndValue:      vals[i],
				FullCondition: where,
				ExecPhase:     model.ExecInit,
			}
			subTasks = append(subTasks, subTask)
		}
	}

	//处理最后1个值
	val := vals[len(vals)-1]
	where := fmt.Sprintf("%s AND %s >= %s", a.Task.ArchiveCondition, splitCol, val)
	subTask := &model.ArchiveSubTask{
		TaskID:        a.Task.ID,
		SplitColumn:   splitCol,
		StartValue:    val,
		FullCondition: where,
		ExecPhase:     model.ExecInit,
	}

	subTasks = append(subTasks, subTask)
	return subTasks, nil
}

func (a *Archiver) GenerateSubTask() (err error) {
	a.Logger.Info("开始生成子任务")
	var subTasks []*model.ArchiveSubTask
	startTime := time.Now()
	defer func() {
		if err != nil {
			a.Logger.Error("生成子任务失败", "execSeconds", int(time.Since(startTime).Seconds()), "err", err)
		} else {
			a.Logger.Info("生成子任务完成", "subTaskCount", len(subTasks), "execSeconds", int(time.Since(startTime).Seconds()))
		}
	}()

	// 不需要拆分的，直接生成1个子任务
	if a.Task.SplitColumn == "" {
		subTasks = append(subTasks, &model.ArchiveSubTask{
			TaskID:        a.Task.ID,
			SplitColumn:   "",
			FullCondition: a.Task.ArchiveCondition,
			ExecPhase:     model.ExecInit,
		})
	} else {
		subTasks, err = a.GetSubTask()
		if err != nil {
			return fmt.Errorf("GenerateSubTask-> %w", err)
		}

	}

	if len(subTasks) > 0 {
		err = a.MetaDB.CreateInBatches(subTasks, 1000).Error
		if err != nil {
			return fmt.Errorf("GenerateSubTask-> %w", err)
		}
	}

	return nil
}

func (a *Archiver) Run() {
	defer a.Close()

	err := a.Init()
	if err != nil {
		a.Logger.Error("初始化失败", "err", err)
		a.Task.Msg = err.Error()
		a.Task.ExecPhase = model.ExecFailed
		a.SaveTask()
		return
	}

	writeLimiter := rate.NewLimiter(rate.Limit(a.Task.WriteRateLimit), max(a.Task.BatchSize, a.Task.WriteRateLimit))
	deleteLimiter := rate.NewLimiter(rate.Limit(a.Task.DeleteRateLimit), max(a.Task.BatchSize, a.Task.DeleteRateLimit))
	executor := &DefaultExecutor{Archiver: a, WriteRateLimiter: writeLimiter, DeleteRateLimiter: deleteLimiter}
	if err := executor.Run(); err != nil {
		a.Logger.Error("执行任务失败", "err", err)
	}
}
