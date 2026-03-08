package archive

import (
	"context"
	"db-archive/model"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"sync/atomic"
	"time"
)

var ErrOutOfTimeWindow = errors.New("out of time window")

type DefaultExecutor struct {
	Archiver          *Archiver
	WriteRateLimiter  *rate.Limiter
	DeleteRateLimiter *rate.Limiter
}

func (e *DefaultExecutor) Run() error {
	a := e.Archiver
	if a.Task.PreparePhase == model.PrepareInit {
		if err := e.Prepare(); err != nil {
			a.Logger.Error("Prepare失败", "err", err)
			return err
		}
	}

	if a.Task.PreparePhase == model.PreparePrepared {
		if err := e.Execute(); err != nil {
			a.Logger.Error("Execute失败", "err", err)
			return err
		}
	}
	return nil
}

func (e *DefaultExecutor) Prepare() (err error) {
	a := e.Archiver

	if a.Task.PreparePhase != model.PrepareInit {
		return fmt.Errorf("PreparePhase!=%s", model.PrepareInit)
	}

	a.Task.PreparePhase = model.PrepareRunning
	err = a.SaveTask()
	if err != nil {
		return fmt.Errorf("Prepare-> %w", err)
	}

	defer func() {
		if err != nil {
			a.Task.Msg = err.Error()
		}
		a.SaveTask()
	}()

	err = a.GenerateSubTask()
	if err != nil {
		a.Task.PreparePhase = model.PrepareFailed
		return fmt.Errorf("Prepare-> %w", err)
	}

	a.Task.PreparePhase = model.PreparePrepared

	return nil
}

func (e *DefaultExecutor) Execute() (err error) {
	a := e.Archiver

	if a.Task.PreparePhase != model.PreparePrepared {
		return fmt.Errorf("PreparePhase!=%s", model.PreparePrepared)
	}

	if a.Task.ExecPhase != model.ExecQueueing {
		return fmt.Errorf("ExecPhase!=%s", model.ExecQueueing)
	}

	startTime := time.Now()
	a.Task.ExecPhase = model.ExecRunning
	a.Task.ExecStart = &startTime
	err = a.SaveTask()
	if err != nil {
		return fmt.Errorf("Execute-> %w", err)
	}

	var successCount int64
	var subTasks []*model.ArchiveSubTask
	defer func() {
		if err != nil {
			a.Task.Msg = err.Error()
		}

		endTime := time.Now()
		a.Task.ExecEnd = &endTime
		a.Task.ExecSeconds += int(time.Since(startTime).Seconds())

		a.Logger.Info("任务执行完成", "subTaskCount", len(subTasks), "successCount", successCount, "ExecSeconds", a.Task.ExecSeconds)
		a.Logger.Info("行数汇总", "ReadRows", a.Task.ReadRows, "InsertedRows", a.Task.InsertedRows, "DeletedRows", a.Task.DeletedRows)

		a.SaveTask()
	}()

	if err = a.MetaDB.Where("task_id = ? and exec_phase = ?", a.Task.ID, model.ExecInit).
		Find(&subTasks).Error; err != nil {
		a.Task.ExecPhase = model.ExecFailed
		return err
	}

	if len(subTasks) == 0 {
		a.Task.ExecPhase = model.ExecFailed
		return fmt.Errorf("没有待执行子任务")
	}

	// ctx cancel会传递到gCtx，gCtx cancel不会传递到ctx,单向传递
	g, gCtx := errgroup.WithContext(a.ctx)

	concurrency := max(1, min(a.Task.Concurrency, 32))
	a.Logger.Info("并发执行子任务", "concurrency", concurrency, "subTaskCount", len(subTasks))
	g.SetLimit(concurrency)

	var progress int64
	for i := range subTasks {
		st := subTasks[i]
		g.Go(func() error {
			if err := gCtx.Err(); err != nil {
				return err
			}

			if ok, _ := e.Archiver.InTimeWindow(); !ok {
				return ErrOutOfTimeWindow
			}

			rowsRead, rowsInserted, rowsDeleted, err := a.ExecuteSubTask(e.WriteRateLimiter, e.DeleteRateLimiter, st)
			if err != nil {
				return err
			}

			atomic.AddInt64(&a.Task.ReadRows, rowsRead)
			atomic.AddInt64(&a.Task.InsertedRows, rowsInserted)
			atomic.AddInt64(&a.Task.DeletedRows, rowsDeleted)

			atomic.AddInt64(&successCount, 1)

			progress = successCount * 100 / int64(len(subTasks))
			a.Logger.Info("总进度", "subTaskCount", len(subTasks), "successCount", successCount, "progress", fmt.Sprintf("%d%%", progress))

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		switch {
		case errors.Is(err, ErrOutOfTimeWindow):
			a.Task.ExecPhase = model.ExecPaused
		case errors.Is(err, context.Canceled):
			a.Task.ExecPhase = model.ExecStopped
		default:
			a.Task.ExecPhase = model.ExecFailed
		}
		return err
	}

	a.Task.ExecPhase = model.ExecCompleted
	return nil
}
