package executor

import (
	"context"
	"db-archive/archive"
	"log/slog"
	"sync"
	"time"
)

type Executor struct {
	logger  *slog.Logger
	taskCh  chan *archive.Archiver
	workers int
	wg      *sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	running sync.Map // key: task ID (int64), value: *archive.Archiver
}

func NewExecutor(workerNum, queueSize int, logger *slog.Logger) *Executor {
	if workerNum <= 0 {
		panic("workerNum must > 0")
	}
	if queueSize <= 0 {
		panic("queueSize must > 0")
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		taskCh:  make(chan *archive.Archiver, queueSize),
		workers: workerNum,
		logger:  logger,
		wg:      &wg,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (e *Executor) runWorker(num int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("停止Worker", "worker", num)
			return
		case task, ok := <-e.taskCh:
			if !ok {
				e.logger.Info("Worker退出", "worker", num)
				return
			}
			if e.ctx.Err() != nil {
				e.logger.Info("Worker退出", "worker", num)
				return
			}

			e.logger.Info("开始任务", "taskID", task.Task.ID)
			e.running.Store(task.Task.ID, task)
			func() {
				defer func() {
					if r := recover(); r != nil {
						e.logger.Error("任务崩溃", "taskID", task.Task.ID, "panic", r)
					}
				}()
				task.Run()
			}()
			e.running.Delete(task.Task.ID)
			e.logger.Info("结束任务", "taskID", task.Task.ID)

		}
	}
}

func (e *Executor) Start() {
	e.logger.Info("启动执行器", "concurrency", e.workers)
	for i := 1; i <= e.workers; i++ {
		e.wg.Add(1)
		go e.runWorker(i)
	}
}

func (e *Executor) Stop() {
	e.logger.Info("停止执行器", "concurrency", e.workers)
	e.cancel()

	e.running.Range(func(key, value any) bool {
		e.logger.Info("给正在运行的任务发送停止信号", "taskID", key)
		value.(*archive.Archiver).Cancel()
		return true
	})
}

func (e *Executor) Close() {
	defer e.logger.Info("关闭任务队列")
	close(e.taskCh) //关闭任务队列
}

func (e *Executor) Remaining() int {
	return cap(e.taskCh) - len(e.taskCh)
}

func (e *Executor) Wait() {
	e.wg.Wait()
}

func (e *Executor) Submit(ctx context.Context, t *archive.Archiver) {
	select {
	case <-ctx.Done():
		return
	case e.taskCh <- t:
		return
	}
}

func (e *Executor) SubmitWithTimeout(t *archive.Archiver, d time.Duration) bool {
	select {
	case e.taskCh <- t:
		return true
	case <-time.After(d):
		e.logger.Error("提交任务超时，已丢弃", "taskID", t.Task.ID, "timeout", d)
		return false
	}
}

func (e *Executor) CancelTask(taskID int64) bool {
	e.logger.Info("发送取消信号", "taskID", taskID)
	if v, ok := e.running.Load(taskID); ok {
		v.(*archive.Archiver).Cancel()
		return true
	}
	return false
}

func (e *Executor) TerminateTask(taskID int64) bool {
	e.logger.Info("发送终止信号", "taskID", taskID)
	if v, ok := e.running.Load(taskID); ok {
		v.(*archive.Archiver).Terminate()
		return true
	}
	return false
}
