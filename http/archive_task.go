package http

import (
	"db-archive/archive"
	"db-archive/executor"
	"db-archive/model"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"strconv"
)

func ListArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Query("job_id")
		startDate := c.Query("start_date") // 格式: 2026-02-01
		endDate := c.Query("end_date")     // 格式: 2026-02-04
		name := c.Query("name")
		sourceDB := c.Query("source_db")
		sourceTable := c.Query("source_table")

		query := db.Model(&model.ArchiveTask{}).Where("is_deleted=0")

		if jobID != "" {
			query = query.Where("job_id = ?", jobID)
		}

		if startDate != "" {
			query = query.Where("created_at >= ?", startDate+" 00:00:00")
		}
		if endDate != "" {
			query = query.Where("created_at <= ?", endDate+" 23:59:59")
		}

		if name != "" {
			query = query.Where("name LIKE ?", "%"+name+"%")
		}
		if sourceDB != "" {
			query = query.Where("source_db LIKE ?", "%"+sourceDB+"%")
		}
		if sourceTable != "" {
			query = query.Where("source_table LIKE ?", "%"+sourceTable+"%")
		}

		var list []model.ArchiveTask
		if err := query.Order("id desc").Find(&list).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, list)
	}
}

func GetArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		var task model.ArchiveTask
		if err := db.First(&task, "id = ?", id).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusOK, gin.H{"data": nil})
				return
			}
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, task)
	}
}

func CreateArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ArchiveTask
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := db.Create(&req).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"id": req.ID})
	}
}

func UpdateArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ArchiveTask
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
			return
		}

		if err := db.Save(&req).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"id": req.ID})
	}
}

func DeleteArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		if err := db.Model(&model.ArchiveTask{}).Where("id = ?", id).Update("is_deleted", 1).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"deleted": true})
	}
}

func CancelArchiveTask(e *executor.Executor) gin.HandlerFunc {
	return func(c *gin.Context) {
		param := c.Param("id")
		taskID, err := strconv.Atoi(param)
		if err != nil {
			c.Error(err)
			c.JSON(http.StatusBadRequest, gin.H{"msg": "无效参数:" + param})
			return
		}
		ok := e.CancelTask(int64(taskID))
		if !ok {
			c.JSON(http.StatusOK, gin.H{"msg": "任务已停止"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"msg": "已发送暂停信号"})
	}
}

func TerminateArchiveTask(e *executor.Executor) gin.HandlerFunc {
	return func(c *gin.Context) {
		param := c.Param("id")
		taskID, err := strconv.Atoi(param)
		if err != nil {
			c.Error(err)
			c.JSON(http.StatusBadRequest, gin.H{"msg": "无效参数:" + param})
			return
		}
		ok := e.TerminateTask(int64(taskID))
		if !ok {
			c.JSON(http.StatusOK, gin.H{"msg": "任务已停止"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"msg": "已发送终止信号"})
	}
}

func ResumeArchiveTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		if err := db.Model(&model.ArchiveTask{}).Where("id = ?", id).Update("exec_phase", model.ExecResuming).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"msg": "任务状态已更新"})
	}
}

func TestArchiveConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var task *model.ArchiveTask
		if err := c.ShouldBindJSON(&task); err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "ShouldBindJSON failed: " + err.Error()})
			return
		}

		var sourceDS, sinkDS model.DataSource

		var err error
		err = db.Model(model.DataSource{}).First(&sourceDS, "id = ?", task.SourceID).Error
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "get source config failed: " + err.Error()})
			return
		}

		err = db.Model(model.DataSource{}).First(&sinkDS, "id = ?", task.SinkID).Error
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "get sink config failed: " + err.Error()})
			return
		}

		task.SourceDataSource = sourceDS
		task.SinkDataSource = sinkDS

		arch := archive.NewArchiver(nil, task)

		arch.Source, err = archive.NewSource(task)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "connect to source failed: " + err.Error()})
			return
		}
		defer arch.Source.Close()

		err = arch.Source.Init()
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "source init failed: " + err.Error()})
			return
		}

		arch.Sink, err = archive.NewSink(task)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "connect to sink failed: " + err.Error()})
			return
		}
		defer arch.Sink.Close()

		err = arch.Sink.Init(arch.Source.GetColumns())
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "预检结果未知", "detail": "sink init failed: " + err.Error()})
			return
		}

		/* code 返回值说明
		-1 未知
		0 一致
		1 字段类型或长度不一致
		2 字段个数不一致
		*/
		code, err := arch.CompareCols()

		var detail string
		if err != nil {
			detail = err.Error()
		}

		switch code {
		case 0:
			c.JSON(http.StatusOK, gin.H{"code": code, "msg": "预检通过", "detail": detail})
			return
		case 1:
			c.JSON(http.StatusOK, gin.H{"code": code, "msg": "预检部分通过", "detail": detail})
			return
		case 2:
			c.JSON(http.StatusOK, gin.H{"code": code, "msg": "预检不通过", "detail": detail})
			return
		default:
			c.JSON(http.StatusOK, gin.H{"code": code, "msg": "预检结果未知", "detail": detail})
			return
		}
	}
}
