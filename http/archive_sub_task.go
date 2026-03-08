package http

import (
	"db-archive/model"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func ListArchiveSubTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskID := c.Query("task_id")
		startDate := c.Query("start_date") // 格式: 2026-02-01
		endDate := c.Query("end_date")     // 格式: 2026-02-04

		query := db.Model(&model.ArchiveSubTask{})

		if taskID != "" {
			query = query.Where("task_id = ?", taskID)
		}

		if startDate != "" {
			query = query.Where("exec_start >= ?", startDate+" 00:00:00")
		}
		if endDate != "" {
			query = query.Where("exec_start <= ?", endDate+" 23:59:59")
		}

		var list []model.ArchiveSubTask
		if err := query.Order("id").Find(&list).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, list)
	}
}

func GetArchiveSubTask(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		var st model.ArchiveSubTask
		if err := db.First(&st, "id = ?", id).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusOK, gin.H{"data": nil})
				return
			}
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, st)
	}
}
