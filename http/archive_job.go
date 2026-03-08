package http

import (
	"db-archive/model"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func ListArchiveJob(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Query("name")
		sourceDB := c.Query("source_db")
		sourceTable := c.Query("source_table")

		query := db.Where("is_deleted = ?", 0).Order("id desc")

		if name != "" {
			query = query.Where("name LIKE ?", "%"+name+"%")
		}
		if sourceDB != "" {
			query = query.Where("source_db LIKE ?", "%"+sourceDB+"%")
		}
		if sourceTable != "" {
			query = query.Where("source_table LIKE ?", "%"+sourceTable+"%")
		}

		var list []model.ArchiveJob
		if err := query.Find(&list).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, list)
	}
}

func GetArchiveJob(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		var job model.ArchiveJob
		if err := db.First(&job, "id = ?", id).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusOK, gin.H{"data": nil})
				return
			}
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, job)
	}
}

func CreateArchiveJob(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ArchiveJob
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
			return
		}

		req.IsEnabled = 1

		if err := db.Create(&req).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"id": req.ID})
	}
}

func UpdateArchiveJob(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ArchiveJob
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

func DeleteArchiveJob(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		if err := db.Model(&model.ArchiveJob{}).Where("id = ?", id).Update("is_deleted", 1).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"deleted": true})
	}
}
