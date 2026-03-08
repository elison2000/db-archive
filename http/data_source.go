package http

import (
	"db-archive/config"
	"db-archive/model"
	"db-archive/util"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"strconv"
)

func ListDataSource(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		role := c.Query("role")
		dbType := c.Query("db_type")

		query := db.Model(&model.DataSource{})

		if role != "" {
			query = query.Where("role = ?", role)
		}
		if dbType != "" {
			query = query.Where("db_type = ?", dbType)
		}

		var list []model.DataSource
		if err := query.Order("id desc").Find(&list).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, list)
	}
}

func GetDataSource(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, _ := strconv.ParseUint(c.Param("id"), 10, 64)

		var ds model.DataSource
		if err := db.First(&ds, id).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusOK, gin.H{"data": nil})
				return
			}
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, ds)
	}
}

func CreateDataSource(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.DataSource
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
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

func UpdateDataSource(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {

		id, _ := strconv.ParseInt(c.Param("id"), 10, 64)

		var req model.DataSource
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误"})
			return
		}

		req.ID = id
		if err := db.Save(&req).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"updated": true})
	}
}

func DeleteDataSource(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		if err := db.Delete(&model.DataSource{}, "id = ?", id).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"deleted": true})
	}
}

func TestConnectionHandler(c *gin.Context) {

	var req model.DataSource
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	optMap := make(map[string]string)
	if req.Extra == "" {
		req.Extra = "{}"
	}
	if err := json.Unmarshal([]byte(req.Extra), &optMap); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Extra is not json: " + err.Error()})
		return
	}

	var dbName string
	if req.DBType == "mysql" {
		dbName = optMap["database"]
	} else if req.DBType == "oracle" {
		dbName = optMap["serviceName"]
	} else if req.DBType == "pgsql" {
		dbName = optMap["database"]
	} else if req.DBType == "doris" {
		dbName = optMap["database"]
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "不支持的数据库类型"})
		return
	}

	cfg := &config.DBConfig{
		Host:     req.Host,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
		Database: dbName,
	}

	err := util.PingDB(req.DBType, cfg)
	if err != nil {
		c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接失败: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "连接成功！"})
}
