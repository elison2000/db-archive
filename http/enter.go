package http

import (
	"db-archive/executor"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"os"
)

func StartService(db *gorm.DB, port int, executor *executor.Executor) {

	f, err := os.OpenFile("http.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	// 设置 Gin 的日志输出
	gin.DefaultWriter = f
	gin.DefaultErrorWriter = f

	r := gin.Default()

	// 解决跨域 (可选，但在前后端调试期间很有用)
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// 静态资源托管：将 ./web 目录下的文件映射到 /static 路径 (如果你的HTML引用了css/js文件)
	r.Static("/db-archive/static", "./web/static")

	r.GET("/db-archive", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	r.GET("/db-archive/sub-task/:id", func(c *gin.Context) {
		c.File("./web/sub_task.html")
	})

	//r.GET("/db-archive/archive-tasks", func(c *gin.Context) {
	//	c.File("./web/archive_tasks.html")
	//})

	// 注册 API
	r.POST("/db-archive/api/ping", TestConnectionHandler)
	r.GET("/db-archive/api/cancel-archive-task/:id", CancelArchiveTask(executor))
	r.GET("/db-archive/api/terminate-archive-task/:id", TerminateArchiveTask(executor))
	r.POST("/db-archive/api/test-archive-config", TestArchiveConfig(db))

	{
		g := r.Group("/db-archive/api/data-sources")
		g.GET("/", ListDataSource(db))
		g.GET("/:id", GetDataSource(db))
		g.POST("", CreateDataSource(db))
		g.PUT("/:id", UpdateDataSource(db))
		g.DELETE("/:id", DeleteDataSource(db))
	}

	{
		g := r.Group("/db-archive/api/archive-jobs")
		g.GET("/", ListArchiveJob(db))
		g.GET("/:id", GetArchiveJob(db))
		g.POST("/", CreateArchiveJob(db))
		g.PUT("/:id", UpdateArchiveJob(db))
		g.DELETE("/:id", DeleteArchiveJob(db))
	}

	{
		g := r.Group("/db-archive/api/archive-tasks")
		g.GET("/", ListArchiveTask(db))
		g.GET("/:id", GetArchiveTask(db))
		g.POST("/", CreateArchiveTask(db))
		g.PUT("/:id", UpdateArchiveTask(db))
		g.DELETE("/:id", DeleteArchiveTask(db))
		g.POST("/:id", ResumeArchiveTask(db)) //重新加入队列
	}

	{
		g := r.Group("/db-archive/api/archive-sub-tasks")
		g.GET("/", ListArchiveSubTask(db))
		g.GET("/:id", GetArchiveSubTask(db))
	}

	r.Run(fmt.Sprintf(":%d", port))
}
