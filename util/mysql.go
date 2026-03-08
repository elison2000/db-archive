package util

import (
	"database/sql"
	"db-archive/config"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"os"
	"time"
)

func NewMysqlDB(c *config.DBConfig) (db *sql.DB, err error) {
	//获取数据库连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&loc=Local", c.User, c.Password, c.Host, c.Port, c.Database)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}

	err = db.Ping()
	if err != nil {
		return
	}

	db.SetMaxOpenConns(128)                //最大连接数
	db.SetMaxIdleConns(2)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
	db.SetConnMaxLifetime(time.Minute * 5) //最大存活保持时间
	db.SetConnMaxIdleTime(time.Minute * 5) //最大空闲保持时间
	return db, nil
}
func NewMysqlORM(c *config.DBConfig, file *os.File) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&parseTime=true&loc=Local", c.User, c.Password, c.Host, c.Port, c.Database)
	newLogger := logger.New(
		log.New(file, "[GORM] ", log.LstdFlags|log.Lshortfile), // io.Writer
		logger.Config{
			SlowThreshold:             100 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: false,
			Colorful:                  false,
		})

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: newLogger, NamingStrategy: schema.NamingStrategy{SingularTable: true}})
	if err != nil {
		return nil, err
	}
	// 获取底层 sql.DB 句柄
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(128)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(time.Minute * 5)
	sqlDB.SetConnMaxIdleTime(time.Minute * 5)
	return db, nil
}
