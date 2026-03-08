package util

import (
	"database/sql"
	"db-archive/config"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"time"
)

func NewOracleDB(c *config.DBConfig) (db *sql.DB, err error) {
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
	db, err = sql.Open("oracle", dsn)
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
