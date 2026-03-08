package util

import (
	"context"
	"database/sql"
	"db-archive/config"
	"fmt"
	"strings"
	"time"
)

func PingDB(dbType string, cfg *config.DBConfig) (err error) {
	var db *sql.DB
	switch dbType {
	case "mysql", "polar", "tdsqlc", "doris":
		db, err = NewMysqlDB(cfg)
	case "oracle":
		db, err = NewOracleDB(cfg)
	//case "pgsql":
	//	db, err = NewPgsqlDB(cfg)
	//case "oceanbase":
	//	db, err = NewOceanbaseDB(cfg)
	default:
		err = fmt.Errorf("不支持该数据库类型: %s", dbType)
		return
	}

	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}
	return
}

func EncloseStr(str string, quote string) string {
	buf := strings.Builder{}
	buf.Grow(len(str) + 2*len(quote))
	buf.WriteString(quote)
	buf.WriteString(str)
	buf.WriteString(quote)
	return buf.String()
}

func EncloseAndJoin(list []string, quote string) string {
	buf := strings.Builder{}
	for i, _ := range list {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(quote)
		buf.WriteString(list[i])
		buf.WriteString(quote)
	}
	return buf.String()
}
func QueryCount(db *sql.DB, sqlText string) (count int64, err error) {
	err = db.QueryRow(sqlText).Scan(&count)
	return
}

func QueryReturnList(db *sql.DB, sqlText string) (rows [][]string, err error) {
	//执行sql，返回二维数组
	var cur *sql.Rows
	cur, err = db.Query(sqlText)
	if err != nil {
		return
	}
	defer cur.Close()

	cols, err := cur.Columns()
	if err != nil {
		return
	}

	values := make([]sql.RawBytes, len(cols))
	valuesP := make([]interface{}, len(cols))
	for i := range values {
		valuesP[i] = &values[i]
	}

	for cur.Next() {
		err = cur.Scan(valuesP...)
		if err != nil {
			return
		}
		row := make([]string, len(cols)) //不能在循环外层定义，否则是浅拷贝
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = string(v)
			}
		}

		rows = append(rows, row)
	}
	return
}

func QueryReturnListWithNil(db *sql.DB, sqlText string) (rows [][]any, err error) {

	cur, err := db.Query(sqlText)
	if err != nil {
		return
	}
	defer cur.Close()

	cols, err := cur.Columns()
	if err != nil {
		return
	}

	values := make([]sql.RawBytes, len(cols))
	valuesP := make([]interface{}, len(cols))
	for i := range values {
		valuesP[i] = &values[i]
	}

	for cur.Next() {
		err = cur.Scan(valuesP...)
		if err != nil {
			return
		}

		row := make([]any, len(cols))
		for i, v := range values {
			if v == nil {
				row[i] = nil
			} else {
				row[i] = string(v)
			}
		}

		rows = append(rows, row)

	}
	return
}

func QueryReturnDict(db *sql.DB, sqlText string) ([]map[string]string, error) {
	//执行sql，返回二维map
	cur, err := db.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	cols, err := cur.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]*sql.RawBytes, len(cols))
	valuesP := make([]interface{}, len(cols))
	for i := range values {
		valuesP[i] = &values[i]
	}

	data := []map[string]string{}
	for cur.Next() {
		err := cur.Scan(valuesP...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string, len(cols)) //在循环内创建内存，才是深拷贝模式
		for i, v := range values {
			if v == nil {
				row[cols[i]] = "NULL"
			} else {
				row[cols[i]] = string(*v)
			}

		}

		data = append(data, row)
	}
	return data, nil
}
