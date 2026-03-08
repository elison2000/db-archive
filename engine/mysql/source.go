package mysql

import (
	"context"
	"database/sql"
	"db-archive/config"
	"db-archive/model"
	"db-archive/util"
	"fmt"
	"golang.org/x/time/rate"
	"strings"
)

const quote = "`"

type MySQLSource struct {
	Opt         *model.ArchiveTask
	DBName      string
	TbName      string
	fullTbName  string
	DB          *sql.DB
	Where       string
	SplitColumn string
	SplitSize   int
	BatchSize   int
	Columns     []string
	ColumnTypes []string
}

func NewMySQLSource(task *model.ArchiveTask) (*MySQLSource, error) {
	ds := task.SourceDataSource
	db, err := util.NewMysqlDB(&config.DBConfig{Host: ds.Host, Port: ds.Port, User: ds.User, Password: ds.Password, Database: task.SourceDB})
	if err != nil {
		return nil, err
	}

	return &MySQLSource{
		Opt:         task,
		DBName:      task.SourceDB,
		TbName:      task.SourceTable,
		fullTbName:  fmt.Sprintf("%s.%s", util.EncloseStr(task.SourceDB, quote), util.EncloseStr(task.SourceTable, quote)),
		DB:          db,
		Where:       task.ArchiveCondition,
		SplitColumn: task.SplitColumn,
		SplitSize:   task.SplitSize,
		BatchSize:   task.BatchSize,
	}, nil
}

func (s *MySQLSource) GetDBName() string {
	return s.DBName
}

func (s *MySQLSource) GetTableName() string {
	return s.TbName
}

func (s *MySQLSource) GetColumns() []string {
	return s.Columns
}

func (s *MySQLSource) Init() error {
	err := s.getColumns()
	if err != nil {
		return err
	}

	return nil
}

func (s *MySQLSource) GetColumnTypes() (map[string]string, error) {
	query := fmt.Sprintf("desc %s", s.fullTbName)
	cur, err := s.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	cols, err := cur.Columns()
	if err != nil {
		return nil, err
	}

	var args []any
	for i := 0; i < len(cols); i++ {
		args = append(args, new(sql.RawBytes))
	}

	colMap := make(map[string]string)
	for cur.Next() {
		err := cur.Scan(args...)
		if err != nil {
			return nil, err
		}

		_name := string(*args[0].(*sql.RawBytes))
		_type := string(*args[1].(*sql.RawBytes))
		colMap[_name] = _type

	}

	return colMap, nil
}

func (s *MySQLSource) GetSplitValues() (vals []string, err error) {
	var colType string
	for i, col := range s.Columns {
		if col == s.SplitColumn {
			colType = s.ColumnTypes[i]
			break
		}
	}

	if colType == "" {
		return nil, fmt.Errorf("split column %s not found", s.SplitColumn)
	}

	scanner, err := s.buildSQLValueScanner(colType)
	if err != nil {
		return nil, fmt.Errorf("GetSplitValues-> %w", err)
	}

	query := fmt.Sprintf(`SELECT split_col FROM (
    SELECT split_col, (@rn := @rn + 1) rn FROM (SELECT %s AS split_col FROM %s WHERE %s AND %s IS NOT NULL ORDER BY %s) x, (SELECT @rn := 0) r
) t WHERE MOD(rn, %d) = 1`, s.SplitColumn, s.fullTbName, s.Where, s.SplitColumn, s.SplitColumn, s.SplitSize)

	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetSplitValues:Query-> %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		val, err := scanner(rows)
		if err != nil {
			return nil, fmt.Errorf("GetSplitValues:Scan-> %w", err)
		}

		// 去重逻辑
		if len(vals) == 0 || val != vals[len(vals)-1] {
			vals = append(vals, val)
		}

	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetSplitValues:rows.Err-> %w", err)
	}

	return vals, nil
}

func (s *MySQLSource) GetCount(where string) (cnt int64, err error) {
	query := fmt.Sprintf(`SELECT count(*) cnt FROM %s WHERE %s`, s.fullTbName, where)
	cnt, err = util.QueryCount(s.DB, query)
	if err != nil {
		return cnt, fmt.Errorf("GetCount-> %w", err)
	}
	return
}

func (s *MySQLSource) FetchBatch(ctx context.Context, ch chan<- []any, where string) (cnt int64, err error) {
	query := s.getSelectSQL(where)
	rows, err := s.DB.Query(query)
	if err != nil {
		return cnt, fmt.Errorf("FetchBatch:Query-> %w", err)
	}
	defer rows.Close()

	scanArgs := make([]interface{}, len(s.Columns))
	for i, columnType := range s.ColumnTypes {
		switch columnType {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "UNSIGNED INT", "UNSIGNED INTEGER", "UNSIGNED BIGINT":
			scanArgs[i] = new(util.NullUint64)
		case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
			scanArgs[i] = new(sql.NullFloat64)
		case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
			scanArgs[i] = new(sql.NullString)
		case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM":
			scanArgs[i] = new(sql.NullString)
		case "SET", "BIT", "JSON":
			scanArgs[i] = new(sql.RawBytes)
		default:
			return cnt, fmt.Errorf("unsupported column type: %s", columnType)
		}
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return cnt, fmt.Errorf("FetchBatch:Scan-> %w", err)
		}

		row := make([]any, len(s.Columns))
		for i, v := range scanArgs {
			switch v := v.(type) {
			case *int:
				row[i] = *v
			case *string:
				row[i] = *v
			case *sql.NullString:
				if v.Valid {
					row[i] = v.String
				} else {
					row[i] = nil
				}
			case *bool:
				row[i] = *v
			case *sql.NullInt64:
				if v.Valid {
					row[i] = v.Int64
				} else {
					row[i] = nil
				}
			case *util.NullUint64:
				if v.Valid {
					row[i] = v.Uint64
				} else {
					row[i] = nil
				}
			case *sql.NullFloat64:
				if v.Valid {
					row[i] = v.Float64
				} else {
					row[i] = nil
				}
			case *sql.NullTime:
				if v.Valid {
					row[i] = v.Time
				} else {
					row[i] = nil
				}
			case *sql.NullBool:
				if v.Valid {
					row[i] = v.Bool
				} else {
					row[i] = nil
				}
			case *float64:
				row[i] = *v
			case *sql.RawBytes:
				row[i] = string(*v)
			}
		}

		select {
		case <-ctx.Done():
			return cnt, fmt.Errorf("user terminate")
		case ch <- row:
			cnt++
		}

	}

	if err = rows.Err(); err != nil {
		return cnt, fmt.Errorf("FetchBatch:rows.Err-> %w", err)
	}

	return cnt, nil
}

func (s *MySQLSource) DeleteBatch(ctx context.Context, limit *rate.Limiter, where string) (cnt int64, err error) {

	var rowsAffected int64
	var result sql.Result
	query := s.getDeleteSQL(where)

	for {
		if ctx.Err() != nil {
			return cnt, fmt.Errorf("user terminate")
		}

		err = limit.WaitN(context.Background(), s.BatchSize)
		if err != nil {
			return cnt, fmt.Errorf("DeleteBatch:WaitN-> %w", err)
		}

		result, err = s.DB.Exec(query)
		if err != nil {
			return cnt, fmt.Errorf("DeleteBatch:Exec-> %w", err)
		}

		rowsAffected, err = result.RowsAffected()
		if err != nil {
			return cnt, fmt.Errorf("DeleteBatch:RowsAffected-> %w", err)
		}

		cnt += rowsAffected
		if rowsAffected < int64(s.BatchSize) {
			break
		}

	}

	return cnt, nil

}

func (s *MySQLSource) Close() {
	if s.DB == nil {
		return
	}
	s.DB.Close()

}

func (s *MySQLSource) getColumns() error {
	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", s.fullTbName)

	rows, err := s.DB.Query(query)
	if err != nil {
		return fmt.Errorf("getColumns:Query-> %w", err)
	}

	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("getColumns:ColumnTypes-> %w", err)
	}

	for _, ct := range columnTypes {
		s.Columns = append(s.Columns, ct.Name())
		s.ColumnTypes = append(s.ColumnTypes, ct.DatabaseTypeName())
	}

	return nil
}

func (s *MySQLSource) getSelectSQL(where string) string {
	columnsText := util.EncloseAndJoin(s.Columns, quote)
	return fmt.Sprintf(`SELECT %s FROM %s WHERE %s`, columnsText, s.fullTbName, where)
}

func (s *MySQLSource) getDeleteSQL(where string) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE %s LIMIT %d`, s.fullTbName, where, s.BatchSize)
}

//func (s *MySQLSource) FormatValueFunc(columnType string) func(raw *sql.RawBytes) string {
//	switch columnType {
//	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
//		return func(raw *sql.RawBytes) (str string) { return string(*raw) }
//	case "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "UNSIGNED INT", "UNSIGNED INTEGER", "UNSIGNED BIGINT":
//		return func(raw *sql.RawBytes) (str string) { return string(*raw) }
//	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
//		return func(raw *sql.RawBytes) (str string) { return string(*raw) }
//	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
//		return func(raw *sql.RawBytes) (str string) { return "'" + string(*raw) + "'" }
//	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM":
//		return func(raw *sql.RawBytes) (str string) { return "'" + string(*raw) + "'" }
//	case "SET", "BIT", "JSON":
//		return func(raw *sql.RawBytes) (str string) { return "'" + string(*raw) + "'" }
//	default:
//		return func(raw *sql.RawBytes) (str string) { return "'" + string(*raw) + "'" }
//	}
//}

func (s *MySQLSource) buildSQLValueScanner(colType string) (func(*sql.Rows) (string, error), error) {
	switch colType {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
		return func(r *sql.Rows) (string, error) {
			var v *int64 // 使用指针处理 NULL
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			// 数字类型不需要引号
			return fmt.Sprint(*v), nil
		}, nil

	case "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "UNSIGNED INT", "UNSIGNED INTEGER", "UNSIGNED BIGINT":
		return func(r *sql.Rows) (string, error) {
			var v *uint64 // 使用指针处理 NULL
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			// 数字类型不需要引号
			return fmt.Sprint(*v), nil
		}, nil

	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
		return func(r *sql.Rows) (string, error) {
			var v *float64 // 使用指针处理 NULL
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			// 数字类型不需要引号
			return fmt.Sprint(*v), nil
		}, nil

	case "CHAR", "VARCHAR", "TINYTEXT", "ENUM":
		return func(r *sql.Rows) (string, error) {
			var v *string
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			// 字符串处理转义单引号
			safeStr := strings.ReplaceAll(*v, "'", "''")
			return fmt.Sprintf("'%s'", safeStr), nil
		}, nil

	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return func(r *sql.Rows) (string, error) {
			var v *string
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			return fmt.Sprintf("'%s'", *v), nil
		}, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}

}
