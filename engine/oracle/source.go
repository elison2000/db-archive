package oracle

import (
	"context"
	"database/sql"
	"db-archive/config"
	"db-archive/model"
	"db-archive/util"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"strings"
	"time"
)

const quote = `"`

type OracleSource struct {
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

func NewOracleSource(task *model.ArchiveTask) (*OracleSource, error) {

	ds := task.SourceDataSource
	if ds.Extra == "" {
		return nil, errors.New("NewOracleSource: Extra is empty")
	}

	optMap := make(map[string]string)
	err := json.Unmarshal([]byte(ds.Extra), &optMap)
	if err != nil {
		return nil, fmt.Errorf("NewOracleSource: %w", err)
	}

	serviceName := optMap["serviceName"]
	if serviceName == "" {
		return nil, fmt.Errorf("NewOracleSource: serviceName is empty")
	}

	db, err := util.NewOracleDB(&config.DBConfig{Host: ds.Host, Port: ds.Port, User: ds.User, Password: ds.Password, Database: serviceName})
	if err != nil {
		return nil, err
	}

	return &OracleSource{
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

func (s *OracleSource) GetDBName() string {
	return s.DBName
}

func (s *OracleSource) GetTableName() string {
	return s.TbName
}

func (s *OracleSource) GetColumns() []string {
	return s.Columns
}

func (s *OracleSource) Init() error {
	err := s.getColumns()
	if err != nil {
		return err
	}

	return nil
}

func (s *OracleSource) GetColumnTypes() (map[string]string, error) {
	query := fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' ORDER BY COLUMN_ID`, s.DBName, s.TbName)

	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("getColumnTypes -> %w", err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var name, dataType string
		var length, precision, scale sql.NullInt64
		if err := rows.Scan(&name, &dataType, &length, &precision, &scale); err != nil {
			return nil, err
		}

		colType := dataType
		if precision.Valid && scale.Valid {
			colType = fmt.Sprintf("%s(%d,%d)", dataType, precision.Int64, scale.Int64)
		} else if length.Valid {
			colType = fmt.Sprintf("%s(%d)", dataType, length.Int64)
		}

		columns[name] = colType
	}

	return columns, nil
}

func (s *OracleSource) GetSplitValues() (vals []string, err error) {
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

	query := fmt.Sprintf(`SELECT split_col FROM ( SELECT %s AS split_col, ROW_NUMBER() OVER (ORDER BY %s) AS rn FROM %s WHERE %s AND %s IS NOT NULL) WHERE MOD(rn, %d) = 1`,
		s.SplitColumn,
		s.SplitColumn,
		s.fullTbName,
		s.Where,
		s.SplitColumn,
		s.SplitSize,
	)

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

func (s *OracleSource) GetCount(where string) (cnt int64, err error) {
	query := fmt.Sprintf(`SELECT count(*) cnt FROM %s WHERE %s`, s.fullTbName, where)
	cnt, err = util.QueryCount(s.DB, query)
	if err != nil {
		return cnt, fmt.Errorf("GetCount-> %w", err)
	}
	return
}

func (s *OracleSource) FetchBatch(ctx context.Context, ch chan<- []any, where string) (cnt int64, err error) {
	query := s.getSelectSQL(where)
	rows, err := s.DB.Query(query)
	if err != nil {
		return cnt, fmt.Errorf("FetchBatch:Query-> %w", err)
	}
	defer rows.Close()

	scanArgs := make([]interface{}, len(s.Columns))
	for i, columnType := range s.ColumnTypes {
		switch columnType {
		case "INTEGER":
			scanArgs[i] = new(sql.NullInt64)
		case "NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
			scanArgs[i] = new(sql.NullFloat64)
		case "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "LongVarChar":
			scanArgs[i] = new(sql.NullString)
		case "DATE", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
			scanArgs[i] = new(sql.NullTime)
		case "IntervalYM_DTY", "IntervalDS_DTY":
			scanArgs[i] = new(sql.NullString)
		//case "CLOB", "NCLOB":
		//	scanArgs[i] = new(sql.NullString)
		//case "RAW", "LONG", "LongRaw", "OCIBlobLocator", "IBDouble":
		//	scanArgs[i] = new(sql.RawBytes)
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

func (s *OracleSource) DeleteBatch(ctx context.Context, limit *rate.Limiter, where string) (cnt int64, err error) {

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

func (s *OracleSource) Close() {
	if s.DB == nil {
		return
	}
	s.DB.Close()

}

func (s *OracleSource) getColumns() error {
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

func (s *OracleSource) getSelectSQL(where string) string {
	columnsText := util.EncloseAndJoin(s.Columns, quote)
	return fmt.Sprintf(`SELECT %s FROM %s WHERE %s`, columnsText, s.fullTbName, where)
}

func (s *OracleSource) getDeleteSQL(where string) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE %s AND rownum<=%d`, s.fullTbName, where, s.BatchSize)
}

func (s *OracleSource) buildSQLValueScanner(colType string) (func(*sql.Rows) (string, error), error) {
	switch colType {
	case "NUMBER", "INTEGER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
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

	case "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "LongVarChar":
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

	case "DATE", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
		return func(r *sql.Rows) (string, error) {
			var v *time.Time
			if err := r.Scan(&v); err != nil {
				return "", err
			}
			if v == nil {
				return "NULL", nil
			}
			// 修正 Oracle 日期语法
			return fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')",
				v.Format("2006-01-02 15:04:05")), nil
		}, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}

}
