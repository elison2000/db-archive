package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"db-archive/config"
	"db-archive/model"
	"db-archive/util"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
)

type OracleSink struct {
	Opt           *model.ArchiveTask
	DBName        string
	TbName        string
	fullTbName    string
	DB            *sql.DB
	BatchSize     int
	SourceColumns []string
	Keys          []string
	InsertSQL     string //insert必须由SourceColumns生成，防止源表和目标表的字段顺序和个数不一致
}

func NewOracleSink(task *model.ArchiveTask) (*OracleSink, error) {
	ds := task.SinkDataSource

	if ds.Extra == "" {
		return nil, errors.New("NewOracleSink: Extra is empty")
	}

	optMap := make(map[string]string)
	err := json.Unmarshal([]byte(ds.Extra), &optMap)
	if err != nil {
		return nil, fmt.Errorf("NewOracleSink: %w", err)
	}

	serviceName := optMap["serviceName"]
	if serviceName == "" {
		return nil, fmt.Errorf("NewOracleSink: serviceName is empty")
	}

	db, err := util.NewOracleDB(&config.DBConfig{Host: ds.Host, Port: ds.Port, User: ds.User, Password: ds.Password, Database: serviceName})
	if err != nil {
		return nil, err
	}

	return &OracleSink{
		Opt:        task,
		DBName:     task.SinkDB,
		TbName:     task.SinkTable,
		fullTbName: fmt.Sprintf("%s.%s", util.EncloseStr(task.SinkDB, quote), util.EncloseStr(task.SinkTable, quote)),
		BatchSize:  task.BatchSize,
		DB:         db,
	}, nil
}

func (s *OracleSink) Init(sourceColumns []string) error {
	s.SourceColumns = sourceColumns
	if s.SourceColumns == nil || len(s.SourceColumns) == 0 {
		return fmt.Errorf("SourceColumns is empty")
	}
	err := s.getKeys()
	if err != nil {
		return fmt.Errorf("Init:getKeys-> %w", err)
	}

	if s.Opt.WriteMode == model.WriteUpsert {
		if s.Keys == nil || len(s.Keys) == 0 {
			return fmt.Errorf("write_mode=upsert requires non-empty keys")
		}
	}

	s.InsertSQL = s.getInsertSQL()
	return nil
}

func (s *OracleSink) GetDBName() string {
	return s.DBName
}

func (s *OracleSink) GetTableName() string {
	return s.TbName
}

func (s *OracleSink) GetColumnTypes() (map[string]string, error) {
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

func (s *OracleSink) getKeys() error {

	sql := fmt.Sprintf(`SELECT acc.COLUMN_NAME FROM ALL_CONS_COLUMNS acc JOIN ALL_CONSTRAINTS ac ON acc.OWNER = ac.OWNER AND acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
WHERE ac.CONSTRAINT_TYPE = 'P' AND acc.OWNER = '%s' AND acc.TABLE_NAME='%s' ORDER BY POSITION`, s.DBName, s.TbName)

	rows, err := util.QueryReturnList(s.DB, sql)
	if err != nil {
		return fmt.Errorf("GetKeys-> %w", err)
	}

	for _, row := range rows {
		s.Keys = append(s.Keys, row[0])
	}

	return nil
}

func (s *OracleSink) GetCount(where string) (cnt int64, err error) {
	query := fmt.Sprintf(`SELECT count(*) cnt FROM %s WHERE %s`, s.fullTbName, where)
	cnt, err = util.QueryCount(s.DB, query)
	if err != nil {
		return cnt, fmt.Errorf("GetCount-> %w", err)
	}
	return
}

func (s *OracleSink) WriteBatch(ctx context.Context, limit *rate.Limiter, ch <-chan []any) (cnt int64, err error) {
	insertSQL := s.getInsertSQL()
	for {
		rows, ok := util.GetManyOfChan(ctx, ch, s.BatchSize)
		if len(rows) == 0 {
			break
		}

		err = limit.WaitN(context.Background(), len(rows))
		if err != nil {
			return cnt, err
		}

		// 构造列存储容器 (二维切片)
		// 每个 arr[i] 存储对应列的所有数据
		arr := make([]any, len(s.SourceColumns))
		for i := range s.SourceColumns {
			arr[i] = make([]any, 0, len(rows))
		}

		// 将行数据转置为列数据
		for _, row := range rows {
			for i, val := range row {
				// 将每一行的第 i 列值放入 arr[i] 切片中
				arr[i] = append(arr[i].([]any), val)
			}
		}

		_, err = s.DB.Exec(insertSQL, arr...)
		if err != nil {
			return cnt, fmt.Errorf("WriteBatch:BulkExec-> %w", err)
		}

		cnt += int64(len(rows))
		if !ok {
			break
		}
	}
	return cnt, nil
}

func (s *OracleSink) Close() {
	if s.DB == nil {
		return
	}
	s.DB.Close()
}

func (s *OracleSink) genInsertSQL() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (", s.fullTbName, util.EncloseAndJoin(s.SourceColumns, quote)))
	for i, _ := range s.SourceColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf(`:%d`, i+1))
	}

	buf.WriteString(")")

	return buf.String()
}

func (s *OracleSink) genMergeSQL() string {

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("MERGE INTO %s t USING (SELECT ", s.fullTbName))
	for i, col := range s.SourceColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf(`:%d AS "%s"`, i+1, col))
	}
	buf.WriteString(" FROM dual) s ON (")
	for i, key := range s.Keys {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(fmt.Sprintf(`t.%s = s.%s`, key, key))
	}
	buf.WriteString(")\n WHEN MATCHED THEN UPDATE SET ")

	updateCols := make([]string, 0, len(s.SourceColumns))
	for _, col := range s.SourceColumns {
		if !util.InSlice(col, s.Keys) {
			updateCols = append(updateCols, col)
		}
	}

	for i, col := range updateCols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf(`t.%s = s.%s`, col, col))
	}
	buf.WriteString("\n WHEN NOT MATCHED THEN INSERT (")
	for i, col := range s.SourceColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf(`"%s"`, col))
	}
	buf.WriteString(")\n VALUES (")
	for i, col := range s.SourceColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf(`s."%s"`, col))
	}
	buf.WriteString(")")
	return buf.String()
}

func (s *OracleSink) getInsertSQL() string {
	if s.Opt.WriteMode == model.WriteInsert {
		return s.genInsertSQL()
	} else if s.Opt.WriteMode == model.WriteUpsert {
		return s.genMergeSQL()
	} else {
		return s.genInsertSQL()
	}
}
