package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"db-archive/config"
	"db-archive/model"
	"db-archive/util"
	"fmt"
	"golang.org/x/time/rate"
)

type MySQLSink struct {
	Opt        *model.ArchiveTask
	DBName     string
	TbName     string
	fullTbName string
	DB         *sql.DB
	BatchSize  int
	//Columns       []string
	//ColumnTypes   []string
	SourceColumns []string
	InsertSQL     string //insert必须由SourceColumns生成，防止源表和目标表的字段顺序和个数不一致
}

func NewMySQLSink(task *model.ArchiveTask) (*MySQLSink, error) {
	ds := task.SinkDataSource
	db, err := util.NewMysqlDB(&config.DBConfig{Host: ds.Host, Port: ds.Port, User: ds.User, Password: ds.Password, Database: task.SinkDB})
	if err != nil {
		return nil, err
	}

	return &MySQLSink{
		Opt:        task,
		DBName:     task.SinkDB,
		TbName:     task.SinkTable,
		fullTbName: fmt.Sprintf("%s.%s", util.EncloseStr(task.SinkDB, quote), util.EncloseStr(task.SinkTable, quote)),
		BatchSize:  task.BatchSize,
		DB:         db,
	}, nil
}

func (s *MySQLSink) Init(sourceColumns []string) error {
	s.SourceColumns = sourceColumns
	if s.SourceColumns == nil || len(s.SourceColumns) == 0 {
		return fmt.Errorf("SourceColumns is empty")
	}
	s.InsertSQL = s.getInsertSQL(s.BatchSize)
	return nil
}

func (s *MySQLSink) GetDBName() string {
	return s.DBName
}

func (s *MySQLSink) GetTableName() string {
	return s.TbName
}

func (s *MySQLSink) GetColumnTypes() (map[string]string, error) {

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

func (s *MySQLSink) GetCount(where string) (cnt int64, err error) {
	query := fmt.Sprintf(`SELECT count(*) cnt FROM %s WHERE %s`, s.fullTbName, where)
	cnt, err = util.QueryCount(s.DB, query)
	if err != nil {
		return cnt, fmt.Errorf("GetCount-> %w", err)
	}
	return
}

func (s *MySQLSink) WriteBatch(ctx context.Context, limit *rate.Limiter, ch <-chan []any) (cnt int64, err error) {
	var args []any

	for {

		err = limit.WaitN(context.Background(), s.BatchSize)
		if err != nil {
			return cnt, fmt.Errorf("WriteBatch:WaitN-> %w", err)
		}

		rows, ok := util.GetManyOfChan(ctx, ch, s.BatchSize)

		args = args[:0]
		for _, row := range rows {
			for i := range row {
				args = append(args, row[i])
			}
		}

		if len(rows) == s.BatchSize {
			_, err = s.DB.Exec(s.InsertSQL, args...)
			if err != nil {
				return cnt, fmt.Errorf("WriteBatch:Exec-> %w", err)
			}
		} else {
			query := s.getInsertSQL(len(rows))
			_, err = s.DB.Exec(query, args...)
			if err != nil {
				return cnt, fmt.Errorf("WriteBatch:Exec-> %w", err)
			}

		}
		cnt += int64(len(rows))

		if !ok {
			break
		}

	}

	return cnt, nil
}

func (s *MySQLSink) Close() {
	if s.DB == nil {
		return
	}
	s.DB.Close()
}

func (s *MySQLSink) genInsertSQL(batchSize int) string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", s.fullTbName, util.EncloseAndJoin(s.SourceColumns, quote)))

	for line := 0; line < batchSize; line++ {
		if line > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString("(")
		for i := 0; i < len(s.SourceColumns); i++ {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("?")
		}
		buf.WriteString(")")

	}

	return buf.String()
}

func (s *MySQLSink) genReplaceSQL(batchSize int) string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("REPLACE INTO %s (%s) VALUES ", s.fullTbName, util.EncloseAndJoin(s.SourceColumns, quote)))

	for line := 0; line < batchSize; line++ {
		if line > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString("(")
		for i := 0; i < len(s.SourceColumns); i++ {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("?")
		}
		buf.WriteString(")")

	}

	return buf.String()
}

func (s *MySQLSink) getInsertSQL(batchSize int) string {
	if s.Opt.WriteMode == model.WriteInsert {
		return s.genInsertSQL(batchSize)
	} else if s.Opt.WriteMode == model.WriteUpsert {
		return s.genReplaceSQL(batchSize)
	} else {
		return s.genInsertSQL(batchSize)
	}
}

//func (s *MySQLSink) getColumns() error {
//	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", s.fullTbName)
//
//	rows, err := s.DB.Query(query)
//	if err != nil {
//		return fmt.Errorf("getColumns:Query-> %w", err)
//	}
//
//	defer rows.Close()
//
//	columnTypes, err := rows.ColumnTypes()
//	if err != nil {
//		return fmt.Errorf("getColumns:ColumnTypes-> %w", err)
//	}
//
//	for _, ct := range columnTypes {
//		s.Columns = append(s.Columns, ct.Name())
//		s.ColumnTypes = append(s.ColumnTypes, ct.DatabaseTypeName())
//	}
//
//	return nil
//}
