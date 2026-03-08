package doris

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
	"io"
	"net/http"
	"strings"
)

type DorisSink struct {
	Opt           *model.ArchiveTask
	DBName        string
	TbName        string
	fullTbName    string
	DB            *sql.DB
	BatchSize     int
	SourceColumns []string
	feHttpURL     string
	user          string
	password      string
}

func NewDorisSink(task *model.ArchiveTask) (*DorisSink, error) {
	ds := task.SinkDataSource
	db, err := util.NewMysqlDB(&config.DBConfig{Host: ds.Host, Port: ds.Port, User: ds.User, Password: ds.Password, Database: task.SinkDB})
	if err != nil {
		return nil, fmt.Errorf("NewDorisSink:NewMysqlDB-> %w", err)
	}

	if ds.Extra == "" {
		return nil, errors.New("NewDorisSink: Extra is empty")
	}

	optMap := make(map[string]string)
	err = json.Unmarshal([]byte(ds.Extra), &optMap)
	if err != nil {
		return nil, fmt.Errorf("NewDorisSink: %w", err)
	}

	feHttpURL := optMap["feHttpURL"]
	if feHttpURL == "" {
		return nil, fmt.Errorf("NewDorisSink: feHttpURL is empty")
	}

	return &DorisSink{
		Opt:        task,
		DBName:     task.SinkDB,
		TbName:     task.SinkTable,
		fullTbName: fmt.Sprintf("%s.%s", util.EncloseStr(task.SinkDB, quote), util.EncloseStr(task.SinkTable, quote)),
		BatchSize:  task.BatchSize,
		DB:         db,
		user:       ds.User,
		password:   ds.Password,
		feHttpURL:  feHttpURL,
	}, nil
}

func (s *DorisSink) Init(sourceColumns []string) error {
	s.SourceColumns = sourceColumns
	if s.SourceColumns == nil || len(s.SourceColumns) == 0 {
		return fmt.Errorf("SourceColumns is empty")
	}
	return nil
}

func (s *DorisSink) GetDBName() string {
	return s.DBName
}

func (s *DorisSink) GetTableName() string {
	return s.TbName
}

func (s *DorisSink) GetColumnTypes() (map[string]string, error) {

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

func (s *DorisSink) GetCount(where string) (cnt int64, err error) {
	query := fmt.Sprintf(`SELECT count(*) cnt FROM %s WHERE %s`, s.fullTbName, where)
	cnt, err = util.QueryCount(s.DB, query)
	if err != nil {
		return cnt, fmt.Errorf("GetCount-> %w", err)
	}
	return
}

type resultT struct {
	TxnID                  int    `json:"TxnId"`
	Label                  string `json:"Label"`
	Comment                string `json:"Comment"`
	TwoPhaseCommit         string `json:"TwoPhaseCommit"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int    `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	BeginTxnTimeMs         int    `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	ReceiveDataTimeMs      int    `json:"ReceiveDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
	ErrorURL               string
}

func (s *DorisSink) streamLoad(data []map[string]interface{}) (n int, err error) {

	url := fmt.Sprintf("%s/api/%s/%s/_stream_load", s.feHttpURL, s.DBName, s.TbName)

	// 一次性编码为 JSON 数组
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return n, fmt.Errorf("json marshal failed: %v", err)
	}

	req, err := http.NewRequest("PUT", url, bytes.NewReader(jsonBytes))
	if err != nil {
		return n, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("format", "json")
	req.Header.Set("strip_outer_array", "true")
	req.Header.Set("columns", strings.Join(s.SourceColumns, ","))
	req.Header.Set("Expect", "100-continue")

	req.SetBasicAuth(s.user, s.password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return n, fmt.Errorf("streamLoad:Do-> %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return n, fmt.Errorf("streamLoad:ReadAll-> %w", err)
	}

	if resp.StatusCode != 200 {
		return n, fmt.Errorf("streamLoad-> %s", string(body))
	}

	var result resultT
	err = json.Unmarshal(body, &result)
	if err != nil {
		return n, fmt.Errorf("streamLoad:Unmarshal-> %w", err)
	}

	if result.Status != "Success" {
		return n, fmt.Errorf("streamLoad error, ErrorURL=%s", result.ErrorURL)
	}

	return result.NumberLoadedRows, nil
}

func (s *DorisSink) WriteBatch(ctx context.Context, limit *rate.Limiter, ch <-chan []any) (cnt int64, err error) {

	for {

		err = limit.WaitN(context.Background(), s.BatchSize)
		if err != nil {
			return cnt, fmt.Errorf("WriteBatch:WaitN-> %w", err)
		}

		rows, ok := util.GetManyOfChan(ctx, ch, s.BatchSize)

		// 构造 []map[string]interface{}
		var data []map[string]interface{}
		for _, row := range rows {
			rowMap := make(map[string]interface{}, len(s.SourceColumns))
			for i, col := range s.SourceColumns {
				rowMap[col] = row[i]
			}
			data = append(data, rowMap)
		}

		n, err := s.streamLoad(data)
		if err != nil {
			return cnt, fmt.Errorf("WriteBatch-> %w", err)
		}

		cnt += int64(n)
		if n != len(rows) {
			return cnt, fmt.Errorf("WriteBatch: 预期行数=%d 实际行数=%d", len(rows), n)
		}

		if !ok {
			break
		}

	}

	return cnt, nil
}

func (s *DorisSink) Close() {
	if s.DB != nil {
		s.DB.Close()
	}
}
