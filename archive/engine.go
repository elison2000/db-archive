package archive

import (
	"db-archive/engine/doris"
	"db-archive/engine/mysql"
	"db-archive/engine/oracle"
	"db-archive/model"
	"fmt"
)

func NewSource(t *model.ArchiveTask) (db model.Source, err error) {
	switch t.SourceDataSource.DBType {
	case "mysql":
		db, err = mysql.NewMySQLSource(t)
		if err != nil {
			return nil, err //这里db是typed-nil,不能直接返回，要返回nil, 否则Close()时判断db!=nil永远为真，最终导致panic
		}
		return db, nil
	case "doris":
		db, err = doris.NewDorisSource(t)
		if err != nil {
			return nil, err
		}
		return db, nil
	case "oracle":
		db, err = oracle.NewOracleSource(t)
		if err != nil {
			return nil, err
		}
		return db, nil
	default:
		return nil, fmt.Errorf("Unsupported db type: %s", t.SourceDataSource.DBType)
	}
}

func NewSink(t *model.ArchiveTask) (db model.Sink, err error) {
	switch t.SinkDataSource.DBType {
	case "mysql":
		db, err = mysql.NewMySQLSink(t)
		if err != nil {
			return nil, err
		}
		return db, nil
	case "doris":
		db, err = doris.NewDorisSink(t)
		if err != nil {
			return nil, err
		}
		return db, nil
	case "oracle":
		db, err = oracle.NewOracleSink(t)
		if err != nil {
			return nil, err
		}
		return db, nil
	default:
		return nil, fmt.Errorf("Unsupported db type: %s", t.SinkDataSource.DBType)
	}
}
