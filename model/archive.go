package model

import (
	"context"
	"golang.org/x/time/rate"
)

type Source interface {
	GetDBName() string
	GetTableName() string
	GetColumns() []string
	GetColumnTypes() (colMap map[string]string, err error)
	GetSplitValues() (vals []string, err error)
	GetCount(where string) (int64, error)
	FetchBatch(ctx context.Context, ch chan<- []any, where string) (int64, error)
	DeleteBatch(ctx context.Context, limit *rate.Limiter, where string) (int64, error)
	Init() error
	Close()
}

type Sink interface {
	GetDBName() string
	GetTableName() string
	GetColumnTypes() (colMap map[string]string, err error)
	GetCount(where string) (int64, error)
	WriteBatch(ctx context.Context, limit *rate.Limiter, ch <-chan []any) (int64, error)
	Init(cols []string) error
	Close()
}

type Executor interface {
	Run(ctx context.Context) error
}
