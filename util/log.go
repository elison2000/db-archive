package util

import (
	"fmt"
	"log/slog"
	"os"
	"path"
)

func NewLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok {
					file := path.Base(src.File)
					a.Value = slog.StringValue(
						fmt.Sprintf("%s:%d", file, src.Line),
					)
				}
			}
			return a
		},
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	return slog.New(handler)
}

func NewFileLogger(file *os.File) *slog.Logger {
	writer := file
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok {
					file := path.Base(src.File)
					a.Value = slog.StringValue(
						fmt.Sprintf("%s:%d", file, src.Line),
					)
				}
			}
			return a
		},
	}

	handler := slog.NewTextHandler(writer, opts)
	return slog.New(handler)
}
