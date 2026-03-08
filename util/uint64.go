package util

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

func (n *NullUint64) Scan(value interface{}) error {
	if value == nil {
		n.Uint64, n.Valid = 0, false
		return nil
	}
	switch v := value.(type) {
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		n.Uint64 = u
	case int64:
		n.Uint64 = uint64(v)
	case []byte:
		u, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}
		n.Uint64 = u
	default:
		return fmt.Errorf("unsupported Scan type for NullUint64: %T", value)
	}
	n.Valid = true
	return nil
}

func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Uint64, nil
}
