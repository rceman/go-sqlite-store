package store

import (
	"errors"
	"fmt"

	"github.com/rceman/go-sqlite-store/internal/sqlite3c"
)

type ExecResult = sqlite3c.ExecResult
type QueryResult = sqlite3c.QueryResult

var ErrRowsAffectedMismatch = errors.New("sqlite statement rows affected did not match requirement")

type RowsAffectedMismatchError struct {
	Statement int
	Required  int64
	Actual    int64
}

func (e *RowsAffectedMismatchError) Error() string {
	return fmt.Sprintf("%v: statement=%d required=%d actual=%d", ErrRowsAffectedMismatch, e.Statement, e.Required, e.Actual)
}

func (e *RowsAffectedMismatchError) Unwrap() error { return ErrRowsAffectedMismatch }

type Statement struct {
	SQL                 string `json:"sql"`
	Args                []any  `json:"args,omitempty"`
	RequireRowsAffected int64  `json:"require_rows_affected,omitempty"`
}
