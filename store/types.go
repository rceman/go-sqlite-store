package store

import "github.com/rceman/go-sqlite-store/internal/sqlite3c"

type ExecResult = sqlite3c.ExecResult
type QueryResult = sqlite3c.QueryResult

type Statement struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}
