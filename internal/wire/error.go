package wire

import (
	"errors"

	"github.com/rceman/go-sqlite-store/store"
)

type ErrorCode string

const (
	ErrorCodeRowsAffectedMismatch ErrorCode = "rows_affected_mismatch"
	ErrorCodeReadOnlyRequired     ErrorCode = "read_only_required"
	ErrorCodeStatementNotAllowed  ErrorCode = "statement_not_allowed"
	ErrorCodeMultipleStatements   ErrorCode = "multiple_statements"
	ErrorCodeClosed               ErrorCode = "closed"
)

type ErrorResponse struct {
	Error string    `json:"error"`
	Code  ErrorCode `json:"code,omitempty"`
}

func CodeForError(err error) ErrorCode {
	switch {
	case errors.Is(err, store.ErrRowsAffectedMismatch):
		return ErrorCodeRowsAffectedMismatch
	case errors.Is(err, store.ErrReadOnlyRequired):
		return ErrorCodeReadOnlyRequired
	case errors.Is(err, store.ErrStatementNotAllowed):
		return ErrorCodeStatementNotAllowed
	case errors.Is(err, store.ErrMultipleStatements):
		return ErrorCodeMultipleStatements
	case errors.Is(err, store.ErrClosed):
		return ErrorCodeClosed
	default:
		return ""
	}
}

func SentinelForCode(code ErrorCode) error {
	switch code {
	case ErrorCodeRowsAffectedMismatch:
		return store.ErrRowsAffectedMismatch
	case ErrorCodeReadOnlyRequired:
		return store.ErrReadOnlyRequired
	case ErrorCodeStatementNotAllowed:
		return store.ErrStatementNotAllowed
	case ErrorCodeMultipleStatements:
		return store.ErrMultipleStatements
	case ErrorCodeClosed:
		return store.ErrClosed
	default:
		return nil
	}
}
