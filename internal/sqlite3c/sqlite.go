package sqlite3c

/*
#cgo pkg-config: sqlite3
#include <sqlite3.h>
#include <stdlib.h>

static int user_write_authorizer(void *unused, int action, const char *a, const char *b, const char *c, const char *d) {
    (void)unused; (void)a; (void)b; (void)c; (void)d;
    switch (action) {
    case SQLITE_TRANSACTION:
    case SQLITE_SAVEPOINT:
    case SQLITE_ATTACH:
    case SQLITE_DETACH:
    case SQLITE_PRAGMA:
        return SQLITE_DENY;
    default:
        return SQLITE_OK;
    }
}

static int user_read_authorizer(void *unused, int action, const char *a, const char *b, const char *c, const char *d) {
    (void)unused; (void)a; (void)b; (void)c; (void)d;
    switch (action) {
    case SQLITE_TRANSACTION:
    case SQLITE_SAVEPOINT:
    case SQLITE_ATTACH:
    case SQLITE_DETACH:
        return SQLITE_DENY;
    default:
        return SQLITE_OK;
    }
}

static int set_user_write_authorizer(sqlite3 *db) {
    return sqlite3_set_authorizer(db, user_write_authorizer, NULL);
}

static int set_user_read_authorizer(sqlite3 *db) {
    return sqlite3_set_authorizer(db, user_read_authorizer, NULL);
}

static int clear_authorizer(sqlite3 *db) {
    return sqlite3_set_authorizer(db, NULL, NULL);
}

static int bind_text_transient(sqlite3_stmt *stmt, int idx, const char *v, int n) {
    return sqlite3_bind_text(stmt, idx, v, n, SQLITE_TRANSIENT);
}

static int bind_blob_transient(sqlite3_stmt *stmt, int idx, const void *v, int n) {
    return sqlite3_bind_blob(stmt, idx, v, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"
)

var (
	ErrBusy                = errors.New("sqlite busy")
	ErrLocked              = errors.New("sqlite locked")
	ErrReadOnlyRequired    = errors.New("sqlite statement must be read-only")
	ErrStatementNotAllowed = errors.New("sqlite statement is not allowed through the managed store API")
	ErrMultipleStatements  = errors.New("sqlite request must contain exactly one statement")
)

type Config struct {
	Path              string
	Synchronous       string
	BusyTimeout       time.Duration
	CacheKiB          int
	MmapBytes         int64
	WALAutoCheckpoint int
	JournalSizeLimit  int64
	ForeignKeys       bool
}

type Conn struct {
	db *C.sqlite3
}

type ExecResult struct {
	RowsAffected int64 `json:"rows_affected"`
	LastInsertID int64 `json:"last_insert_id"`
}

type QueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func Open(cfg Config) (*Conn, error) {
	if cfg.Path == "" {
		return nil, errors.New("sqlite path is required")
	}
	cpath := C.CString(cfg.Path)
	defer C.free(unsafe.Pointer(cpath))

	var db *C.sqlite3
	flags := C.int(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_NOMUTEX)
	if rc := C.sqlite3_open_v2(cpath, &db, flags, nil); rc != C.SQLITE_OK {
		if db != nil {
			msg := C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close(db)
			return nil, fmt.Errorf("sqlite open: %s", msg)
		}
		return nil, fmt.Errorf("sqlite open rc=%d", int(rc))
	}
	conn := &Conn{db: db}

	timeoutMS := int(cfg.BusyTimeout / time.Millisecond)
	if timeoutMS <= 0 {
		timeoutMS = 5000
	}
	C.sqlite3_busy_timeout(db, C.int(timeoutMS))

	syncMode := strings.ToUpper(strings.TrimSpace(cfg.Synchronous))
	if syncMode == "" {
		syncMode = "FULL"
	}
	switch syncMode {
	case "FULL", "NORMAL", "EXTRA", "OFF":
	default:
		conn.Close()
		return nil, fmt.Errorf("unsupported synchronous mode %q", cfg.Synchronous)
	}
	if cfg.CacheKiB <= 0 {
		cfg.CacheKiB = 8192
	}
	if cfg.MmapBytes <= 0 {
		cfg.MmapBytes = 256 << 20
	}
	if cfg.WALAutoCheckpoint <= 0 {
		cfg.WALAutoCheckpoint = 2000
	}
	if cfg.JournalSizeLimit <= 0 {
		cfg.JournalSizeLimit = 64 << 20
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=" + syncMode,
		"PRAGMA temp_store=MEMORY",
		fmt.Sprintf("PRAGMA cache_size=-%d", cfg.CacheKiB),
		fmt.Sprintf("PRAGMA mmap_size=%d", cfg.MmapBytes),
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", cfg.WALAutoCheckpoint),
		fmt.Sprintf("PRAGMA journal_size_limit=%d", cfg.JournalSizeLimit),
	}
	if cfg.ForeignKeys {
		pragmas = append(pragmas, "PRAGMA foreign_keys=ON")
	}
	for _, pragma := range pragmas {
		if _, err := conn.Exec(pragma); err != nil {
			conn.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return conn, nil
}

func (c *Conn) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	rc := C.sqlite3_close(c.db)
	c.db = nil
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite close rc=%d", int(rc))
	}
	return nil
}

func (c *Conn) Exec(sql string, args ...any) (ExecResult, error) {
	stmt, err := c.prepare(sql)
	if err != nil {
		return ExecResult{}, err
	}
	defer C.sqlite3_finalize(stmt)
	if err := bindAll(stmt, args); err != nil {
		return ExecResult{}, err
	}
	for {
		rc := C.sqlite3_step(stmt)
		if rc == C.SQLITE_ROW {
			continue
		}
		if rc != C.SQLITE_DONE {
			return ExecResult{}, c.errForRC(rc)
		}
		break
	}
	return ExecResult{
		RowsAffected: int64(C.sqlite3_changes(c.db)),
		LastInsertID: int64(C.sqlite3_last_insert_rowid(c.db)),
	}, nil
}

// ExecUser executes one caller-supplied write statement while preserving store-owned
// transaction and connection semantics. Transaction control, ATTACH/DETACH and PRAGMA
// statements are rejected.
func (c *Conn) ExecUser(sql string, args ...any) (ExecResult, error) {
	stmt, err := c.prepareUser(sql, false)
	if err != nil {
		return ExecResult{}, err
	}
	defer C.sqlite3_finalize(stmt)
	if err := bindAll(stmt, args); err != nil {
		return ExecResult{}, err
	}
	for {
		rc := C.sqlite3_step(stmt)
		if rc == C.SQLITE_ROW {
			continue
		}
		if rc != C.SQLITE_DONE {
			return ExecResult{}, c.errForRC(rc)
		}
		break
	}
	return ExecResult{
		RowsAffected: int64(C.sqlite3_changes(c.db)),
		LastInsertID: int64(C.sqlite3_last_insert_rowid(c.db)),
	}, nil
}

// QueryReadOnly executes exactly one caller-supplied statement and rejects anything
// SQLite does not classify as read-only. This prevents reader connections from
// bypassing the single-writer invariant (for example with INSERT ... RETURNING).
func (c *Conn) QueryReadOnly(sql string, args ...any) (QueryResult, error) {
	stmt, err := c.prepareUser(sql, true)
	if err != nil {
		return QueryResult{}, err
	}
	defer C.sqlite3_finalize(stmt)
	if C.sqlite3_stmt_readonly(stmt) == 0 {
		return QueryResult{}, ErrReadOnlyRequired
	}
	return c.queryPrepared(stmt, args)
}

func (c *Conn) Query(sql string, args ...any) (QueryResult, error) {
	stmt, err := c.prepare(sql)
	if err != nil {
		return QueryResult{}, err
	}
	defer C.sqlite3_finalize(stmt)
	return c.queryPrepared(stmt, args)
}

func (c *Conn) queryPrepared(stmt *C.sqlite3_stmt, args []any) (QueryResult, error) {
	if err := bindAll(stmt, args); err != nil {
		return QueryResult{}, err
	}

	ncol := int(C.sqlite3_column_count(stmt))
	out := QueryResult{Columns: make([]string, ncol)}
	for i := 0; i < ncol; i++ {
		out.Columns[i] = C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
	}
	for {
		rc := C.sqlite3_step(stmt)
		switch rc {
		case C.SQLITE_DONE:
			return out, nil
		case C.SQLITE_ROW:
			row := make([]any, ncol)
			for i := 0; i < ncol; i++ {
				row[i] = columnValue(stmt, i)
			}
			out.Rows = append(out.Rows, row)
		default:
			return QueryResult{}, c.errForRC(rc)
		}
	}
}

func (c *Conn) BeginImmediate() error { _, err := c.Exec("BEGIN IMMEDIATE"); return err }
func (c *Conn) Commit() error         { _, err := c.Exec("COMMIT"); return err }
func (c *Conn) Rollback() error       { _, err := c.Exec("ROLLBACK"); return err }

func (c *Conn) prepare(sql string) (*C.sqlite3_stmt, error) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var stmt *C.sqlite3_stmt
	var tail *C.char
	rc := C.sqlite3_prepare_v2(c.db, csql, -1, &stmt, &tail)
	if rc != C.SQLITE_OK {
		return nil, c.errForRC(rc)
	}
	if stmt == nil {
		return nil, errors.New("sqlite statement is empty")
	}

	// Ask SQLite to parse the tail too. Whitespace/comments produce no statement;
	// any second real statement is rejected to keep one request = one statement.
	if tail != nil && *tail != 0 {
		var extra *C.sqlite3_stmt
		rc = C.sqlite3_prepare_v2(c.db, tail, -1, &extra, nil)
		if rc != C.SQLITE_OK {
			C.sqlite3_finalize(stmt)
			return nil, c.errForRC(rc)
		}
		if extra != nil {
			C.sqlite3_finalize(extra)
			C.sqlite3_finalize(stmt)
			return nil, ErrMultipleStatements
		}
	}
	return stmt, nil
}

func (c *Conn) prepareUser(sql string, readOnly bool) (*C.sqlite3_stmt, error) {
	var rc C.int
	if readOnly {
		rc = C.set_user_read_authorizer(c.db)
	} else {
		rc = C.set_user_write_authorizer(c.db)
	}
	if rc != C.SQLITE_OK {
		return nil, c.errForRC(rc)
	}
	stmt, err := c.prepare(sql)
	clearRC := C.clear_authorizer(c.db)
	if err != nil {
		if strings.Contains(err.Error(), "not authorized") {
			return nil, fmt.Errorf("%w: %v", ErrStatementNotAllowed, err)
		}
		return nil, err
	}
	if clearRC != C.SQLITE_OK {
		C.sqlite3_finalize(stmt)
		return nil, c.errForRC(clearRC)
	}
	return stmt, nil
}

func (c *Conn) errForRC(rc C.int) error {
	msg := C.GoString(C.sqlite3_errmsg(c.db))
	switch rc {
	case C.SQLITE_BUSY:
		return fmt.Errorf("%w: %s", ErrBusy, msg)
	case C.SQLITE_LOCKED:
		return fmt.Errorf("%w: %s", ErrLocked, msg)
	default:
		return fmt.Errorf("sqlite rc=%d: %s", int(rc), msg)
	}
}

func bindAll(stmt *C.sqlite3_stmt, args []any) error {
	want := int(C.sqlite3_bind_parameter_count(stmt))
	if want != len(args) {
		return fmt.Errorf("sqlite bind count: query wants %d args, got %d", want, len(args))
	}
	for i, arg := range args {
		if err := bindOne(stmt, i+1, arg); err != nil {
			return fmt.Errorf("bind arg %d: %w", i+1, err)
		}
	}
	return nil
}

func bindOne(stmt *C.sqlite3_stmt, idx int, v any) error {
	var rc C.int
	switch x := v.(type) {
	case nil:
		rc = C.sqlite3_bind_null(stmt, C.int(idx))
	case bool:
		n := 0
		if x {
			n = 1
		}
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(n))
	case int:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case int8:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case int16:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case int32:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case int64:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case uint:
		if uint64(x) > math.MaxInt64 {
			return errors.New("uint overflows sqlite INTEGER")
		}
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case uint8:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case uint16:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case uint32:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case uint64:
		if x > math.MaxInt64 {
			return errors.New("uint64 overflows sqlite INTEGER")
		}
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x))
	case float32:
		rc = C.sqlite3_bind_double(stmt, C.int(idx), C.double(x))
	case float64:
		rc = C.sqlite3_bind_double(stmt, C.int(idx), C.double(x))
	case string:
		p := C.CString(x)
		defer C.free(unsafe.Pointer(p))
		rc = C.bind_text_transient(stmt, C.int(idx), p, C.int(len(x)))
	case []byte:
		if len(x) == 0 {
			rc = C.bind_blob_transient(stmt, C.int(idx), nil, 0)
		} else {
			p := C.CBytes(x)
			defer C.free(p)
			rc = C.bind_blob_transient(stmt, C.int(idx), p, C.int(len(x)))
		}
	case time.Time:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(x.UnixNano()))
	default:
		return fmt.Errorf("unsupported sqlite bind type %T", v)
	}
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite bind rc=%d", int(rc))
	}
	return nil
}

func columnValue(stmt *C.sqlite3_stmt, idx int) any {
	i := C.int(idx)
	switch C.sqlite3_column_type(stmt, i) {
	case C.SQLITE_INTEGER:
		return int64(C.sqlite3_column_int64(stmt, i))
	case C.SQLITE_FLOAT:
		return float64(C.sqlite3_column_double(stmt, i))
	case C.SQLITE_TEXT:
		p := C.sqlite3_column_text(stmt, i)
		n := C.sqlite3_column_bytes(stmt, i)
		return C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
	case C.SQLITE_BLOB:
		p := C.sqlite3_column_blob(stmt, i)
		n := C.sqlite3_column_bytes(stmt, i)
		if p == nil || n == 0 {
			return []byte{}
		}
		return C.GoBytes(p, n)
	case C.SQLITE_NULL:
		return nil
	default:
		return nil
	}
}
