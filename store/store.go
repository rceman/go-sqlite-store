package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rceman/go-sqlite-store/internal/sqlite3c"
)

var (
	ErrClosed              = errors.New("sqlite store is closed")
	ErrAlreadyOpen         = errors.New("sqlite store database already has an owner")
	ErrUnsupportedPlatform = errors.New("sqlite store platform is unsupported")
	ErrReadOnlyRequired    = sqlite3c.ErrReadOnlyRequired
	ErrStatementNotAllowed = sqlite3c.ErrStatementNotAllowed
	ErrMultipleStatements  = sqlite3c.ErrMultipleStatements
)

type Store struct {
	cfg Config

	readQ  chan readReq
	writeQ chan *writeReq
	done   chan struct{}
	owner  *ownerLock

	admitMu   sync.RWMutex
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
	stats     counters
}

type readReq struct {
	ctx  context.Context
	sql  string
	args []any
	res  chan readResp
}

type readResp struct {
	result QueryResult
	err    error
}

type writeReq struct {
	ctx   context.Context
	stmts []Statement
	res   chan writeResp
}

type writeResp struct {
	results []ExecResult
	err     error
}

func Open(cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	if cfg.Path == "" {
		return nil, errors.New("store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, err
	}

	owner, err := acquireOwnerLock(cfg.Path)
	if err != nil {
		return nil, err
	}

	// Open the writer synchronously so Open fails fast on bad paths/pragmas.
	writer, err := sqlite3c.Open(toSQLiteConfig(cfg))
	if err != nil {
		_ = owner.close()
		return nil, err
	}

	s := &Store{
		cfg:    cfg,
		readQ:  make(chan readReq, cfg.Readers*4),
		writeQ: make(chan *writeReq, cfg.WriteQueueDepth),
		done:   make(chan struct{}),
		owner:  owner,
	}

	s.wg.Add(1)
	go s.writerLoop(writer)

	openedReaders := make([]*sqlite3c.Conn, 0, cfg.Readers)
	for i := 0; i < cfg.Readers; i++ {
		conn, err := sqlite3c.Open(toSQLiteConfig(cfg))
		if err != nil {
			_ = s.Close()
			for _, c := range openedReaders {
				_ = c.Close()
			}
			return nil, fmt.Errorf("open reader %d: %w", i, err)
		}
		openedReaders = append(openedReaders, conn)
	}
	for _, conn := range openedReaders {
		s.wg.Add(1)
		go s.readerLoop(conn)
	}
	return s, nil
}

func (s *Store) Config() Config { return s.cfg }

func (s *Store) Query(ctx context.Context, sql string, args ...any) (QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req := readReq{ctx: ctx, sql: sql, args: append([]any(nil), args...), res: make(chan readResp, 1)}
	s.admitMu.RLock()
	select {
	case <-s.done:
		s.admitMu.RUnlock()
		return QueryResult{}, ErrClosed
	default:
	}
	select {
	case <-ctx.Done():
		s.admitMu.RUnlock()
		return QueryResult{}, ctx.Err()
	case s.readQ <- req:
		s.admitMu.RUnlock()
	}
	select {
	case <-ctx.Done():
		return QueryResult{}, ctx.Err()
	case out := <-req.res:
		return out.result, out.err
	}
}

func (s *Store) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	results, err := s.Batch(ctx, []Statement{{SQL: sql, Args: args}})
	if err != nil {
		return ExecResult{}, err
	}
	return results[0], nil
}

func (s *Store) Batch(ctx context.Context, stmts []Statement) ([]ExecResult, error) {
	if len(stmts) == 0 {
		return nil, errors.New("batch requires at least one statement")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	copyStmts := make([]Statement, len(stmts))
	for i, st := range stmts {
		if st.SQL == "" {
			return nil, fmt.Errorf("statement %d has empty SQL", i)
		}
		if st.RequireRowsAffected < 0 {
			return nil, fmt.Errorf("statement %d require_rows_affected must not be negative", i)
		}
		copyStmts[i] = Statement{
			SQL: st.SQL, Args: append([]any(nil), st.Args...),
			RequireRowsAffected: st.RequireRowsAffected,
		}
	}
	req := &writeReq{ctx: ctx, stmts: copyStmts, res: make(chan writeResp, 1)}
	s.admitMu.RLock()
	select {
	case <-s.done:
		s.admitMu.RUnlock()
		return nil, ErrClosed
	default:
	}
	select {
	case <-ctx.Done():
		s.admitMu.RUnlock()
		return nil, ctx.Err()
	case s.writeQ <- req:
		s.admitMu.RUnlock()
	}
	select {
	case <-ctx.Done():
		// The writer may still commit this request if it already started. This mirrors
		// normal database cancellation semantics: callers must use idempotency where needed.
		return nil, ctx.Err()
	case out := <-req.res:
		return out.results, out.err
	}
}

func (s *Store) Stats() Stats {
	writes := s.stats.writes.Load()
	txns := s.stats.txns.Load()
	avg := 0.0
	if txns > 0 {
		avg = float64(writes) / float64(txns)
	}
	return Stats{
		Reads:         s.stats.reads.Load(),
		WriteRequests: writes,
		Transactions:  txns,
		FailedReads:   s.stats.failedReads.Load(),
		FailedWrites:  s.stats.failedWrites.Load(),
		QueueDepth:    len(s.writeQ),
		AvgBatchSize:  avg,
	}
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		// Serialize against admission so every request accepted before done closes is
		// visible in a queue before workers switch into drain mode.
		s.admitMu.Lock()
		close(s.done)
		s.admitMu.Unlock()
		s.wg.Wait()
		if s.owner != nil {
			s.closeErr = s.owner.close()
		}
	})
	return s.closeErr
}

func (s *Store) readerLoop(conn *sqlite3c.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	closing := false
	for {
		var req readReq
		if closing {
			select {
			case req = <-s.readQ:
			default:
				return
			}
		} else {
			select {
			case <-s.done:
				closing = true
				continue
			case req = <-s.readQ:
			}
		}
		s.executeRead(conn, req)
	}
}

func (s *Store) executeRead(conn *sqlite3c.Conn, req readReq) {
	if err := req.ctx.Err(); err != nil {
		req.res <- readResp{err: err}
		return
	}
	result, err := conn.QueryReadOnly(req.sql, req.args...)
	if err != nil {
		s.stats.failedReads.Add(1)
	} else {
		s.stats.reads.Add(1)
	}
	req.res <- readResp{result: result, err: err}
}

func (s *Store) writerLoop(conn *sqlite3c.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	closing := false
	for {
		var first *writeReq
		if closing {
			select {
			case first = <-s.writeQ:
			default:
				return
			}
		} else {
			select {
			case <-s.done:
				closing = true
				continue
			case first = <-s.writeQ:
			}
		}

		batch := []*writeReq{first}
		if closing {
			s.drainWriteBatch(&batch)
		} else if s.cfg.BatchWindow > 0 && len(batch) < s.cfg.BatchSize {
			timer.Reset(s.cfg.BatchWindow)
		collect:
			for len(batch) < s.cfg.BatchSize {
				select {
				case req := <-s.writeQ:
					batch = append(batch, req)
				case <-timer.C:
					break collect
				case <-s.done:
					closing = true
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if closing {
				s.drainWriteBatch(&batch)
			}
		}
		s.executeBatch(conn, batch)
	}
}

func (s *Store) drainWriteBatch(batch *[]*writeReq) {
	for len(*batch) < s.cfg.BatchSize {
		select {
		case req := <-s.writeQ:
			*batch = append(*batch, req)
		default:
			return
		}
	}
}

func (s *Store) executeBatch(conn *sqlite3c.Conn, batch []*writeReq) {
	active := batch[:0]
	for _, req := range batch {
		if req.ctx.Err() != nil {
			req.res <- writeResp{err: req.ctx.Err()}
			continue
		}
		active = append(active, req)
	}
	if len(active) == 0 {
		return
	}

	if err := conn.BeginImmediate(); err != nil {
		s.failBatch(active, err)
		return
	}

	type pending struct {
		req     *writeReq
		results []ExecResult
		err     error
	}
	outcomes := make([]pending, 0, len(active))
	for i, req := range active {
		name := fmt.Sprintf("r%d", i)
		if _, err := conn.Exec("SAVEPOINT " + name); err != nil {
			_ = conn.Rollback()
			s.failBatch(active, fmt.Errorf("create request savepoint: %w", err))
			return
		}
		results := make([]ExecResult, 0, len(req.stmts))
		var reqErr error
		for statementIndex, st := range req.stmts {
			r, err := conn.ExecUser(st.SQL, st.Args...)
			if err != nil {
				reqErr = err
				break
			}
			if st.RequireRowsAffected > 0 && r.RowsAffected != st.RequireRowsAffected {
				reqErr = &RowsAffectedMismatchError{
					Statement: statementIndex,
					Required:  st.RequireRowsAffected,
					Actual:    r.RowsAffected,
				}
				break
			}
			results = append(results, r)
		}
		if reqErr != nil {
			if _, err := conn.Exec("ROLLBACK TO " + name); err != nil {
				_ = conn.Rollback()
				s.failBatch(active, fmt.Errorf("rollback request savepoint: %w", err))
				return
			}
			if _, err := conn.Exec("RELEASE " + name); err != nil {
				_ = conn.Rollback()
				s.failBatch(active, fmt.Errorf("release failed request savepoint: %w", err))
				return
			}
			outcomes = append(outcomes, pending{req: req, err: reqErr})
			continue
		}
		if _, err := conn.Exec("RELEASE " + name); err != nil {
			_ = conn.Rollback()
			s.failBatch(active, fmt.Errorf("release request savepoint: %w", err))
			return
		}
		outcomes = append(outcomes, pending{req: req, results: results})
	}

	if err := conn.Commit(); err != nil {
		_ = conn.Rollback()
		s.failBatch(active, err)
		return
	}

	s.stats.txns.Add(1)
	for _, out := range outcomes {
		if out.err != nil {
			s.stats.failedWrites.Add(1)
			out.req.res <- writeResp{err: out.err}
			continue
		}
		s.stats.writes.Add(1)
		out.req.res <- writeResp{results: out.results}
	}
}

func (s *Store) failBatch(reqs []*writeReq, err error) {
	s.stats.failedWrites.Add(uint64(len(reqs)))
	for _, req := range reqs {
		req.res <- writeResp{err: err}
	}
}

func toSQLiteConfig(cfg Config) sqlite3c.Config {
	return sqlite3c.Config{
		Path:              cfg.Path,
		Synchronous:       cfg.Synchronous,
		BusyTimeout:       cfg.BusyTimeout,
		CacheKiB:          cfg.CacheKiB,
		MmapBytes:         cfg.MmapBytes,
		WALAutoCheckpoint: cfg.WALAutoCheckpoint,
		JournalSizeLimit:  cfg.JournalSizeLimit,
		ForeignKeys:       !cfg.DisableForeignKeys,
	}
}
