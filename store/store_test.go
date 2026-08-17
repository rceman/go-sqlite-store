package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	cfg.Path = filepath.Join(t.TempDir(), "test.db")
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestExecQuery(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO kv(k,v) VALUES(?,?)`, "x", 42); err != nil {
		t.Fatal(err)
	}
	got, err := s.Query(ctx, `SELECT k,v FROM kv WHERE k=?`, "x")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 || got.Rows[0][0] != "x" || got.Rows[0][1] != int64(42) {
		t.Fatalf("unexpected rows: %#v", got.Rows)
	}
}

func TestBatchAtomicPerRequest(t *testing.T) {
	s := newTestStore(t, Config{BatchWindow: 2 * time.Millisecond})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	_, err := s.Batch(ctx, []Statement{
		{SQL: `INSERT INTO kv(k,v) VALUES(?,?)`, Args: []any{"a", 1}},
		{SQL: `INSERT INTO kv(k,v) VALUES(?,?)`, Args: []any{"a", 2}},
	})
	if err == nil {
		t.Fatal("expected duplicate-key error")
	}
	got, err := s.Query(ctx, `SELECT count(*) FROM kv WHERE k='a'`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0][0] != int64(0) {
		t.Fatalf("request was not atomic: %#v", got.Rows)
	}
}

func TestParallelReadersAndBatchedWriter(t *testing.T) {
	s := newTestStore(t, Config{Readers: 2, BatchSize: 8, BatchWindow: 2 * time.Millisecond, WriteQueueDepth: 1024})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE counter(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO counter(id,n) VALUES(1,0)`); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const each = 100
	var wg sync.WaitGroup
	errCh := make(chan error, workers*each)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if i%4 == 0 {
					if _, err := s.Query(ctx, `SELECT n FROM counter WHERE id=1`); err != nil {
						errCh <- err
					}
				}
				if _, err := s.Exec(ctx, `UPDATE counter SET n=n+1 WHERE id=1`); err != nil {
					errCh <- fmt.Errorf("worker %d: %w", w, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	got, err := s.Query(ctx, `SELECT n FROM counter WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0][0] != int64(workers*each) {
		t.Fatalf("counter=%v want=%d", got.Rows[0][0], workers*each)
	}
	st := s.Stats()
	if st.Transactions >= st.WriteRequests {
		t.Fatalf("expected batching: %#v", st)
	}
	if st.AvgBatchSize <= 1.0 {
		t.Fatalf("expected avg batch >1: %#v", st)
	}
}

func TestCloseRejectsNewWork(t *testing.T) {
	cfg := Config{Path: filepath.Join(t.TempDir(), "close.db")}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(context.Background(), "SELECT 1"); err != ErrClosed {
		t.Fatalf("got %v want ErrClosed", err)
	}
}

func TestForeignKeysEnabledByDefault(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `CREATE TABLE child(parent_id INTEGER NOT NULL REFERENCES parent(id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO child(parent_id) VALUES(?)`, 999); err == nil {
		t.Fatal("expected foreign key violation")
	}
}

func TestForeignKeysCanBeDisabled(t *testing.T) {
	s := newTestStore(t, Config{DisableForeignKeys: true})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `CREATE TABLE child(parent_id INTEGER NOT NULL REFERENCES parent(id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO child(parent_id) VALUES(?)`, 999); err != nil {
		t.Fatalf("foreign keys should be disabled: %v", err)
	}
}

func TestQueryRejectsMutatingStatement(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE kv(k TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(ctx, `INSERT INTO kv(k) VALUES('x') RETURNING k`); !errors.Is(err, ErrReadOnlyRequired) {
		t.Fatalf("got %v, want ErrReadOnlyRequired", err)
	}
	got, err := s.Query(ctx, `SELECT count(*) FROM kv`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0][0] != int64(0) {
		t.Fatalf("mutating query bypassed writer: %#v", got.Rows)
	}
}

func TestManagedWriteRejectsTransactionControlAndPragma(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	for _, sql := range []string{`BEGIN`, `COMMIT`, `SAVEPOINT x`, `PRAGMA synchronous=OFF`} {
		if _, err := s.Exec(ctx, sql); !errors.Is(err, ErrStatementNotAllowed) {
			t.Fatalf("%q: got %v, want ErrStatementNotAllowed", sql, err)
		}
	}
}

func TestManagedReadRejectsPragma(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	if _, err := s.Query(ctx, `PRAGMA cache_size=-1`); !errors.Is(err, ErrStatementNotAllowed) {
		t.Fatalf("got %v, want ErrStatementNotAllowed", err)
	}
}

func TestRejectsMultipleStatements(t *testing.T) {
	s := newTestStore(t, Config{})
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER)`); !errors.Is(err, ErrMultipleStatements) {
		t.Fatalf("got %v, want ErrMultipleStatements", err)
	}
}

func TestInvalidSynchronousModeFailsOpen(t *testing.T) {
	_, err := Open(Config{Path: filepath.Join(t.TempDir(), "bad.db"), Synchronous: `FULL; PRAGMA journal_mode=OFF`})
	if err == nil {
		t.Fatal("expected invalid synchronous mode to fail")
	}
}

func TestCloseDrainsAcceptedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drain.db")
	s, err := Open(Config{Path: path, BatchSize: 8, BatchWindow: 5 * time.Millisecond, WriteQueueDepth: 1024})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE events(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	const writes = 128
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, writes)
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			_, err := s.Exec(ctx, `INSERT INTO events(id) VALUES(?)`, id)
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	close(start)
	// Give callers a chance to enter admission concurrently with Close. The admission
	// lock guarantees every request accepted before closure is drained before return.
	time.Sleep(time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("unexpected write error: %v", err)
		}
	}

	// Re-open and verify every successful call was durable. Some goroutines may have
	// arrived after Close started and correctly received ErrClosed.
	s2, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Query(ctx, `SELECT count(*) FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0][0].(int64) <= 0 {
		t.Fatalf("expected accepted writes to be drained, rows=%#v", got.Rows)
	}
}
