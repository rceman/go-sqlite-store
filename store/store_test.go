package store

import (
	"context"
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
