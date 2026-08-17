//go:build linux

package benchmarks

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rceman/go-sqlite-store/client"
	"github.com/rceman/go-sqlite-store/daemon"
	"github.com/rceman/go-sqlite-store/store"
)

type benchAPI interface {
	Query(context.Context, string, ...any) (store.QueryResult, error)
	Exec(context.Context, string, ...any) (store.ExecResult, error)
}

func BenchmarkMixedEmbedded(b *testing.B) {
	s := openBenchStore(b)
	defer s.Close()
	runMixed(b, s)
}

func BenchmarkMixedUnixSocket(b *testing.B) {
	s := openBenchStore(b)
	sock := filepath.Join(b.TempDir(), "store.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		_ = s.Close()
		b.Fatal(err)
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		_ = ln.Close()
		_ = s.Close()
		b.Fatal(err)
	}
	srv := &http.Server{
		Handler:           daemon.NewHandler(s),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	c := client.OpenUnix(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := c.Health(ctx); err != nil {
		cancel()
		_ = srv.Close()
		_ = s.Close()
		b.Fatal(err)
	}
	cancel()

	runMixed(b, c)
	b.StopTimer()
	c.CloseIdleConnections()
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv.Shutdown(ctx)
	cancel()
	select {
	case <-errCh:
	default:
	}
	_ = s.Close()
}

func openBenchStore(b *testing.B) *store.Store {
	b.Helper()
	s, err := store.Open(store.Config{
		Path:        filepath.Join(b.TempDir(), "bench.db"),
		Readers:     2,
		BatchSize:   8,
		BatchWindow: 250 * time.Microsecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)`); err != nil {
		_ = s.Close()
		b.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO kv(id,n) VALUES(1,0)`); err != nil {
		_ = s.Close()
		b.Fatal(err)
	}
	return s
}

func runMixed(b *testing.B, api benchAPI) {
	b.Helper()
	b.ReportAllocs()
	b.SetParallelism(4)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			n++
			if n%5 == 0 {
				if _, err := api.Exec(ctx, `UPDATE kv SET n=n+1 WHERE id=1`); err != nil {
					b.Error(err)
				}
				continue
			}
			if _, err := api.Query(ctx, `SELECT n FROM kv WHERE id=1`); err != nil {
				b.Error(err)
			}
		}
	})
}
