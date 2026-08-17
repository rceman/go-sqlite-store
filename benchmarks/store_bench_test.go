package benchmarks

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rceman/go-sqlite-store/store"
)

func BenchmarkMixedStore(b *testing.B) {
	s, err := store.Open(store.Config{Path: filepath.Join(b.TempDir(), "bench.db"), Readers: 2, BatchSize: 8, BatchWindow: 250 * time.Microsecond})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO kv(id,n) VALUES(1,0)`); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			n++
			if n%5 == 0 {
				if _, err := s.Exec(ctx, `UPDATE kv SET n=n+1 WHERE id=1`); err != nil {
					b.Error(err)
				}
			} else {
				if _, err := s.Query(ctx, `SELECT n FROM kv WHERE id=1`); err != nil {
					b.Error(err)
				}
			}
		}
	})
}
