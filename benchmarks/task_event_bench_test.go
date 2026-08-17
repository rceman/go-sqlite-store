package benchmarks

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rceman/go-sqlite-store/store"
)

func BenchmarkTaskEventEmbedded(b *testing.B) {
	s, err := store.Open(store.Config{
		Path:        filepath.Join(b.TempDir(), "task-event.db"),
		Readers:     2,
		BatchSize:   8,
		BatchWindow: 250 * time.Microsecond,
		Synchronous: "FULL",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE tasks(id TEXT PRIMARY KEY, revision INTEGER NOT NULL, status TEXT NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	if _, err := s.Exec(ctx, `CREATE TABLE events(id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, kind TEXT NOT NULL, payload BLOB, FOREIGN KEY(task_id) REFERENCES tasks(id))`); err != nil {
		b.Fatal(err)
	}
	const taskCount = 8
	for i := 0; i < taskCount; i++ {
		if _, err := s.Exec(ctx, `INSERT INTO tasks(id,revision,status) VALUES(?,0,'active')`, fmt.Sprintf("TSK-%02d", i)); err != nil {
			b.Fatal(err)
		}
	}

	var seq atomic.Uint64
	payload := make([]byte, 512)
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			n++
			id := seq.Add(1)
			taskID := fmt.Sprintf("TSK-%02d", id%taskCount)
			if n%5 == 0 {
				if _, err := s.Batch(ctx, []store.Statement{
					{
						SQL:                 `UPDATE tasks SET revision=revision+1 WHERE id=?`,
						Args:                []any{taskID},
						RequireRowsAffected: 1,
					},
					{
						SQL:                 `INSERT INTO events(id,task_id,kind,payload) VALUES(?,?,'task.revised',?)`,
						Args:                []any{id, taskID, payload},
						RequireRowsAffected: 1,
					},
				}); err != nil {
					b.Error(err)
				}
				continue
			}
			if _, err := s.Query(ctx, `SELECT id,revision,status FROM tasks WHERE id=?`, taskID); err != nil {
				b.Error(err)
			}
		}
	})
}
