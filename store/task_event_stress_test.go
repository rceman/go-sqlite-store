package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestTaskEventAtomicityUnderConcurrentLoad models the GPT Tunnel durability
// boundary: an authoritative task revision change and its append-only event must
// commit atomically while readers continue querying the same database.
func TestTaskEventAtomicityUnderConcurrentLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task-event.db")
	cfg := Config{
		Path:        path,
		Readers:     2,
		BatchSize:   8,
		BatchWindow: 250 * time.Microsecond,
		Synchronous: "FULL",
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE tasks (
		id TEXT PRIMARY KEY,
		revision INTEGER NOT NULL,
		status TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `CREATE TABLE events (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		revision INTEGER NOT NULL,
		kind TEXT NOT NULL,
		FOREIGN KEY(task_id) REFERENCES tasks(id)
	)`); err != nil {
		t.Fatal(err)
	}
	const tasks = 8
	const revisionsPerTask = 100
	for i := 0; i < tasks; i++ {
		id := fmt.Sprintf("TSK-%02d", i)
		if _, err := s.Exec(ctx, `INSERT INTO tasks(id, revision, status) VALUES(?, 0, 'active')`, id); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, tasks+4)
	for i := 0; i < tasks; i++ {
		taskID := fmt.Sprintf("TSK-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for revision := 1; revision <= revisionsPerTask; revision++ {
				eventID := fmt.Sprintf("EVT-%s-%03d", taskID, revision)
				results, err := s.Batch(ctx, []Statement{
					{
						SQL:                 `UPDATE tasks SET revision=? WHERE id=? AND revision=?`,
						Args:                []any{revision, taskID, revision - 1},
						RequireRowsAffected: 1,
					},
					{
						SQL:                 `INSERT INTO events(id, task_id, revision, kind) VALUES(?, ?, ?, 'task.revised')`,
						Args:                []any{eventID, taskID, revision},
						RequireRowsAffected: 1,
					},
				})
				if err != nil {
					errCh <- fmt.Errorf("%s revision %d: %w", taskID, revision, err)
					return
				}
				if len(results) != 2 || results[0].RowsAffected != 1 || results[1].RowsAffected != 1 {
					errCh <- fmt.Errorf("%s revision %d unexpected results: %#v", taskID, revision, results)
					return
				}
			}
		}()
	}

	// Concurrent readers intentionally use aggregate/query patterns rather than
	// sleeping, exercising WAL readers throughout the write burst.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 250; j++ {
				if _, err := s.Query(ctx, `SELECT COUNT(*), COALESCE(MAX(revision),0) FROM events`); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	assertTaskEventInvariant(t, s, tasks, revisionsPerTask)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertTaskEventInvariant(t, reopened, tasks, revisionsPerTask)
}

func assertTaskEventInvariant(t *testing.T, s *Store, tasks, revisionsPerTask int) {
	t.Helper()
	out, err := s.Query(context.Background(), `SELECT t.id, t.revision, COUNT(e.id), COALESCE(MAX(e.revision),0)
		FROM tasks t LEFT JOIN events e ON e.task_id=t.id
		GROUP BY t.id, t.revision ORDER BY t.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Rows) != tasks {
		t.Fatalf("task rows=%d want=%d", len(out.Rows), tasks)
	}
	for _, row := range out.Rows {
		if len(row) != 4 {
			t.Fatalf("unexpected row: %#v", row)
		}
		revision, ok := row[1].(int64)
		if !ok {
			t.Fatalf("revision type=%T", row[1])
		}
		count, ok := row[2].(int64)
		if !ok {
			t.Fatalf("count type=%T", row[2])
		}
		maxRevision, ok := row[3].(int64)
		if !ok {
			t.Fatalf("max revision type=%T", row[3])
		}
		if revision != int64(revisionsPerTask) || count != revision || maxRevision != revision {
			t.Fatalf("task=%v revision=%d events=%d max_event_revision=%d", row[0], revision, count, maxRevision)
		}
	}
}
