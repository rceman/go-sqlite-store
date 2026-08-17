package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestRowsAffectedRequirementRollsBackWholeRequest(t *testing.T) {
	s, err := Open(Config{Path: filepath.Join(t.TempDir(), "expected-rows.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Exec(ctx, `CREATE TABLE tasks(id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `CREATE TABLE events(id TEXT PRIMARY KEY, task_id TEXT NOT NULL, revision INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `INSERT INTO tasks(id,revision) VALUES('TSK-1',1)`); err != nil {
		t.Fatal(err)
	}

	_, err = s.Batch(ctx, []Statement{
		{SQL: `UPDATE tasks SET revision=2 WHERE id='TSK-1' AND revision=0`, RequireRowsAffected: 1},
		{SQL: `INSERT INTO events(id,task_id,revision) VALUES('EVT-2','TSK-1',2)`, RequireRowsAffected: 1},
	})
	if !errors.Is(err, ErrRowsAffectedMismatch) {
		t.Fatalf("expected ErrRowsAffectedMismatch, got %v", err)
	}
	out, err := s.Query(ctx, `SELECT revision, (SELECT COUNT(*) FROM events) FROM tasks WHERE id='TSK-1'`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Rows[0][0] != int64(1) || out.Rows[0][1] != int64(0) {
		t.Fatalf("failed request was not rolled back: %#v", out.Rows)
	}

	results, err := s.Batch(ctx, []Statement{
		{SQL: `UPDATE tasks SET revision=2 WHERE id='TSK-1' AND revision=1`, RequireRowsAffected: 1},
		{SQL: `INSERT INTO events(id,task_id,revision) VALUES('EVT-2','TSK-1',2)`, RequireRowsAffected: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].RowsAffected != 1 || results[1].RowsAffected != 1 {
		t.Fatalf("unexpected results: %#v", results)
	}
	out, err = s.Query(ctx, `SELECT revision, (SELECT COUNT(*) FROM events) FROM tasks WHERE id='TSK-1'`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Rows[0][0] != int64(2) || out.Rows[0][1] != int64(1) {
		t.Fatalf("successful request invariant failed: %#v", out.Rows)
	}
}
