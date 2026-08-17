package migrate

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rceman/go-sqlite-store/store"
)

func TestApplyIdempotent(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "migrate.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	migrations := []Migration{
		{Version: 1, Name: "create_items", Statements: []store.Statement{{SQL: `CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`}}},
		{Version: 2, Name: "seed_item", Statements: []store.Statement{{SQL: `INSERT INTO items(id,name) VALUES(?,?)`, Args: []any{int64(1), "one"}}}},
	}
	ctx := context.Background()
	if err := Apply(ctx, s, migrations, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, s, migrations, Options{}); err != nil {
		t.Fatal(err)
	}
	out, err := s.Query(ctx, `SELECT COUNT(*) FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Rows[0][0]; got != int64(1) {
		t.Fatalf("count=%v", got)
	}
}

func TestApplyRollsBackFailedMigration(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "rollback.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	err = Apply(ctx, s, []Migration{{
		Version: 1,
		Name:    "broken",
		Statements: []store.Statement{
			{SQL: `CREATE TABLE should_not_exist(id INTEGER PRIMARY KEY)`},
			{SQL: `INSERT INTO missing_table(id) VALUES(1)`},
		},
	}}, Options{})
	if err == nil {
		t.Fatal("expected migration failure")
	}
	out, err := s.Query(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='should_not_exist'`)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Rows[0][0]; got != int64(0) {
		t.Fatalf("failed migration leaked schema change: %v", got)
	}
}

func TestApplyRejectsVersionNameMismatch(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "mismatch.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	one := []Migration{{Version: 1, Name: "one", Statements: []store.Statement{{SQL: `CREATE TABLE x(id INTEGER)`}}}}
	if err := Apply(ctx, s, one, Options{}); err != nil {
		t.Fatal(err)
	}
	two := []Migration{{Version: 1, Name: "renamed", Statements: []store.Statement{{SQL: `CREATE TABLE x(id INTEGER)`}}}}
	if err := Apply(ctx, s, two, Options{}); err == nil {
		t.Fatal("expected version/name mismatch")
	}
}
