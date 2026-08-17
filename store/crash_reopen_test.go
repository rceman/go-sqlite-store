//go:build linux

package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const crashHelperEnv = "GO_SQLITE_STORE_CRASH_HELPER"
const crashPathEnv = "GO_SQLITE_STORE_CRASH_PATH"

func TestCommittedWriteSurvivesAbruptProcessExit(t *testing.T) {
	if os.Getenv(crashHelperEnv) == "1" {
		path := os.Getenv(crashPathEnv)
		s, err := Open(Config{Path: path, Synchronous: "FULL"})
		if err != nil {
			os.Exit(20)
		}
		ctx := context.Background()
		if _, err := s.Exec(ctx, `CREATE TABLE durable(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
			os.Exit(21)
		}
		if _, err := s.Exec(ctx, `INSERT INTO durable(id,value) VALUES(?,?)`, int64(1), "committed"); err != nil {
			os.Exit(22)
		}
		// Deliberately skip Store.Close and all defers. The parent verifies WAL recovery.
		os.Exit(0)
	}

	path := filepath.Join(t.TempDir(), "crash.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommittedWriteSurvivesAbruptProcessExit$")
	cmd.Env = append(os.Environ(), crashHelperEnv+"=1", crashPathEnv+"="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper failed: %v\n%s", err, out)
	}

	s, err := Open(Config{Path: path, Synchronous: "FULL"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	out, err := s.Query(context.Background(), `SELECT value FROM durable WHERE id=?`, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Rows) != 1 || len(out.Rows[0]) != 1 || out.Rows[0][0] != "committed" {
		t.Fatalf("unexpected durable row: %#v", out.Rows)
	}
}
