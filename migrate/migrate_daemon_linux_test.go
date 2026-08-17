//go:build linux

package migrate

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/rceman/go-sqlite-store/client"
	"github.com/rceman/go-sqlite-store/daemon"
	"github.com/rceman/go-sqlite-store/store"
)

func TestApplyThroughUnixSocketClient(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "daemon-migrate.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sock := filepath.Join(t.TempDir(), "store.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.NewHandler(s)}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}()

	c := client.OpenUnix(sock)
	defer c.CloseIdleConnections()
	migrations := []Migration{
		{Version: 1, Name: "create_items", Statements: []store.Statement{{SQL: `CREATE TABLE items(id INTEGER PRIMARY KEY, payload BLOB)`}}},
		{Version: 2, Name: "seed_item", Statements: []store.Statement{{SQL: `INSERT INTO items(id,payload) VALUES(?,?)`, Args: []any{int64(9223372036854775807), []byte{1, 2, 3}}}}},
	}
	ctx := context.Background()
	if err := Apply(ctx, c, migrations, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, c, migrations, Options{}); err != nil {
		t.Fatal(err)
	}
	out, err := c.Query(ctx, `SELECT id,payload FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Rows) != 1 || out.Rows[0][0] != int64(9223372036854775807) {
		t.Fatalf("integer did not survive daemon migration/query: %#v", out.Rows)
	}
	blob, ok := out.Rows[0][1].([]byte)
	if !ok || len(blob) != 3 || blob[0] != 1 || blob[1] != 2 || blob[2] != 3 {
		t.Fatalf("blob did not survive daemon migration/query: %#v", out.Rows[0][1])
	}
}
