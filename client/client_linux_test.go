//go:build linux

package client

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/rceman/go-sqlite-store/daemon"
	"github.com/rceman/go-sqlite-store/store"
)

func TestUnixClientPreservesSQLiteValueTypes(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "types.db")})
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

	c := OpenUnix(sock)
	defer c.CloseIdleConnections()
	ctx := context.Background()
	if _, err := c.Exec(ctx, `CREATE TABLE values_test(i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)`); err != nil {
		t.Fatal(err)
	}
	max := int64(9223372036854775807)
	blob := []byte{0, 1, 254, 255}
	if _, err := c.Exec(ctx, `INSERT INTO values_test(i,r,t,b,n) VALUES(?,?,?,?,?)`, max, 1.25, "hello", blob, nil); err != nil {
		t.Fatal(err)
	}
	out, err := c.Query(ctx, `SELECT i,r,t,b,n FROM values_test`)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Rows) != 1 || len(out.Rows[0]) != 5 {
		t.Fatalf("unexpected rows: %#v", out.Rows)
	}
	if got, ok := out.Rows[0][0].(int64); !ok || got != max {
		t.Fatalf("integer=%#v (%T)", out.Rows[0][0], out.Rows[0][0])
	}
	if got, ok := out.Rows[0][1].(float64); !ok || got != 1.25 {
		t.Fatalf("real=%#v (%T)", out.Rows[0][1], out.Rows[0][1])
	}
	if got, ok := out.Rows[0][2].(string); !ok || got != "hello" {
		t.Fatalf("text=%#v (%T)", out.Rows[0][2], out.Rows[0][2])
	}
	gotBlob, ok := out.Rows[0][3].([]byte)
	if !ok || len(gotBlob) != len(blob) {
		t.Fatalf("blob=%#v (%T)", out.Rows[0][3], out.Rows[0][3])
	}
	for i := range blob {
		if gotBlob[i] != blob[i] {
			t.Fatalf("blob mismatch: %v vs %v", gotBlob, blob)
		}
	}
	if out.Rows[0][4] != nil {
		t.Fatalf("null=%#v (%T)", out.Rows[0][4], out.Rows[0][4])
	}
}
