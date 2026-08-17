package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/rceman/go-sqlite-store/internal/wire"
	"github.com/rceman/go-sqlite-store/store"
)

func TestHandlerExecAndQuery(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "daemon.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := NewHandler(s)

	call := func(path string, body any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b)).WithContext(context.Background())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("/v1/exec", wire.SQLRequest{SQL: "CREATE TABLE kv(k TEXT PRIMARY KEY,v INTEGER,b BLOB)"}); w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	args, err := wire.EncodeArgs([]any{"a", int64(7), []byte{0, 255}})
	if err != nil {
		t.Fatal(err)
	}
	if w := call("/v1/exec", wire.SQLRequest{SQL: "INSERT INTO kv(k,v,b) VALUES(?,?,?)", Args: args}); w.Code != 200 {
		t.Fatalf("insert: %d %s", w.Code, w.Body.String())
	}
	queryArgs, _ := wire.EncodeArgs([]any{"a"})
	w := call("/v1/query", wire.SQLRequest{SQL: "SELECT v,b FROM kv WHERE k=?", Args: queryArgs})
	if w.Code != 200 {
		t.Fatalf("query: %d %s", w.Code, w.Body.String())
	}
	var encoded wire.QueryResult
	if err := json.Unmarshal(w.Body.Bytes(), &encoded); err != nil {
		t.Fatal(err)
	}
	got, err := wire.DecodeQueryResult(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 || got.Rows[0][0] != int64(7) {
		t.Fatalf("integer type/value lost: %#v", got.Rows)
	}
	blob, ok := got.Rows[0][1].([]byte)
	if !ok || len(blob) != 2 || blob[0] != 0 || blob[1] != 255 {
		t.Fatalf("blob type/value lost: %#v", got.Rows[0][1])
	}
}

func TestHandlerEnforcesManagedSQLBoundary(t *testing.T) {
	s, err := store.Open(store.Config{Path: filepath.Join(t.TempDir(), "boundary.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := NewHandler(s)

	call := func(path string, body any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("/v1/exec", wire.SQLRequest{SQL: "CREATE TABLE kv(k TEXT PRIMARY KEY)"}); w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/query", wire.SQLRequest{SQL: "INSERT INTO kv(k) VALUES('x') RETURNING k"}); w.Code != http.StatusBadRequest {
		t.Fatalf("mutating query: got %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/exec", wire.SQLRequest{SQL: "BEGIN"}); w.Code != http.StatusBadRequest {
		t.Fatalf("transaction control: got %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/exec", wire.SQLRequest{SQL: "CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER)"}); w.Code != http.StatusBadRequest {
		t.Fatalf("multiple statements: got %d %s", w.Code, w.Body.String())
	}
}
