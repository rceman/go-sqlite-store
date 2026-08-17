package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

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
	if w := call("/v1/exec", map[string]any{"sql": "CREATE TABLE kv(k TEXT PRIMARY KEY,v INTEGER)"}); w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/exec", map[string]any{"sql": "INSERT INTO kv(k,v) VALUES(?,?)", "args": []any{"a", 7}}); w.Code != 200 {
		t.Fatalf("insert: %d %s", w.Code, w.Body.String())
	}
	w := call("/v1/query", map[string]any{"sql": "SELECT v FROM kv WHERE k=?", "args": []any{"a"}})
	if w.Code != 200 {
		t.Fatalf("query: %d %s", w.Code, w.Body.String())
	}
	var got store.QueryResult
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	// JSON turns int64 into float64 when decoding into interface{}.
	if len(got.Rows) != 1 || got.Rows[0][0] != float64(7) {
		t.Fatalf("got %#v", got.Rows)
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
	if w := call("/v1/exec", map[string]any{"sql": "CREATE TABLE kv(k TEXT PRIMARY KEY)"}); w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/query", map[string]any{"sql": "INSERT INTO kv(k) VALUES('x') RETURNING k"}); w.Code != http.StatusBadRequest {
		t.Fatalf("mutating query: got %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/exec", map[string]any{"sql": "BEGIN"}); w.Code != http.StatusBadRequest {
		t.Fatalf("transaction control: got %d %s", w.Code, w.Body.String())
	}
	if w := call("/v1/exec", map[string]any{"sql": "CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER)"}); w.Code != http.StatusBadRequest {
		t.Fatalf("multiple statements: got %d %s", w.Code, w.Body.String())
	}
}
