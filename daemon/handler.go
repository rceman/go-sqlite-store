package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/rceman/go-sqlite-store/internal/wire"
	"github.com/rceman/go-sqlite-store/store"
)

type Handler struct{ Store *store.Store }

func NewHandler(s *store.Store) http.Handler {
	h := &Handler{Store: s}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", h.health)
	mux.HandleFunc("GET /v1/stats", h.stats)
	mux.HandleFunc("POST /v1/query", h.query)
	mux.HandleFunc("POST /v1/exec", h.exec)
	mux.HandleFunc("POST /v1/batch", h.batch)
	return mux
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (h *Handler) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.Store.Stats())
}
func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	var req wire.SQLRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("sql is required"))
		return
	}
	args, err := wire.DecodeArgs(req.Args)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	out, err := h.Store.Query(r.Context(), req.SQL, args...)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	encoded, err := wire.EncodeQueryResult(out)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, encoded)
}
func (h *Handler) exec(w http.ResponseWriter, r *http.Request) {
	var req wire.SQLRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("sql is required"))
		return
	}
	args, err := wire.DecodeArgs(req.Args)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	out, err := h.Store.Exec(r.Context(), req.SQL, args...)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (h *Handler) batch(w http.ResponseWriter, r *http.Request) {
	var req wire.BatchRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Statements) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("batch requires at least one statement"))
		return
	}
	stmts := make([]store.Statement, len(req.Statements))
	for i, statement := range req.Statements {
		if strings.TrimSpace(statement.SQL) == "" {
			writeErr(w, http.StatusBadRequest, errors.New("batch statement SQL is required"))
			return
		}
		args, err := wire.DecodeArgs(statement.Args)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		stmts[i] = store.Statement{
			SQL:                 statement.SQL,
			Args:                args,
			RequireRowsAffected: statement.RequireRowsAffected,
		}
	}
	out, err := h.Store.Batch(r.Context(), stmts)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func decodeJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r, 8<<20))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("request must contain exactly one JSON value")
	}
	return nil
}

func writeStoreErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, store.ErrRowsAffectedMismatch) {
		status = http.StatusConflict
	} else if errors.Is(err, store.ErrReadOnlyRequired) ||
		errors.Is(err, store.ErrStatementNotAllowed) ||
		errors.Is(err, store.ErrMultipleStatements) {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, wire.ErrorResponse{Error: err.Error(), Code: wire.CodeForError(err)})
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, wire.ErrorResponse{Error: err.Error()})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
