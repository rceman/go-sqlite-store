package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

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

type sqlRequest struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

type batchRequest struct {
	Statements []store.Statement `json:"statements"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (h *Handler) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.Store.Stats())
}
func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	var req sqlRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("sql is required"))
		return
	}
	args, err := normalizeArgs(req.Args)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	out, err := h.Store.Query(r.Context(), req.SQL, args...)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (h *Handler) exec(w http.ResponseWriter, r *http.Request) {
	var req sqlRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("sql is required"))
		return
	}
	args, err := normalizeArgs(req.Args)
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
	var req batchRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Statements) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("batch requires at least one statement"))
		return
	}
	for i := range req.Statements {
		if strings.TrimSpace(req.Statements[i].SQL) == "" {
			writeErr(w, http.StatusBadRequest, errors.New("batch statement SQL is required"))
			return
		}
		args, err := normalizeArgs(req.Statements[i].Args)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		req.Statements[i].Args = args
	}
	out, err := h.Store.Batch(r.Context(), req.Statements)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func decodeJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r, 8<<20))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("request must contain exactly one JSON value")
	}
	return nil
}

func normalizeArgs(args []any) ([]any, error) {
	out := make([]any, len(args))
	for i, v := range args {
		switch x := v.(type) {
		case json.Number:
			if n, err := x.Int64(); err == nil {
				out[i] = n
				continue
			}
			f, err := strconv.ParseFloat(string(x), 64)
			if err != nil {
				return nil, err
			}
			out[i] = f
		case nil, bool, string, float64:
			out[i] = x
		default:
			return nil, errors.New("SQL args must be scalar JSON values")
		}
	}
	return out, nil
}

func writeStoreErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, store.ErrReadOnlyRequired) ||
		errors.Is(err, store.ErrStatementNotAllowed) ||
		errors.Is(err, store.ErrMultipleStatements) {
		status = http.StatusBadRequest
	}
	writeErr(w, status, err)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
