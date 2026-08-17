package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/rceman/go-sqlite-store/internal/wire"
	"github.com/rceman/go-sqlite-store/store"
)

type Client struct {
	http *http.Client
}

func OpenUnix(socketPath string) *Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
		DisableCompression:  true,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{http: &http.Client{Transport: tr}}
}

func (c *Client) Health(ctx context.Context) error {
	var out map[string]string
	if err := c.do(ctx, http.MethodGet, "/v1/health", nil, &out); err != nil {
		return err
	}
	if out["status"] != "ok" {
		return fmt.Errorf("unexpected health status %q", out["status"])
	}
	return nil
}

func (c *Client) Stats(ctx context.Context) (store.Stats, error) {
	var out store.Stats
	return out, c.do(ctx, http.MethodGet, "/v1/stats", nil, &out)
}

func (c *Client) Query(ctx context.Context, sql string, args ...any) (store.QueryResult, error) {
	encoded, err := wire.EncodeArgs(args)
	if err != nil {
		return store.QueryResult{}, err
	}
	var out wire.QueryResult
	if err := c.do(ctx, http.MethodPost, "/v1/query", wire.SQLRequest{SQL: sql, Args: encoded}, &out); err != nil {
		return store.QueryResult{}, err
	}
	return wire.DecodeQueryResult(out)
}

func (c *Client) Exec(ctx context.Context, sql string, args ...any) (store.ExecResult, error) {
	encoded, err := wire.EncodeArgs(args)
	if err != nil {
		return store.ExecResult{}, err
	}
	var out store.ExecResult
	err = c.do(ctx, http.MethodPost, "/v1/exec", wire.SQLRequest{SQL: sql, Args: encoded}, &out)
	return out, err
}

func (c *Client) Batch(ctx context.Context, stmts []store.Statement) ([]store.ExecResult, error) {
	encoded := wire.BatchRequest{Statements: make([]wire.Statement, len(stmts))}
	for i, st := range stmts {
		args, err := wire.EncodeArgs(st.Args)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		encoded.Statements[i] = wire.Statement{SQL: st.SQL, Args: args, RequireRowsAffected: st.RequireRowsAffected}
	}
	var out []store.ExecResult
	err := c.do(ctx, http.MethodPost, "/v1/batch", encoded, &out)
	return out, err
}

func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://sqlite-store"+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			return fmt.Errorf("sqlite-store: %s", e.Error)
		}
		return fmt.Errorf("sqlite-store: HTTP %s", resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
