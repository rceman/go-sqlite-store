package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rceman/go-sqlite-store/daemon"
	"github.com/rceman/go-sqlite-store/store"
)

func main() {
	var cfg store.Config
	var socket string
	flag.StringVar(&cfg.Path, "db", "", "SQLite database path")
	flag.StringVar(&socket, "socket", "", "Unix socket path")
	flag.IntVar(&cfg.Readers, "readers", 2, "read connection workers")
	flag.IntVar(&cfg.BatchSize, "batch-size", 8, "maximum write requests per transaction")
	flag.DurationVar(&cfg.BatchWindow, "batch-window", 250*time.Microsecond, "maximum micro-batch collection window")
	flag.IntVar(&cfg.CacheKiB, "cache-kib", 8192, "SQLite cache budget per connection in KiB")
	flag.IntVar(&cfg.WALAutoCheckpoint, "wal-autocheckpoint", 2000, "WAL autocheckpoint page threshold")
	flag.StringVar(&cfg.Synchronous, "synchronous", "FULL", "SQLite synchronous mode")
	flag.BoolVar(&cfg.DisableForeignKeys, "disable-foreign-keys", false, "disable SQLite foreign key enforcement")
	flag.Parse()

	if cfg.Path == "" || socket == "" {
		flag.Usage()
		os.Exit(2)
	}

	s, err := store.Open(cfg)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ln, err := listenUnix(socket)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer func() { ln.Close(); _ = os.Remove(socket) }()

	srv := &http.Server{Handler: daemon.NewHandler(s), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	log.Printf("sqlite-stored db=%s socket=%s readers=%d batch=%d window=%s", cfg.Path, socket, s.Config().Readers, s.Config().BatchSize, s.Config().BatchWindow)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("signal %s: shutting down", sig)
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func listenUnix(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to remove non-socket path %s", path)
		}
		conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("unix socket %s is already accepting connections", path)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !os.IsNotExist(dialErr) {
			return nil, fmt.Errorf("check existing unix socket %s: %w", path, dialErr)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}
