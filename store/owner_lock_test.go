//go:build linux

package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOnlyOneStoreOwnsDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner.db")
	first, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(Config{Path: path})
	if err == nil {
		_ = second.Close()
		t.Fatal("expected second owner to be rejected")
	}
	if !errors.Is(err, ErrAlreadyOpen) {
		t.Fatalf("expected ErrAlreadyOpen, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Config{Path: path})
	if err != nil {
		t.Fatalf("reopen after owner close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
