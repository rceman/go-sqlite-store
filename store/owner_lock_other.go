//go:build !linux

package store

import "fmt"

type ownerLock struct{}

func acquireOwnerLock(dbPath string) (*ownerLock, error) {
	return nil, fmt.Errorf("go-sqlite-store single-owner locking is Linux-only: %w", ErrUnsupportedPlatform)
}

func (l *ownerLock) close() error { return nil }
