package store

import "time"

type Config struct {
	Path               string
	Readers            int
	WriteQueueDepth    int
	BatchSize          int
	BatchWindow        time.Duration
	BusyTimeout        time.Duration
	CacheKiB           int
	MmapBytes          int64
	WALAutoCheckpoint  int
	JournalSizeLimit   int64
	Synchronous        string
	DisableForeignKeys bool
}

func (c Config) withDefaults() Config {
	if c.Readers <= 0 {
		c.Readers = 2
	}
	if c.WriteQueueDepth <= 0 {
		c.WriteQueueDepth = 4096
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 8
	}
	if c.BatchWindow <= 0 {
		c.BatchWindow = 250 * time.Microsecond
	}
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = 5 * time.Second
	}
	if c.CacheKiB <= 0 {
		c.CacheKiB = 8192
	}
	if c.MmapBytes <= 0 {
		c.MmapBytes = 256 << 20
	}
	if c.WALAutoCheckpoint <= 0 {
		c.WALAutoCheckpoint = 2000
	}
	if c.JournalSizeLimit <= 0 {
		c.JournalSizeLimit = 64 << 20
	}
	if c.Synchronous == "" {
		c.Synchronous = "FULL"
	}
	return c
}
