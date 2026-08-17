package store

import "sync/atomic"

type Stats struct {
	Reads         uint64  `json:"reads"`
	WriteRequests uint64  `json:"write_requests"`
	Transactions  uint64  `json:"transactions"`
	FailedReads   uint64  `json:"failed_reads"`
	FailedWrites  uint64  `json:"failed_writes"`
	QueueDepth    int     `json:"queue_depth"`
	AvgBatchSize  float64 `json:"avg_batch_size"`
}

type counters struct {
	reads, writes, txns, failedReads, failedWrites atomic.Uint64
}
