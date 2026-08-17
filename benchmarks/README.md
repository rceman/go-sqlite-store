# Benchmark baseline

The initial architecture was selected after same-host Go vs Rust testing against the same system SQLite library.

Production-like workload:

- 16 logical clients;
- 2 read connections;
- 1 dedicated writer;
- write batch cap 8;
- 80% reads / 20% writes;
- `WAL` + `synchronous=FULL`;
- `wal_autocheckpoint=2000`;
- 512-byte event payload;
- 8 MiB SQLite cache per connection;
- 30 second sustained run.

Selected Go result from the sandbox run:

- ~34.9k mixed ops/s;
- ~7.0k durable logical writes/s;
- write p95 ~7.7 ms;
- write p99 ~18.6 ms;
- average process CPU ~52%;
- average RSS ~21 MiB;
- peak RSS ~24 MiB;
- zero `SQLITE_BUSY` and zero operation errors.

Rust was within ~1% throughput of Go on the same workload, so language choice was not a meaningful storage-performance lever. The material performance improvement came from the concurrency model: one writer, bounded batching, and a small read pool.

Use `go test -bench=. ./benchmarks` for a local regression signal. For hardware tuning, run sustained workloads and compare throughput, p95/p99 latency, CPU, RSS, and kernel write bytes per logical write.
