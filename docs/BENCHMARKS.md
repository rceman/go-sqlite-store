# Benchmark evidence

Benchmarks in this repository are intended to compare architecture/configuration choices on the same host. Absolute numbers depend on CPU, kernel, filesystem, storage, SQLite build, and runner noise.

## Integration-ready run — 2026-08-17

Source commit: `33204bb2871e6f96321cb61aebfb376a349f0b16`

Environment reported by GitHub Actions:

- Ubuntu 24.04.4
- Linux/amd64
- AMD EPYC 7763 64-Core Processor
- Go 1.23.12
- SQLite development package 3.45.1
- CGO enabled

Command:

```bash
go test ./benchmarks -run '^$' \
  -bench 'Benchmark(MixedEmbedded|MixedUnixSocket|TaskEventEmbedded)$' \
  -benchmem -benchtime=5s -count=3
```

Common store profile:

- 2 reader connections;
- 1 writer connection;
- batch cap 8;
- 250 microsecond batch window;
- WAL;
- `synchronous=FULL`;
- remaining settings use production defaults.

### Results

| Benchmark | Run 1 | Run 2 | Run 3 | Median | Median allocations |
|---|---:|---:|---:|---:|---:|
| Task/event embedded | 12.866 us/op | 12.817 us/op | 13.387 us/op | **12.866 us/op** | 480 B/op, 19 allocs/op |
| Mixed embedded | 9.172 us/op | 9.329 us/op | 9.301 us/op | **9.301 us/op** | 290 B/op, 9 allocs/op |
| Mixed UDS | 35.224 us/op | 34.327 us/op | 35.479 us/op | **35.224 us/op** | ~9.46 KiB/op, 123 allocs/op |

Approximate aggregate operation rates implied by the medians on this runner:

- mixed embedded: ~107.5k ops/s;
- GPT-Tunnel-like task/event embedded: ~77.7k ops/s;
- mixed UDS: ~28.4k ops/s.

The task/event benchmark uses an 80/20 read/write pattern. Each logical write is an atomic `Batch` containing a task state update and an event insert, both with `RequireRowsAffected: 1`; event payload size is 512 bytes.

### Interpretation

On this runner the UDS path is about **3.8x** the mixed embedded latency and allocates materially more memory per request because it includes HTTP framing, JSON encoding/decoding, typed value conversion, and socket I/O. The absolute UDS latency is still small for local service use, but there is no reason for an in-process GPT Tunnel integration to pay that cost.

The recommended GPT Tunnel deployment is therefore **embedded `store`**, while `sqlite-stored` remains an optional process-ownership boundary when multiple independent processes need one database owner.

No prepared-statement cache was added for this release. The embedded path is already fast enough that cache invalidation/schema-change complexity is not justified without profile evidence.

## Sustained resource baseline

Earlier 30-second sandbox tests of the same one-writer/small-read-pool architecture used 16 logical clients, 80/20 reads/writes, `FULL`, batch cap 8, checkpoint 2000, and 512-byte event payload. The service-model Go run measured approximately:

- 38.8k mixed ops/s;
- 7.75k durable logical writes/s;
- 5.5 ms write p95;
- 14.3 ms write p99;
- 55.7% average process CPU (percentage of one core);
- 21.2 MiB average RSS;
- 24.6 MiB peak RSS;
- zero `SQLITE_BUSY` and zero application errors.

Those sustained numbers came from a different host/storage environment and must not be compared directly with the GitHub Actions microbenchmarks above. They are retained as evidence for the memory/CPU shape and the one-writer architecture rather than as a universal throughput promise.
