# Changelog

## 0.2.0 — 2026-08-17

Integration-ready release candidate for durable local service state.

### Added

- Linux single-owner database lock with `ErrAlreadyOpen`.
- Application migration helper shared by embedded and daemon modes.
- `Statement.RequireRowsAffected` and `ErrRowsAffectedMismatch` for optimistic atomic mutations.
- Typed UDS wire values preserving int64 precision, BLOB/TEXT distinction, NULL, REAL, boolean and `time.Time` argument behavior.
- Typed daemon error codes with `errors.Is` preservation in the Go client.
- Abrupt-process-exit WAL recovery test.
- Concurrent task-state + durable-event stress/reopen test.
- Embedded-vs-UDS and GPT-Tunnel-like task/event benchmarks.
- GPT Tunnel integration contract and recorded benchmark evidence.

### Changed

- Daemon mode is explicitly positioned as a process-ownership boundary; embedded mode is preferred for in-process Go services.
- Migration and authoritative write acknowledgement semantics are documented around the managed `Batch`/COMMIT boundary.
- Integration benchmark workflow is manual-only.

### Deliberately deferred

- Prepared-statement caching until profiling demonstrates a material parse/prepare bottleneck.
- `sqlite3_interrupt`/progress-handler query cancellation until bounded generated queries demonstrate a real need.
- Custom binary IPC protocol; measured UDS HTTP/JSON overhead does not justify the added protocol complexity.

## 0.1.0 — 2026-08-17

Initial foundation with WAL/FULL defaults, single writer, bounded micro-batching, small read pool, managed SQL boundary, embedded API, Unix socket daemon, Go client, stats, tests, race detection and CI.
