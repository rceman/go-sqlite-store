# GPT Tunnel integration contract

This document defines the intended first integration of `go-sqlite-store` into GPT Tunnel. The store remains domain-agnostic; GPT Tunnel owns entity schemas, validation, Query DSL compilation, revision semantics, and Hub synchronization.

## Deployment mode

Use **embedded mode** for the first GPT Tunnel integration:

```text
GPT Tunnel daemon
  -> go-sqlite-store/store
       -> 2 reader connections
       -> 1 batched writer connection
       -> SQLite WAL
```

Do not add an IPC hop inside the same process. `sqlite-stored` remains available when multiple independent processes genuinely need one database owner.

## Startup order

1. Resolve the local database path.
2. `store.Open` with the production durability profile.
3. Apply application migrations with `migrate.Apply`.
4. Build repositories/query adapters on top of the opened store.
5. Only then admit MCP/session/application traffic.
6. Start Hub synchronization after local storage is ready.

Migrations must run before normal traffic. They use the same managed transaction path as application writes.

## Recommended configuration

```go
s, err := store.Open(store.Config{
    Path:              dbPath,
    Readers:           2,
    BatchSize:         8,
    BatchWindow:       250 * time.Microsecond,
    Synchronous:       "FULL",
    CacheKiB:          8192,
    WALAutoCheckpoint: 2000,
})
```

The defaults already match this profile where values are omitted.

## Authoritative mutation boundary

A domain state change and the durable event that explains it belong in **one `Batch` request**. Do not update the task and append its event in separate store calls.

For optimistic concurrency, require the expected row count so a stale revision rolls the whole request back before the surrounding batched transaction commits:

```go
results, err := s.Batch(ctx, []store.Statement{
    {
        SQL: `UPDATE tasks
              SET revision=?, status=?, updated_at=?
              WHERE id=? AND revision=?`,
        Args: []any{nextRevision, nextStatus, now, taskID, currentRevision},
        RequireRowsAffected: 1,
    },
    {
        SQL: `INSERT INTO events(id, task_id, revision, kind, payload, recorded_at)
              VALUES(?, ?, ?, ?, ?, ?)`,
        Args: []any{eventID, taskID, nextRevision, eventKind, payload, now},
        RequireRowsAffected: 1,
    },
})
```

`errors.Is(err, store.ErrRowsAffectedMismatch)` means the optimistic precondition failed. The request has been rolled back to its per-request SAVEPOINT; the event must not exist.

A successful `Batch` return means the surrounding SQLite `COMMIT` returned successfully. This is the point at which GPT Tunnel may acknowledge an authoritative local mutation as committed.

## Timeout and retry semantics

Context cancellation before admission prevents the request from entering the writer queue. Cancellation after admission is different: the caller can observe `context.DeadlineExceeded` while the writer may already be committing the request.

Treat a timeout after submission as **outcome unknown**, not as proof of rollback. GPT Tunnel should use:

- deterministic event IDs / unique constraints;
- expected entity revision predicates;
- a re-read of authoritative local state before retrying.

This makes retries safe without exposing caller-controlled `BEGIN`/`COMMIT`.

## Hub synchronization

Hub/Git synchronization is outside the local SQLite transaction.

```text
local mutation -> durable SQLite COMMIT -> acknowledge locally
                                  |
                                  +-> async Hub sync later
```

A Hub failure must not roll back a successful local mutation. Record/report the sync failure through GPT Tunnel's normal local durable messaging/event mechanism and retry asynchronously.

The Hub should receive only entities that the GPT Tunnel domain classifies as shared/syncable. Local operational entities remain local even though they are durable across process restarts.

## Query path

Compile GPT Tunnel Query DSL into parameterized, read-only SQL and execute it through `Store.Query`. The store independently rejects mutating statements, transaction control, `ATTACH`/`DETACH`, `PRAGMA`, and multiple SQL statements.

Generated list/search queries must be bounded with an explicit `LIMIT` and backed by application-owned indexes for common filters/sorts. Current context cancellation can stop queue admission / caller waiting but does not interrupt an already-running `sqlite3_step`; therefore unbounded scans are not an acceptable Query DSL execution plan.

This is not a blocker for initial integration when the DSL compiler guarantees bounded queries. Add SQLite interrupt/progress cancellation only if profiling shows long-running generated queries in production or E2E tests.

## Database ownership

Exactly one `go-sqlite-store` owner should manage a database file. On Linux the store takes a non-blocking advisory `flock` on `<database>.lock` for its lifetime and rejects a second store owner.

The lock protects cooperating `go-sqlite-store` processes. It cannot prevent unrelated code from opening SQLite directly. GPT Tunnel must therefore treat direct writes from other processes/libraries as unsupported.

## Embedded vs daemon

Both modes expose the same logical operations (`Query`, `Exec`, `Batch`). The daemon wire protocol uses typed cells so INTEGER, REAL, TEXT, BLOB, NULL, and boolean arguments do not silently change type through JSON.

For GPT Tunnel use embedded mode unless process separation becomes a real requirement. The UDS daemon is an ownership boundary, not a performance optimization.

## Integration acceptance conditions

The store is ready to be adopted by GPT Tunnel when all of the following remain green on Linux CI:

- normal tests, vet, race detector, and daemon build;
- migration idempotence and rollback;
- single-owner rejection and reopen;
- abrupt-process-exit WAL recovery of acknowledged writes;
- concurrent task-state + event atomicity followed by reopen;
- rows-affected mismatch rolls back the whole logical mutation;
- UDS typed-value round trip and migrations through the daemon client;
- representative embedded and UDS benchmarks recorded without a correctness regression.
