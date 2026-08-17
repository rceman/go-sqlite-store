# Design

## Concurrency model

SQLite permits concurrent readers in WAL mode but still serializes writes. The store makes that serialization explicit instead of letting arbitrary caller connections fight over the writer lock.

```text
clients
  |-- reads  --> bounded read queue --> 1..N reader workers/connections
  `-- writes --> bounded write queue --> one writer goroutine/connection
                                      `--> bounded micro-batches
```

The default read pool is intentionally small. In the benchmark workload, adding reader connections past the useful level increased CPU and RSS without improving point-query throughput. Reader requests are additionally checked with SQLite read-only classification and an authorizer, so SQL routed through `Query` cannot mutate state and bypass the single-writer path.

## Write batching and acknowledgement

The writer collects up to eight queued requests for up to 250 microseconds and commits them in one outer `BEGIN IMMEDIATE` transaction. Each logical request gets its own SQLite savepoint:

```text
BEGIN IMMEDIATE
  SAVEPOINT r0
    request 0 statements
  RELEASE r0
  SAVEPOINT r1
    request 1 statements
  RELEASE r1
COMMIT
```

A failing request rolls back to its savepoint, so its statements do not partially apply and unrelated requests in the same outer transaction can still succeed. A failure of the outer commit fails every request whose result depended on that commit.

A caller receives success only after the outer `COMMIT` has returned successfully. This is the durability acknowledgement boundary used by GPT-Tunnel-style authoritative state mutations.

This deliberately trades a bounded amount of commit independence for substantially lower fsync/WAL pressure. Requests remain serializable in writer order. Caller SQL cannot issue `BEGIN`, `COMMIT`, `ROLLBACK`, savepoint control, `ATTACH`/`DETACH`, or `PRAGMA`; the store owns those connection and transaction semantics. A request is also restricted to exactly one parsed SQLite statement.

## Optimistic atomicity

`Statement.RequireRowsAffected` adds an exact positive row-count requirement to a statement. Zero disables the requirement.

This is intended for patterns such as:

```text
UPDATE authoritative state WHERE id=? AND revision=?  -- require 1
INSERT durable event                                  -- require 1
```

If a requirement fails, the logical request is rolled back to its savepoint before the surrounding micro-batch commits. The caller receives `ErrRowsAffectedMismatch`.

This avoids exposing caller-owned transactions merely to implement optimistic revision checks and guarantees that a stale state update cannot accidentally append an event describing a mutation that did not occur.

## Cancellation semantics

Cancellation before admission prevents a request from entering the relevant queue. Once a write has been admitted, cancellation can race with execution/commit: a caller may observe `context.DeadlineExceeded` even though the writer is already committing the request.

Therefore a timed-out admitted write has an **unknown outcome**. Applications that retry authoritative mutations should combine deterministic IDs/unique constraints, expected revisions, and a re-read before retrying.

Read cancellation currently does not call `sqlite3_interrupt` on an already-running `sqlite3_step`. Generated query systems must therefore emit bounded/indexed queries rather than rely on cancellation to stop an unbounded scan. SQLite interrupt/progress cancellation is intentionally deferred until a real workload demonstrates that it is necessary.

## Graceful shutdown

`Close()` serializes against request admission, closes admission, drains requests that were already accepted, waits for reader/writer workers to close their SQLite connections, and only then releases the database owner lock.

This avoids acknowledging a write and subsequently discarding it merely because the embedding process began an orderly shutdown.

## Durability defaults

The baseline is:

- WAL journal mode;
- `synchronous=FULL`;
- 2000-page WAL autocheckpoint;
- 64 MiB journal size limit;
- 8 MiB SQLite page cache per connection;
- 256 MiB mmap ceiling;
- foreign keys enabled;
- 5 second busy timeout.

`FULL` was chosen over `NORMAL` because the intended workloads include durable task/audit state. Benchmarks still showed thousands of durable logical writes per second with batching, so weakening durability was not necessary for the target use case.

A subprocess crash/reopen test deliberately exits without `Store.Close()` after acknowledged commits and verifies WAL recovery on reopen. This tests abrupt process exit and recovery semantics; it is not a physical power-loss laboratory test.

## Database ownership

On Linux one cooperating store process owns a database path at a time. `Open` acquires a non-blocking advisory `flock` on `<database>.lock` and holds it for the store lifetime. A second owner receives `ErrAlreadyOpen`.

The lock is intentionally advisory. It prevents two `go-sqlite-store` owners from accidentally managing the same file, but unrelated software can still bypass it and open SQLite directly. Direct external writers are unsupported.

Non-Linux platforms are explicitly unsupported by the ownership layer. The target runtime for this project is Linux.

## Migrations

The `migrate` package applies application migrations through the same public `Query`/`Exec`/`Batch` contract. Each migration's statements and its `schema_migrations` marker are one logical batch request.

Migrations are intended to run during startup before normal traffic is admitted. This removes the need for a second privileged SQLite connection or caller-controlled transaction API. The same helper works through the embedded store and the Go UDS client.

## Embedded and daemon modes

`store` is the source of truth. Embedded applications call it directly and pay no IPC cost.

`sqlite-stored` exposes the same logical store operations over HTTP/JSON on a Unix Domain Socket. UDS was chosen for local multi-process use because it provides filesystem permissions, no port allocation, and no exposed TCP listener. The socket mode is `0600` by default.

The daemon protocol uses typed wire values rather than raw JSON `interface{}` values. This preserves INTEGER precision (including full signed int64), REAL, TEXT, BLOB, NULL, boolean argument values, and `time.Time` argument behavior. Managed errors also carry a stable wire code so the Go client can preserve `errors.Is` semantics across the process boundary.

The daemon protocol accepts SQL because this repository is a generic SQLite infrastructure layer. Domain services should wrap it with named operations when SQL must not cross a trust boundary.

## Performance decisions

The final integration benchmark shows the expected tradeoff:

- embedded mode has substantially lower latency and allocation cost;
- UDS mode remains fast enough for local multi-process use but is not a performance optimization;
- a GPT Tunnel integration should use embedded mode unless process separation is a real requirement.

No prepared-statement cache is included in `0.2.0`. The measured embedded path is already fast enough that cache invalidation, `SQLITE_SCHEMA` handling, statement reset lifecycle, and authorizer interactions would add complexity without evidence of a useful bottleneck.

See [`BENCHMARKS.md`](BENCHMARKS.md) for exact measurements and environment details.
