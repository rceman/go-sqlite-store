# Design

## Concurrency model

SQLite permits concurrent readers in WAL mode but still serializes writes. The store makes that serialization explicit instead of letting arbitrary caller connections fight over the writer lock.

```text
clients
  |-- reads  --> bounded read queue --> 1..N reader workers/connections
  `-- writes --> bounded write queue --> one writer goroutine/connection
                                      `--> bounded micro-batches
```

The default read pool is intentionally small. In the benchmark workload, adding reader connections past the useful level increased CPU and RSS without improving point-query throughput. Reader requests are additionally checked with `sqlite3_stmt_readonly`, so SQL routed through `Query` cannot mutate state and bypass the single-writer path.

## Write batching

The writer collects up to eight queued requests for up to 250 microseconds and commits them in one outer `BEGIN IMMEDIATE` transaction. Each request gets its own SQLite savepoint:

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

This deliberately trades a bounded amount of commit independence for substantially lower fsync/WAL pressure. Requests remain serializable in writer order. Caller SQL cannot issue `BEGIN`, `COMMIT`, `ROLLBACK`, savepoint control, `ATTACH`/`DETACH`, or `PRAGMA`; the store owns those connection and transaction semantics. A request is also restricted to exactly one parsed SQLite statement.

`Close()` is graceful: it closes admission first, drains requests that were already accepted, then closes the SQLite connections. This avoids acknowledging a write and subsequently discarding it merely because the embedding process began an orderly shutdown.

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

## Embedded and daemon modes

`store` is the source of truth. Embedded applications call it directly and pay no IPC cost.

`sqlite-stored` exposes the same store over HTTP/JSON on a Unix Domain Socket. UDS was chosen for local multi-process use because it provides filesystem permissions, no port allocation, and no exposed TCP listener. The socket mode is `0600` by default.

The daemon protocol accepts SQL because this repository is a generic SQLite infrastructure layer. Domain services should wrap it with named operations when SQL must not cross a trust boundary.

## Resource baseline

A same-sandbox 30-second workload with 16 logical clients, 2 readers, 1 writer, batch cap 8, 80/20 read/write mix, FULL sync, and 512-byte event payload measured approximately:

- 34.9k mixed operations/s;
- 7.0k durable logical writes/s;
- 7.7 ms write p95;
- 18.6 ms write p99;
- 52% average process CPU;
- 21 MiB average RSS;
- 24 MiB peak RSS;
- zero SQLITE_BUSY errors.

A matched Rust implementation was within about one percent throughput. The concurrency architecture, not the host language, was the significant performance lever.
