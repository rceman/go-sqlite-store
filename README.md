# go-sqlite-store

High-performance SQLite store for Go with WAL, batched writes, read pooling, migrations, and an optional Unix socket daemon.

The design is deliberately small:

- one SQLite writer connection owned by one goroutine;
- bounded write queue with micro-batching;
- a small fixed read-worker pool;
- `WAL` + `synchronous=FULL` durability by default;
- application-owned atomic multi-statement requests;
- optimistic write guards through `RequireRowsAffected`;
- 8 MiB page-cache budget per connection by default;
- no external Go modules: CGO links the system SQLite library;
- embedded package and local daemon use the same storage engine and logical API;
- one cooperating process owns a database file at a time on Linux.

## Embedded

For an in-process application, use the store directly. This is the recommended mode for GPT Tunnel and other Go daemons.

```go
s, err := store.Open(store.Config{Path: "state.db"})
if err != nil { /* handle */ }
defer s.Close()

_, err = s.Exec(ctx,
    `INSERT INTO events(kind,payload) VALUES(?,?)`,
    "task.updated", payload,
)
rows, err := s.Query(ctx,
    `SELECT id,kind FROM events ORDER BY id DESC LIMIT ?`,
    20,
)
```

Concurrent write requests are serialized by the writer goroutine and co-committed in bounded transactions. Each request is wrapped in its own savepoint, so one failed request does not partially apply or poison unrelated requests in the same micro-batch.

A successful `Exec`/`Batch` return is delivered only after the outer SQLite `COMMIT` succeeds. `Close()` stops admission, drains already accepted work, closes SQLite connections, then releases database ownership.

## Atomic state + event updates

A state mutation and the durable event describing it should normally be one `Batch` request. `RequireRowsAffected` turns an optimistic predicate into part of the atomic request contract:

```go
_, err := s.Batch(ctx, []store.Statement{
    {
        SQL: `UPDATE tasks
              SET revision=?, status=?
              WHERE id=? AND revision=?`,
        Args: []any{nextRevision, nextStatus, taskID, currentRevision},
        RequireRowsAffected: 1,
    },
    {
        SQL: `INSERT INTO events(id,task_id,revision,kind,payload)
              VALUES(?,?,?,?,?)`,
        Args: []any{eventID, taskID, nextRevision, kind, payload},
        RequireRowsAffected: 1,
    },
})
if errors.Is(err, store.ErrRowsAffectedMismatch) {
    // The logical request was rolled back to its savepoint.
}
```

If either statement does not affect exactly one row, the entire logical request is rolled back before the surrounding micro-batch commits.

Context cancellation after a write has been admitted can race with the commit. A caller that receives a timeout must treat the outcome as **unknown**, not as proof that the write rolled back. Use deterministic IDs, unique constraints, revision predicates, and a re-read before retrying.

## Migrations

`migrate.Apply` uses the same managed write path. Each migration and its version marker are one atomic `Batch` request.

```go
err := migrate.Apply(ctx, s, []migrate.Migration{
    {
        Version: 1,
        Name:    "create_tasks",
        Statements: []store.Statement{
            {SQL: `CREATE TABLE tasks(id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`},
        },
    },
}, migrate.Options{})
```

Run migrations during startup before normal application traffic is admitted. The same migration API accepts the embedded store or the Unix-socket Go client.

## Daemon

Use daemon mode only when process separation is useful. It is an ownership boundary, not a performance optimization.

```bash
go build -o sqlite-stored ./cmd/sqlite-stored
./sqlite-stored \
  --db ~/.local/share/myapp/state.db \
  --socket ~/.local/share/myapp/sqlite-store.sock
```

The socket is created with mode `0600`. The daemon exposes:

- `GET /v1/health`
- `GET /v1/stats`
- `POST /v1/query`
- `POST /v1/exec`
- `POST /v1/batch`

The protocol is HTTP/JSON over a Unix Domain Socket. It uses explicit typed values instead of raw `interface{}` JSON, preserving SQLite INTEGER precision, REAL, TEXT, BLOB, NULL, boolean arguments, and `time.Time` argument semantics. Managed error identity is also preserved by the Go client, so `errors.Is` works for errors such as `store.ErrRowsAffectedMismatch` across the daemon boundary.

The daemon API is intentionally a generic SQL transport. Applications that need a domain-level contract should wrap it with named operations rather than expose arbitrary SQL across trust boundaries.

## Managed SQL boundary

The store owns connection and transaction semantics:

- `Query` accepts only statements SQLite classifies as read-only;
- `Exec`/`Batch` reject caller-owned transaction/savepoint control;
- `ATTACH`, `DETACH`, and all caller `PRAGMA` statements are rejected;
- each request contains exactly one parsed SQLite statement; `Batch` is the explicit multi-statement API.

This prevents callers from bypassing the single-writer path with constructs such as `INSERT ... RETURNING` through a reader connection.

## Single database owner

On Linux, `Open` takes a non-blocking advisory `flock` on `<database>.lock` for the store lifetime. A second cooperating store owner receives `store.ErrAlreadyOpen`.

This protects `go-sqlite-store` users from accidentally running two store owners against one file. It cannot stop unrelated programs from opening SQLite directly; direct external writers are unsupported.

## Defaults

| Setting | Default |
|---|---:|
| readers | 2 |
| writer connections | 1 |
| batch size | 8 requests |
| batch window | 250 µs |
| write queue | 4096 requests |
| journal mode | WAL |
| synchronous | FULL |
| busy timeout | 5 s |
| cache | 8 MiB / connection |
| mmap | 256 MiB |
| WAL autocheckpoint | 2000 pages |
| journal size limit | 64 MiB |
| foreign keys | enabled (`DisableForeignKeys` / `--disable-foreign-keys` to opt out) |

These are baseline defaults, not universal optimum settings. Storage hardware and workload shape still matter.

## Performance snapshot

The integration-ready GitHub Actions run on an AMD EPYC 7763 measured median aggregate benchmark costs of:

- mixed embedded: **9.301 µs/op**, 290 B/op, 9 allocs/op;
- GPT-Tunnel-like task+event embedded: **12.866 µs/op**, 480 B/op, 19 allocs/op;
- mixed UDS: **35.224 µs/op**, about 9.46 KiB/op, 123 allocs/op.

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for exact environment, three-run evidence, and sustained CPU/RSS results. Do not compare absolute benchmark numbers across different hosts as if they were one performance series.

## Build requirements

Linux with Go, CGO, `pkg-config`, and SQLite development headers/library. On Debian/Ubuntu:

```bash
sudo apt install build-essential pkg-config libsqlite3-dev
```

## Verify

```bash
go test ./...
go vet ./...
go test -race ./...
go build ./cmd/sqlite-stored
```

The integration benchmark is intentionally manual in GitHub Actions. For local runs:

```bash
go test ./benchmarks -run '^$' \
  -bench 'Benchmark(MixedEmbedded|MixedUnixSocket|TaskEventEmbedded)$' \
  -benchmem -benchtime=5s -count=3
```

For the GPT Tunnel adoption contract, see [`docs/GPT_TUNNEL_INTEGRATION.md`](docs/GPT_TUNNEL_INTEGRATION.md).
