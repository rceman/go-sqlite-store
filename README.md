# go-sqlite-store

High-performance SQLite store for Go with WAL, batched writes, read pooling, and an optional Unix socket daemon.

The core design is deliberately small:

- one SQLite writer connection owned by one goroutine;
- bounded write queue with micro-batching;
- a small fixed read-worker pool;
- `WAL` + `synchronous=FULL` durability by default;
- 8 MiB page-cache budget per connection by default;
- no external Go dependencies: CGO links the system SQLite library;
- embedded package and local daemon use the same storage engine.

## Embedded

```go
s, err := store.Open(store.Config{Path: "state.db"})
if err != nil { /* handle */ }
defer s.Close()

_, err = s.Exec(ctx, `INSERT INTO events(kind,payload) VALUES(?,?)`, "task.updated", payload)
rows, err := s.Query(ctx, `SELECT id,kind FROM events ORDER BY id DESC LIMIT ?`, 20)
```

Concurrent write requests are serialized by the writer goroutine and co-committed in bounded transactions. Each request is wrapped in a savepoint so one failed request does not partially apply or poison unrelated requests in the same micro-batch. `Close()` stops admission and drains work that was already accepted before closing the SQLite connections.

## Daemon

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

The protocol is HTTP/JSON over a Unix Domain Socket. The `client` package provides a Go client, so callers do not need to deal with the HTTP transport directly.

The daemon API is intentionally a generic SQL transport. Applications that need a domain-level contract should wrap it with named operations rather than exposing arbitrary SQL across trust boundaries.

The store still owns its concurrency and durability boundary:

- `Query` accepts only statements SQLite classifies as read-only, so a reader cannot bypass the writer with `INSERT ... RETURNING`;
- `Exec`/`Batch` reject caller-owned transaction/savepoint control, `ATTACH`/`DETACH`, and `PRAGMA`;
- each request must contain exactly one SQLite statement (a `Batch` is the explicit multi-statement API).

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

These defaults came from parallel SQLite stress tests with 16 logical clients. They are a baseline, not a universal optimum; storage hardware and workload shape still matter.

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
go test -bench=. ./benchmarks
```

See [`benchmarks/README.md`](benchmarks/README.md) for the measured baseline that motivated the architecture.
