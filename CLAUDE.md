# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Distributed key–value store in Go: replicas coordinated by a from-scratch **Raft** implementation (gRPC between nodes) with an **HTTP/JSON** API for clients. Documentation, design docs, and many comments are in Portuguese.

## Commands

```bash
make build              # go build ./...
make test               # go test ./... -race  (always run with -race)
go test ./internal/wal -race -run TestName    # single test
make proto              # regenerate proto/raft/*.pb.go from proto/raft/raft.proto (needs protoc)

make run-single-node    # 1 node: HTTP :8001 / gRPC :9001
make run1 / run2 / run3 # 3-node cluster, one terminal each (HTTP 8001-8003, gRPC 9001-9003)
make state1             # curl /state of node1 (requires jq)
make propose ADDR=localhost:8001 CMD='set x 42'
```

A Cursor stop hook runs `make cursor-agent-stop` (build + `go test -race ./...`); the codebase is expected to always compile and pass race-enabled tests.

## Rules

@.claude/rules/commit-message.md
@.claude/rules/mcp-golang-context.md

## Architecture

### Node wiring (`cmd/node`)

`cmd/node/main.go` parses flags (`cfg.ParseNodeFlags`: `-id` required; `-grpc-addr`, `-http-addr`, `-peers`, WAL/fsdb/checkpoint dirs default to `tmp/<id>/`), loads env tuning (`cfg.Load`), then delegates to `cmd/node/setup`:

- `setup.OpenRaft` — Raft node + persisted Raft WAL/meta
- `setup.OpenKV` — full persistence stack (see below), WAL replay, background checkpoint/flush goroutines
- `setup.GetRoutes` — registers all HTTP handlers from `internal/api`

Runtime tuning is environment-only via `internal/cfg/cfg.go` (cleanenv): `CACHE_TTL`, `CACHE_MAX_ENTRIES`, `CHECKPOINT_INTERVAL`, `FS_DEFER_WRITES`, `FS_FLUSH_INTERVAL`, etc.

### Persistence stack — three layers, written in order log → disk → memory

`db.Adapter` (`internal/db/adapter.go`) coordinates writes under a single mutex through three interfaces:

1. **WAL** (`internal/wal`, `LogDb`) — durability/recovery; CBOR codec (`internal/code`), `SyncEveryWrite`, periodic checkpoints (`internal/durable` stores LastSeq + checkpoint blobs). `BeforeCheckpoint` flushes the fsdb batcher so checkpoints never run ahead of disk state.
2. **fsdb** (`internal/fsdb`, `DB`) — on-disk store, wrapped by `WriteBatcher` (last-write-wins coalescing when `FS_DEFER_WRITES=true`; flushed periodically by `internal/tasks` and at shutdown).
3. **memdb/cache** (`internal/cache`, `Cache`) — in-memory LRU with TTL and max-entries cap; read-through via `Once`, write/delete coupled to store success via `SaveIfOk`/`DelIfOk`.

On startup, `OpenKV` replays the WAL (from the last checkpoint) to rebuild fsdb/memdb state.

### Raft and replication

- `internal/raft` — Raft core (elections, AppendEntries, commit/apply loops). `internal/raftpb` + `proto/raft` — gRPC transport between nodes.
- **Leader write path:** HTTP handler applies the write locally via `db.Adapter`, then calls `ProposeCommand` to replicate.
- **Follower write path:** committed entries arrive via `raftNode.RunAppliedLoop` in `main.go`, which applies them through `api.GrpcHandle` to the same `db.Adapter`. Followers reject client writes, pointing at the leader (the SDK detects "follower" in the response body and retries against the leader).
- Commands are encoded by `internal/commands` and shared between the Raft log and WAL.
- Writes accept a `raft_min_acks` quorum parameter (query/body); `0`/omitted maps to full-cluster replication (`api.HTTPDefaultRaftMinAcks`).

### HTTP layer (`pkg/www` + `internal/api`)

Handlers are declarative `www.Handler{Pattern, Fn}` values returning `*www.Response` (not raw `http.HandlerFunc`); each route lives in its own `internal/api/*_handle_http.go` file and is registered in `cmd/node/setup/routes.go`. Request params decode via struct tags `param:"..."` (path) and `query:"..."` with a `Validate()` method. Main routes: `POST /table`, `PUT /table/{tableName}` (+ bulk), `GET /table/{tableName}/{key}`, `GET /table/{tableName}?sk=…`, `DELETE`, `GET /state`.

### Client SDK (`SDK/`)

Go client over the HTTP API (`sdk.GetOrCreateTable` → `Table` with item operations); handles follower redirection to the leader.

### Design docs

`specs/`, `docs/` (incl. `docs/adr`), and `refining/` hold design documents (checkpoint/WAL recovery, quorum config, LRU eviction, consistency trade-offs). Check these before changing persistence or replication semantics.
