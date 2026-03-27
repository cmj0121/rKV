# rKV

![rKV](../docs/icon-rkv.svg)

> A persistent, revision-aware key-value store in a single binary

**rKV** is a lightweight key-value storage engine built on LSM-tree architecture. It ships as one
binary that bundles an embedded Rust library, REPL, HTTP API with Web UI, FFI bindings (C/Python/Go),
and peer replication — no external dependencies, no runtime to install.

## Why rKV?

- **All-in-one binary** — REPL, HTTP API, Web UI, FFI bindings, and replication in a single executable
- **Revision history per key** — every write produces a unique RevisionID; query full history without extra schema
- **Per-namespace encryption** — AES-256-GCM with Argon2 key derivation; mix encrypted and plaintext namespaces
- **Content-addressed dedup** — large values stored as BLAKE3-hashed bin objects; identical values share storage
- **Peer replication** — multi-writer with last-writer-wins over pure TCP; no external coordination service
- **Namespace-level sharding** — distribute namespaces across shard groups without breaking scan/rscan
- **Pluggable I/O** — buffered, mmap (default), or direct I/O per database instance
- **TTL / expiry** — per-key time-to-live with automatic cleanup
- **LZ4 + Zstd compression** — bin objects and SSTable blocks compressed independently
- **Embeddable** — use as a Rust library, C/Python/Go FFI, HTTP service, or interactive REPL

## Quick Start

### REPL

```sh
git clone https://github.com/user/rkv && cd rkv
cargo install --path rkv
rkv
> put name "hello world"
> get name
hello world
> scan name*
name
> exit
```

### HTTP Server

```sh
cargo run --features server -- serve --ui

# Put / Get / Scan / Delete
curl -X PUT localhost:8321/api/_/keys/greeting -d '"hello"'
curl localhost:8321/api/_/keys/greeting
curl localhost:8321/api/_/keys?prefix=greet
curl -X DELETE localhost:8321/api/_/keys/greeting
```

Web UI at `http://localhost:8321/ui` when `--ui` is enabled.

### Embedded (Rust)

```rust
use rkv::{DB, Config};

let db = DB::open(Config::new("/tmp/my.db"))?;
let ns = db.namespace("_", None)?;
ns.put("key", "value", None)?;
let val = ns.get("key")?;
```

### Docker

```sh
docker compose up --build
# Primary:  http://localhost:8321 (read + write)
# Replicas: http://localhost:8324, :8325 (read-only)
```

## Key Features

**Revision history** — every write produces a unique RevisionID (ULID-like). Query any key's
full history with `rev_get` / `rev_count`. No schema changes, no extra tables.

**Namespaces** — isolated key spaces within one database. Created on first use, each with
independent MemTable, SSTables, object store, and optional encryption.

**Per-namespace encryption** — AES-256-GCM encryption with Argon2 key derivation. Encrypt
sensitive namespaces while others stay plaintext. One database, mixed security levels.

**Value separation** — values larger than 1 KB are stored as content-addressed bin objects
(BLAKE3 hash). Identical values within a namespace are automatically deduplicated. Reduces
write amplification for large payloads.

**I/O backends** — choose between buffered I/O, memory-mapped I/O (mmap, default), or
direct I/O (O_DIRECT / F_NOCACHE) per database instance.

**Replication** — primary-replica for read scaling, peer-to-peer for multi-writer with
last-writer-wins conflict resolution. Pure TCP, no external coordination service.

**Cluster sharding** — namespace-level sharding across shard groups with stateless gateway
routing. Each shard group uses standard replication internally.

## Architecture

```text
Client ──► AOL (append-only log) ──► MemTable ──► Response
                                        │ flush
                                   L0 SSTables
                                        │ compaction
                                   L1 → L2 → L3
```

Writes hit the append-only log first (crash safety), then the in-memory write buffer.
Background threads handle flush-to-disk and level-based compaction. Reads merge across
the MemTable and all SSTable levels with a streaming merge iterator.

## Documentation

| Document                              | Covers                                                   |
| ------------------------------------- | -------------------------------------------------------- |
| [CONCEPTS.md](CONCEPTS.md)            | Core concepts, configuration, CLI reference, HTTP server |
| [Storage Engine](docs/storage.md)     | LSM-tree internals, compaction, I/O backends, recovery   |
| [Replication](docs/replication.md)    | Primary-replica and peer replication protocols           |
| [Cluster / Sharding](docs/cluster.md) | Namespace-level sharding architecture                    |

## Not Supported (by design)

| Feature             | Rationale                                          |
| ------------------- | -------------------------------------------------- |
| Transactions (ACID) | WriteBatch provides atomicity; no read isolation   |
| Compare-and-swap    | RevisionID is for history, not concurrency control |
| Snapshots           | Every read sees the latest state                   |
| Watch / Subscribe   | Storage engine, not a message broker               |
