# Concepts

## Overview

**rKV** is a persistent, revision-aware key-value store built on LSM-tree architecture. It is designed to be embedded
directly into Rust applications, accessed through FFI bindings (C/Python/Go), or used as a standalone CLI tool.

Every write produces a new **revision**, enabling history queries and compare-and-swap (CAS) operations without
external coordination.

## Architecture

```text
                     ┌───────┐   ┌──────────────┐
   Client API ──────►│  AOL  │──►│  WriteBuffer │──► Response
                     └───────┘   └──────┬───────┘
                                        │ background flush
                                 ┌──────▼───────┐
                                 │   L1 SSTable │
                                 └──────┬───────┘
                                        │ merge
                                 ┌──────▼───────┐
                                 │   L2 SSTable │
                                 └──────┬───────┘
                                        │ merge
                                 ┌──────▼───────┐
                                 │   L3 SSTable │
                                 └──────────────┘
```

- **Write path**: Client -> AOL (append-only log, fsync for durability) -> WriteBuffer (in-memory) -> respond to
  caller. Background flush moves WriteBuffer to L1 SSTable; merge compacts L1->L2->L3.
- **Read path**: WriteBuffer -> frozen buffer -> SSTable files (newest first), with a block cache for decompressed
  blocks.
- **Revisions**: Each key-value pair carries a monotonically increasing revision ID. Reads return the latest revision
  by default; history queries retrieve older revisions.

## Core Concepts

### Key-Value Store

The fundamental unit is a `(key, value)` pair. The store provides `put`, `get`, `del`, `has`, `scan`, `count`, and
`rev` (revision history) operations.

### Key

A Key identifies a record. It is one of two variants:

- **Int** — a signed 64-bit integer (`i64`). Supports ordering and comparison.
- **Str** — a UTF-8 string (max 255 bytes, no interior nulls). No ordering guarantee.

Booleans are syntax sugar: `true` → `Int(1)`, `false` → `Int(0)`.

**Auto-upgrade**: A database starts in **ordered mode** where all keys are `Int` and support comparison. When the first
`Str` key is inserted, the engine performs an irreversible **key type upgrade**: all existing `Int` keys are widened to
`Str` (e.g., `Int(42)` becomes `Str("42")`), and the database enters **unordered mode** permanently. Exact-match
operations (`get`, `has`, `del`) continue to work normally in both modes.

**Scan behavior** depends on the database mode:

- **Ordered mode** (Int keys): `scan(prefix, limit)` returns keys in ascending order starting from `prefix`;
  `rscan` returns keys in descending order.
- **Unordered mode** (Str keys): `scan(prefix, limit)` returns keys whose string representation starts with `prefix`;
  `rscan` returns the same prefix-matched keys in reverse order. Ordering-based range queries are not available.

### Value

A Value is the payload associated with a key. It has three internal states:

- **Data** — arbitrary-length byte vector. An empty `Data` (zero bytes) is a valid, distinct value.
- **Null** — the key exists but carries no payload. `Null` is semantically different from empty `Data`.
- **Tombstone** — an internal deletion marker. When a key is deleted, the engine writes a tombstone instead of
  physically removing the entry. Tombstones are invisible to the public API — `get` on a tombstoned key returns
  "key not found", indistinguishable from a key that never existed. Tombstones are resolved (garbage-collected) during
  SSTable compaction.

### Value Separation (Bin Objects)

In a standard LSM-tree every value — regardless of size — is copied through each compaction level.
For large values (images, JSON blobs, serialized objects) this causes severe **write amplification**:
the same multi-KB payload is rewritten from L1 to L2 to L3 even though only the key ordering changes.

rKV uses **value separation** inspired by WiscKey and Git's object model:

- **Small values** (<= `object_size`, default 1 KB) stay **inline** in the LSM-tree, keeping
  read latency low for common payloads.
- **Large values** (> `object_size`) are stored as **bin objects** — standalone files in a
  content-addressable object store. The LSM-tree entry stores a compact `ValuePointer` instead
  of the raw bytes.

#### Object Identity

Each bin object is identified by its **BLAKE3 content hash** (32 bytes / 64 hex chars). The hash
serves as both the unique identifier and the filename. Identical values produce the same hash,
giving **automatic deduplication** — if two keys store the same 5 MB image, only one object file
exists on disk.

#### Object Store Layout

Object files are stored in a Git-style **fan-out directory** structure using the first byte
(2 hex chars) of the hash as a subdirectory:

```text
<db>/objects/
  ab/cdef0123456789abcdef0123456789abcdef0123456789abcdef01234567
  ab/ff01234567890abcdef01234567890abcdef01234567890abcdef0123456
  cd/0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab
```

This limits the number of entries per directory, scaling to millions of objects without
hitting filesystem performance cliffs.

#### Object File Format

Each object file contains a 1-byte header followed by the payload:

```text
[ flags: 1 byte ][ payload ]

flags bit 0: 0 = raw, 1 = LZ4-compressed
flags bits 1-7: reserved
```

When `compress` is enabled (default), the payload is LZ4-compressed. The flags byte
tells the reader how to decode. Compression is applied per-object at write time.

#### Write Path

```text
put(key, value)
      │
      ▼
  len(value) > object_size?
      │
  ┌───┴────┐
  │ no     │ yes
  ▼        ▼
inline   BLAKE3 hash the value
in LSM   object file exists?
           │
         ┌─┴──┐
         │yes │ no
         │    ▼
         │  LZ4 compress (if enabled)
         │  write to objects/<prefix>/<hash>
         ▼
         store ValuePointer in LSM
```

Deduplication happens naturally: if the object file already exists (same hash), the write
is skipped and only the LSM entry is created.

#### Read Path

`get(key)` → read `ValuePointer` from LSM → open `objects/<prefix>/<hash>` → read flags byte
→ decompress if needed → return value.

#### ValuePointer Format (36 bytes fixed)

| Field  | Type       | Bytes | Description                                |
| ------ | ---------- | ----- | ------------------------------------------ |
| `hash` | `[u8; 32]` | 32    | BLAKE3 content hash (also object filename) |
| `size` | `u32`      | 4     | Original uncompressed value size in bytes  |

#### Tuning

The `object_size` and `compress` fields are configurable via the `Config` struct
(see Configuration below). Setting `object_size` to `0` forces all values to bin objects;
setting it to `usize::MAX` effectively disables separation.

#### Garbage Collection

GC is deferred to a future phase. Bin objects accumulate on disk; dead entries (overwritten or
deleted values) are not reclaimed until a GC mechanism is implemented. Because objects are
content-addressed, an object is safe to delete only when no `ValuePointer` in any LSM level
references its hash.

### Namespace

A namespace is an isolated key-value table within a single database. Each namespace has its own key space and
independent auto-upgrade state. Namespaces are identified by string names and created implicitly on first use via
`db.namespace("name")`.

The default namespace is `_`. The CLI starts on `_` and supports switching with the `use` command.

All data operations (`put`, `get`, `del`, `has`, `scan`, `rscan`, `count`) live on the `Namespace` handle, not on
`DB` directly. `DB` is responsible for lifecycle (`open`, `close`, `path`) and namespace management (`namespace`).

### Revision ID

A RevisionID is a monotonically increasing 128-bit unsigned integer (`u128`) that uniquely identifies a mutation.
RevisionIDs are **system-wide unique** — generated by the rKV instance and shared across all namespaces. A single global
counter ensures no two mutations, regardless of namespace, ever produce the same RevisionID. Every successful `put`
returns the new RevisionID to the caller. RevisionIDs are displayed as Crockford Base32 encoded strings (e.g., `7z` for
revision 255).

History queries use revision indexing: `rev_count(key)` returns the total number of revisions for a key, and
`rev_get(key, index)` retrieves the value at a specific revision index (0 = oldest). CAS operations use RevisionIDs for
optimistic concurrency control.

### TTL (Time-to-Live)

A key can be stored with an optional TTL via `put_with_ttl(key, value, ttl)`. After the duration elapses the key is
considered expired and behaves as if it were deleted — `get` returns "key not found", `exists` returns `false`. The
`ttl(key)` method returns the remaining duration, or `None` if the key has no expiration. A regular `put` (without TTL)
stores the key permanently.

TTL is specified as a `std::time::Duration` in the library API. The CLI accepts human-friendly suffixes:
`10s` (seconds), `5m` (minutes), `2h` (hours), `1d` (days). Plain numbers are treated as seconds.

### Configuration

The `Config` struct controls database behavior and LSM tuning parameters:

| Field               | Type      | Default    | Description                               |
| ------------------- | --------- | ---------- | ----------------------------------------- |
| `path`              | `PathBuf` | (required) | Database directory path                   |
| `create_if_missing` | `bool`    | `true`     | Create the directory if it does not exist |
| `write_buffer_size` | `usize`   | 4 MB       | In-memory write buffer size before flush  |
| `max_levels`        | `usize`   | 3          | Maximum number of LSM levels              |
| `block_size`        | `usize`   | 4 KB       | SSTable block size                        |
| `cache_size`        | `usize`   | 8 MB       | Block cache size for decompressed blocks  |
| `object_size`       | `usize`   | 1 KB       | Bin object size threshold (see above)     |
| `compress`          | `bool`    | `true`     | LZ4-compress bin objects on disk          |

`Config::new(path)` initializes all fields to their defaults. Fields can be overridden before
passing the config to `DB::open`.

### Statistics

`db.stats()` returns a `Stats` snapshot with counters and metadata:

| Field                 | Type       | Description                           |
| --------------------- | ---------- | ------------------------------------- |
| `total_keys`          | `u64`      | Total number of live keys             |
| `data_size_bytes`     | `u64`      | Approximate on-disk data size         |
| `namespace_count`     | `u64`      | Number of namespaces                  |
| `level_count`         | `usize`    | Number of LSM levels (from config)    |
| `sstable_count`       | `u64`      | Total SSTable files across all levels |
| `write_buffer_bytes`  | `u64`      | Current write buffer usage            |
| `pending_compactions` | `u64`      | Pending compaction tasks              |
| `op_puts`             | `u64`      | Cumulative put operations             |
| `op_gets`             | `u64`      | Cumulative get operations             |
| `op_deletes`          | `u64`      | Cumulative delete operations          |
| `cache_hits`          | `u64`      | Block cache hits                      |
| `cache_misses`        | `u64`      | Block cache misses                    |
| `uptime`              | `Duration` | Time since `DB::open`                 |

`stats()` returns `Stats` directly (not `Result<Stats>`) — it cannot fail. In the stub phase most
counters are zero; `level_count` reflects the configured `max_levels` and `uptime` is computed live.

### Maintenance Operations

Maintenance operations handle durability, recovery, backup, and storage optimization. All
maintenance methods return `Result` and are stubs (`NotImplemented`) during the scaffold phase.

#### Flush / Sync

| Method  | Kind     | Signature             | Description                                 |
| ------- | -------- | --------------------- | ------------------------------------------- |
| `flush` | instance | `&self -> Result<()>` | Flush the in-memory write buffer to disk    |
| `sync`  | instance | `&self -> Result<()>` | Flush and fsync all data to durable storage |

`flush` writes the current write buffer to an L1 SSTable but does not guarantee durability —
data may remain in OS page cache. `sync` calls `flush` followed by `fsync`, ensuring all data
reaches durable storage.

#### Destroy / Repair

| Method    | Kind   | Signature                                  | Description                            |
| --------- | ------ | ------------------------------------------ | -------------------------------------- |
| `destroy` | static | `(path: impl Into<PathBuf>) -> Result<()>` | Delete a database and all its data     |
| `repair`  | static | `(path: impl Into<PathBuf>) -> Result<()>` | Attempt to repair a corrupted database |

Both are static methods — they operate on a path, not a live `DB` handle. `destroy` removes the
entire database directory. `repair` scans for structural corruption and rebuilds indices where
possible.

#### Dump / Load

| Method | Kind     | Signature                                       | Description                               |
| ------ | -------- | ----------------------------------------------- | ----------------------------------------- |
| `dump` | instance | `&self, path: impl Into<PathBuf> -> Result<()>` | Export database to a portable backup file |
| `load` | static   | `(path: impl Into<PathBuf>) -> Result<DB>`      | Import database from a backup file        |

`dump` serializes the entire database (all namespaces, keys, values, and metadata) into a
self-contained backup file. `load` is static and returns a new `DB` — the backup file encodes
its own configuration, so no separate `Config` is needed.

`load` is not exposed in the CLI because it would require replacing the live DB handle mid-session.

#### Compaction

| Method    | Kind     | Signature             | Description                                 |
| --------- | -------- | --------------------- | ------------------------------------------- |
| `compact` | instance | `&self -> Result<()>` | Trigger manual compaction of SSTable levels |

Background compaction runs automatically, but `compact` allows manual triggering — useful after
bulk deletes or to reclaim disk space from resolved tombstones.

### LSM-Tree Storage

Data is organized in levels (L1-L3). Fresh writes land in an in-memory buffer and are periodically flushed to sorted
SSTable files on disk. Background merge compaction keeps read amplification bounded.

### Embeddable Library

The engine is a Rust library crate (`rkv`) that can be linked into any Rust program. FFI bindings expose the same API
to C, Python, and Go consumers.

### CLI Tool

A REPL binary built on top of the library provides interactive access for debugging, exploration, and scripting.

## Design Decisions

- **Interior mutability**: `DB` is `Send + Sync`. All mutable fields use `Mutex<T>` or `RwLock<T>` so public methods
  take `&self`.
- **Stub-first development**: The initial scaffold returns `NotImplemented` for all engine methods, allowing the CLI
  and test harness to be built before any storage logic exists.
- **Binary/library boundary**: The CLI (`main.rs`) is strictly binary-only. `lib.rs` exports only engine types
  (`DB`, `Config`, `Error`, `Result`). Nothing from the REPL leaks into the library or FFI surface.
