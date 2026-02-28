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
- **Read path**: WriteBuffer -> SSTable files (newest first), with a block cache for decompressed blocks.
  Both point lookups (`get`) and range/prefix queries (`scan`, `rscan`) merge results across all sources.
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

**Scan behavior** depends on the database mode. Both `scan` and `rscan` accept `(prefix, limit, offset)` where
`offset` skips the first N matching keys before collecting up to `limit` results (for pagination).

- **Ordered mode** (Int keys): `scan(prefix, limit, offset)` returns keys in ascending order starting from `prefix`;
  `rscan` returns keys in descending order.
- **Unordered mode** (Str keys): `scan(prefix, limit, offset)` returns keys whose string representation starts with
  `prefix`; `rscan` returns the same prefix-matched keys in reverse order. Ordering-based range queries are not
  available.

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
serves as both the unique identifier and the filename. Deduplication is **per-namespace**, not
global — identical values in the same namespace share one object file, but the same content in
different namespaces produces separate files. This preserves namespace isolation.

#### Object Store Layout

Object files are stored in a Git-style **fan-out directory** structure, scoped per namespace.
The first byte (2 hex chars) of the hash is used as a subdirectory:

```text
<db>/objects/
  <namespace>/
    ab/cdef0123456789abcdef0123456789abcdef0123456789abcdef01234567
    ab/ff01234567890abcdef01234567890abcdef01234567890abcdef0123456
    cd/0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab
```

Each namespace gets its own isolated object store directory. This mirrors the per-namespace
MemTable pattern and limits the number of entries per directory, scaling to millions of
objects without hitting filesystem performance cliffs.

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
         │  write to objects/<ns>/<prefix>/<hash>
         ▼
         store ValuePointer in LSM
```

Deduplication happens naturally: if the object file already exists (same hash), the write
is skipped and only the LSM entry is created.

#### Read Path

`get(key)` → read `ValuePointer` from LSM → open `objects/<ns>/<prefix>/<hash>` → read flags byte
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
`db.namespace("name", None)`.

The default namespace is `_`. The CLI starts on `_` and supports switching with the `use` command.

All data operations (`put`, `get`, `del`, `has`, `scan`, `rscan`, `count`) live on the `Namespace` handle, not on
`DB` directly. `DB` is responsible for lifecycle (`open`, `close`, `path`) and namespace management (`namespace`,
`list_namespaces`, `drop_namespace`).

### Namespace Encryption

Each namespace can independently be encrypted with a user-supplied password. Encryption is
opt-in per namespace — non-encrypted and encrypted namespaces coexist within the same database.

The unified API uses a single method with an optional password parameter:

- `db.namespace("users", None)` — open as non-encrypted
- `db.namespace("users", Some("s3cret"))` — open as encrypted

The encryption state is recorded on first access and enforced within the session:

- Opening an encrypted namespace without a password → `EncryptionRequired` error
- Opening a non-encrypted namespace with a password → `NotEncrypted` error

**CLI syntax**: `use myns +` prompts for a password (hidden input). The prompt indicator shows
`rkv [myns+]>` for encrypted namespaces and `rkv [myns]>` for non-encrypted ones.

#### Cryptographic Operations

Encryption uses **Argon2id** for key derivation and **AES-256-GCM** for authenticated
encryption. The encryption boundary is at the `Namespace` level — value bytes are
encrypted before entering the write path and decrypted after retrieval.

**Key derivation**: `password + salt → Argon2id → 256-bit key`. Each namespace has a
persistent 16-byte random salt stored at `<db>/crypto/<namespace>.salt`. The salt is
created on first encrypted access and reused on subsequent opens. The derived key is
held in memory only (never written to disk).

**Wire format** (per encrypted value):

```text
[nonce (12 bytes)] [ciphertext (variable)] [GCM tag (16 bytes)]
```

A fresh random 96-bit nonce is generated for each encryption operation, ensuring that
identical plaintext produces different ciphertext.

**Write path**: `plaintext → encrypt → maybe_separate (bin objects) → AOL + MemTable`.
**Read path**: `MemTable → resolve_value (pointer→data) → decrypt → plaintext`.

Non-Data values (`Null`, `Tombstone`, `Pointer`) pass through without encryption.
Key names are stored in plaintext (required for BTreeMap ordering and scans).

Using the wrong password to open an encrypted namespace will produce `Corruption`
errors on `get`/`rev_get` (the GCM tag verification fails).

### Listing and Dropping Namespaces

Two management methods live on `DB`:

| Method            | Signature                         | Description                              |
| ----------------- | --------------------------------- | ---------------------------------------- |
| `list_namespaces` | `&self -> Result<Vec<String>>`    | Sorted list of all known namespace names |
| `drop_namespace`  | `&self, name: &str -> Result<()>` | Remove a namespace and all its data      |

**`list_namespaces`** returns the sorted union of namespaces from the in-memory MemTable map and the L0
SSTable cache. This covers namespaces created at runtime, replayed from the AOL, and persisted in SSTables.

**`drop_namespace`** performs a complete removal:

1. Remove in-memory state — MemTable, L0 SSTable readers, object store, encryption tracking
2. Delete on-disk files — `<db>/sst/<namespace>/`, `<db>/objects/<namespace>/`, `<db>/crypto/<namespace>.salt`
3. Flush remaining namespaces and force-truncate the AOL to prevent the dropped namespace from reappearing
   on restart via AOL replay

Constraints:

- The default namespace `_` cannot be dropped → `InvalidNamespace` error
- Dropping a non-existent namespace → `InvalidNamespace` error
- **CLI**: dropping the current namespace auto-switches the prompt back to `_`

### Revision ID

A RevisionID is a 128-bit unsigned integer (`u128`) that uniquely identifies a mutation.
RevisionIDs are **system-wide unique** — generated by the rKV instance and shared across all namespaces. Every
successful `put` returns the new RevisionID to the caller. RevisionIDs are displayed as Crockford Base32 encoded
strings (e.g., `7z` for revision 255).

#### ULID-Like Layout

Each RevisionID is a compound ID with a ULID-like 48-16-16-48 bit layout:

```text
 MSB                                                              LSB
 [  timestamp 48  |  cluster 16  |  process 16  |  sequence 48  ]
```

| Field      | Bits   | Source                                  |
| ---------- | ------ | --------------------------------------- |
| timestamp  | 127–80 | `SystemTime::now()` ms since Unix epoch |
| cluster_id | 79–64  | `Config::cluster_id` (random if None)   |
| process_id | 63–48  | `std::process::id() as u16`             |
| sequence   | 47–0   | Random via `fastrand`                   |

The timestamp-first layout ensures natural time ordering. The cluster and process fields
disambiguate concurrent writers. The random sequence provides uniqueness within a millisecond.

**Per-key monotonicity**: The MemTable tracks the last RevisionID per key. If a generated
revision is `<=` the previous one for that key, it is bumped to `last_rev + 1`. This ensures
revisions are strictly increasing per key without requiring global ordering.

**Field accessors**: `timestamp_ms()`, `cluster_id()`, `process_id()`, `sequence()` extract
the individual fields from a RevisionID.

History queries use revision indexing: `rev_count(key)` returns the total number of revisions for a key, and
`rev_get(key, index)` retrieves the value at a specific revision index (0 = oldest). CAS operations use RevisionIDs for
optimistic concurrency control.

### TTL (Time-to-Live)

A key can be stored with an optional TTL via the consolidated `put(key, value, ttl)` method.
The `ttl` parameter is `Option<Duration>` — pass `Some(duration)` to set a TTL, or `None`
for a permanent key. After the duration elapses the key is considered expired and behaves as
if it were deleted — `get` returns "key not found", `exists` returns `false`. The `ttl(key)`
method returns the remaining duration, or `None` if the key has no expiration.

TTL is specified as a `std::time::Duration` in the library API. The CLI accepts human-friendly suffixes:
`10s` (seconds), `5m` (minutes), `2h` (hours), `1d` (days). Plain numbers are treated as seconds.

### Bulk Delete

Two methods on `Namespace` allow deleting multiple keys in a single call:

| Method          | Description                                        |
| --------------- | -------------------------------------------------- |
| `delete_range`  | Delete keys in `[start, end)` or `[start, end]`    |
| `delete_prefix` | Delete keys whose string form starts with `prefix` |

Signatures: `delete_range(&self, start, end, inclusive: bool) -> Result<u64>`,
`delete_prefix(&self, prefix: &str) -> Result<u64>`.

Both methods return the number of keys actually deleted. Tombstoned and expired keys are
excluded from the count. Each deleted key is individually tombstoned in the AOL for
crash-safe persistence, and the `op_deletes` counter is incremented by the batch count.

#### CLI: wipe Command

The `wipe` REPL command exposes bulk delete with three syntax forms:

| Syntax                 | Mode       | Example       | Semantics                            |
| ---------------------- | ---------- | ------------- | ------------------------------------ |
| `wipe <prefix>*`       | prefix     | `wipe user_*` | Delete keys starting with `user_`    |
| `wipe <start>..<end>`  | range (ex) | `wipe 1..10`  | Delete keys in `[1, 10)` (exclusive) |
| `wipe <start>..=<end>` | range (in) | `wipe 1..=10` | Delete keys in `[1, 10]` (inclusive) |

Constraints: both sides of a range are required; the prefix glob `*` must have at least
one character before it. Use `help wipe` for detailed usage.

### Configuration

The `Config` struct controls database behavior and LSM tuning parameters:

| Field               | Type          | Default    | Description                                |
| ------------------- | ------------- | ---------- | ------------------------------------------ |
| `path`              | `PathBuf`     | (required) | Database directory path                    |
| `create_if_missing` | `bool`        | `true`     | Create the directory if it doesn't exist   |
| `write_buffer_size` | `usize`       | 4 MB       | In-memory write buffer size before flush   |
| `max_levels`        | `usize`       | 3          | Maximum number of LSM levels               |
| `block_size`        | `usize`       | 4 KB       | SSTable block size                         |
| `cache_size`        | `usize`       | 8 MB       | Block cache size for decompressed blocks   |
| `object_size`       | `usize`       | 1 KB       | Bin object size threshold (see above)      |
| `compress`          | `bool`        | `true`     | LZ4-compress bin objects on disk           |
| `bloom_bits`        | `usize`       | 10         | Bloom filter bits per key (0 = disabled)   |
| `verify_checksums`  | `bool`        | `true`     | Verify checksums on read (see below)       |
| `compression`       | `Compression` | `LZ4`      | SSTable block compression (see above)      |
| `io_model`          | `IoModel`     | `Mmap`     | File I/O strategy (see I/O Modes below)    |
| `cluster_id`        | `Option<u16>` | `None`     | Cluster ID for RevisionID (random if None) |
| `aol_buffer_size`   | `usize`       | 128        | AOL flush threshold in records (0 = every) |
| `l0_max_count`      | `usize`       | 4          | Max L0 SSTable count before compaction     |
| `l0_max_size`       | `usize`       | 64 MB      | Max total L0 size before compaction        |
| `l1_max_size`       | `usize`       | 256 MB     | Max L1 size before merge to L2             |
| `default_max_size`  | `usize`       | 2 GB       | Default max size for L2+ levels            |
| `bloom_prefix_len`  | `usize`       | 0          | Prefix bloom filter length (0 = disabled)  |

The CLI uses dot-notation keys for `config <key> <value>`:

| Config field        | CLI key                     |
| ------------------- | --------------------------- |
| `path`              | `storage.path` (read-only)  |
| `create_if_missing` | `storage.create_if_missing` |
| `write_buffer_size` | `lsm.write_buffer_size`     |
| `max_levels`        | `lsm.max_levels`            |
| `block_size`        | `lsm.block_size`            |
| `cache_size`        | `lsm.cache_size`            |
| `bloom_bits`        | `lsm.bloom_bits`            |
| `verify_checksums`  | `lsm.verify_checksums`      |
| `compression`       | `lsm.compression`           |
| `object_size`       | `object.size`               |
| `compress`          | `object.compress`           |
| `io_model`          | `io.model`                  |
| `cluster_id`        | `revision.cluster_id`       |
| `aol_buffer_size`   | `aol.buffer_size`           |
| `l0_max_count`      | `lsm.l0_max_count`          |
| `l0_max_size`       | `lsm.l0_max_size`           |
| `l1_max_size`       | `lsm.l1_max_size`           |
| `default_max_size`  | `lsm.default_max_size`      |
| `bloom_prefix_len`  | `lsm.bloom_prefix_len`      |

`Config::new(path)` initializes all fields to their defaults. Fields can be overridden before
passing the config to `DB::open`.

### I/O Modes

The `io_model` field selects the file I/O strategy used by the storage engine. The three
modes are mutually exclusive:

| Mode       | Enum variant        | Description                                                       |
| ---------- | ------------------- | ----------------------------------------------------------------- |
| `none`     | `IoModel::None`     | Buffered I/O — all reads and writes go through the OS page cache. |
| `directio` | `IoModel::DirectIO` | Direct I/O — bypasses the OS page cache (O_DIRECT on Linux).      |
| `mmap`     | `IoModel::Mmap`     | Memory-mapped I/O — zero-copy reads via mmap. **(default)**       |

**Buffered** is the simplest strategy: the OS manages caching. It works well for
general workloads but gives the engine no control over eviction or write ordering.

**Direct I/O** is useful when the engine manages its own block cache and wants to avoid
double-caching (once in the engine cache, once in the OS page cache). This mode requires
aligned buffers and is more complex to implement.

**Mmap** maps data files directly into the process address space, enabling zero-copy reads.
It is the default because it provides excellent read throughput with minimal syscall
overhead. The trade-off is that write-heavy workloads may trigger unpredictable page faults
and the engine has less control over I/O scheduling.

**Stub status**: All three backends are defined but return `NotImplemented`. The actual
I/O logic will be implemented when the storage layer is built.

### Statistics

`db.stats()` returns a `Stats` snapshot with counters and metadata:

| Field                 | Type             | Source                 | Description                             |
| --------------------- | ---------------- | ---------------------- | --------------------------------------- |
| `total_keys`          | `u64`            | MemTable (live)        | Total number of live keys               |
| `data_size_bytes`     | `u64`            | MemTable (live)        | Approximate data size in bytes          |
| `namespace_count`     | `u64`            | MemTable map (live)    | Number of namespaces                    |
| `level_count`         | `usize`          | Config                 | Number of LSM levels (from config)      |
| `sstable_count`       | `u64`            | SSTables (live)        | Total SSTable files across all levels   |
| `write_buffer_bytes`  | `u64`            | MemTable (live)        | Current write buffer usage              |
| `pending_compactions` | `u64`            | SSTables (live)        | Levels exceeding compaction thresholds  |
| `level_stats`         | `Vec<LevelStat>` | SSTables (live)        | Per-level file count and size breakdown |
| `op_puts`             | `u64`            | AtomicU64 (persistent) | Cumulative put operations               |
| `op_gets`             | `u64`            | AtomicU64 (persistent) | Cumulative get operations               |
| `op_deletes`          | `u64`            | AtomicU64 (persistent) | Cumulative delete operations            |
| `cache_hits`          | `u64`            | BlockCache (live)      | Block cache hits                        |
| `cache_misses`        | `u64`            | BlockCache (live)      | Block cache misses                      |
| `uptime`              | `Duration`       | Instant (live)         | Time since `DB::open`                   |

`LevelStat` contains `file_count: u64` and `size_bytes: u64`. The `level_stats` vector has
`max_levels` entries; `level_stats[i]` aggregates all namespaces at level `i`.

`pending_compactions` counts L0 levels where file count >= `l0_max_count` or total size >=
`l0_max_size`, plus L1+ levels where total size >= the level's max size threshold.

`stats()` returns `Stats` directly (not `Result<Stats>`) — it cannot fail. Live fields are derived
from MemTable state on each call. Operation counters are tracked via `AtomicU64` (Relaxed ordering)
and persisted to `stats.meta` on `DB::close()` / `Drop`, so they accumulate across restarts.

#### Stats Persistence

Operation counters (`op_puts`, `op_gets`, `op_deletes`) are stored in a 30-byte binary file
`<db>/stats.meta`:

| Offset | Size | Field           |
| ------ | ---- | --------------- |
| 0      | 4    | Magic `rKVT`    |
| 4      | 2    | Version (BE)    |
| 6      | 8    | op_puts (BE)    |
| 14     | 8    | op_gets (BE)    |
| 22     | 8    | op_deletes (BE) |

Written atomically via write-to-temp + rename. Missing or malformed files default counters to zero.

#### Analyze Command

`db.analyze()` re-derives all statistics from current engine state and persists operation counters
to disk. In the CLI, the `analyze` command calls this method and prints the results. Useful as an
admin recovery tool when stats may have drifted.

### Maintenance Operations

Maintenance operations handle durability, recovery, backup, and storage optimization.
Most maintenance methods return `Result`. `flush`, `compact`, `list_namespaces`, and
`drop_namespace` are implemented; remaining methods are stubs (`NotImplemented`).

#### Flush / Sync

| Method  | Kind     | Signature             | Description                                 |
| ------- | -------- | --------------------- | ------------------------------------------- |
| `flush` | instance | `&self -> Result<()>` | Flush the in-memory write buffer to disk    |
| `sync`  | instance | `&self -> Result<()>` | Flush and fsync all data to durable storage |

`flush` drains every non-empty namespace MemTable and writes an L0 SSTable per namespace.
After all SSTables are written, the AOL is truncated back to a header-only state. The flush
path is:

```text
DB::flush()
  for each namespace with non-empty MemTable:
    1. drain_latest() — extract latest value per key (sorted, includes tombstones)
    2. SSTableWriter::add() each entry in key order
    3. SSTableWriter::finish() — write index + footer
    4. SSTableReader::open() — cache reader (newest first)
  truncate AOL
```

SSTable files are stored at `<db>/sst/<namespace>/L<level>/<seq>.sst` where `<level>` is
the LSM level (0, 1, ...) and `<seq>` is a zero-padded monotonically increasing counter
(e.g., `000001.sst`). On `DB::open()`, the engine scans these directories to recover the
reader cache and sequence counter across all levels.

`sync` flushes any buffered AOL writes and calls `fsync` on the AOL file descriptor,
guaranteeing that all committed data is persisted to the storage device.

**Limitations (V1)**:

- TTL is not preserved across flush — keys with TTL become permanent once flushed
- `scan`, `rscan`, `count`, and `exists` only check the MemTable (not SSTables)
- Revision history is not flushed — only the latest value per key is written

#### Destroy / Repair

| Method    | Kind   | Signature                          | Description                  |
| --------- | ------ | ---------------------------------- | ---------------------------- |
| `destroy` | static | `(path) -> Result<()>`             | Delete database and all data |
| `repair`  | static | `(path) -> Result<RecoveryReport>` | Repair a corrupted database  |

Both are static methods — they operate on a path, not a live `DB` handle.

**Destroy** validates the path contains an rKV signature (`aol` file or `sst/` directory) before
removing the entire directory tree. Returns an I/O error if the path does not exist, or a
`Corruption` error if the directory does not look like an rKV database.

**Repair** performs an offline scan of three data sources:

1. **AOL**: Replays with checksum verification. Corrupted/truncated records are skipped. If any
   records were skipped, the AOL is rewritten with only valid records.
2. **SSTables**: Opens each `.sst` file and verifies block checksums via `iter_entries(true)`.
   Corrupted files are deleted.
3. **Bin objects**: Reads each object with BLAKE3 hash verification. Corrupted objects are deleted.

Returns a `RecoveryReport` describing what was scanned, recovered, and lost (see Data Integrity
below). The database is openable after repair.

#### Dump / Load

| Method | Kind     | Signature                                       | Description                               |
| ------ | -------- | ----------------------------------------------- | ----------------------------------------- |
| `dump` | instance | `&self, path: impl Into<PathBuf> -> Result<()>` | Export database to a portable backup file |
| `load` | static   | `(path: impl Into<PathBuf>) -> Result<DB>`      | Import database from a backup file        |

`dump` flushes all in-memory write buffers, merges SSTable levels per namespace
(same strategy as compaction), filters tombstones, resolves `Pointer` values to
inline `Data`, and writes each entry to the dump file with a CRC32C checksum.
Encrypted namespaces are skipped (v1 limitation).

`load` reads the dump file, creates a fresh DB at the stored path, replays all
records via `namespace.put()`, and flushes. Returns `InvalidConfig` if the target
path already contains data.

`load` is not exposed in the CLI because it would require replacing the live DB
handle mid-session.

##### Dump File Format

```text
Header:
  [magic: 4B "rKVD"]  [version: 2B BE]
  [path_len: 2B BE]   [path: UTF-8 bytes]

Records (repeating):
  [payload_len: 4B BE] [payload] [checksum: 5B CRC32C]

Payload:
  [ns_len: 2B BE]  [namespace]
  [key_len: 2B BE] [key_bytes]
  [value_tag: 1B]  [value_data_len: 4B BE] [value_data]
  [expires_at_ms: 8B BE]

EOF sentinel:
  [payload_len: 4B = 0x00000000]
```

The format mirrors the AOL record layout for consistency. Each record is
self-describing and independently verifiable via its checksum.

#### Compaction

| Method                | Kind     | Signature             | Description                                  |
| --------------------- | -------- | --------------------- | -------------------------------------------- |
| `compact`             | instance | `&self -> Result<()>` | Trigger manual compaction of SSTable levels  |
| `wait_for_compaction` | instance | `&self`               | Block until background compaction cycle done |

`compact` merges L0 SSTables into L1, then cascades through deeper levels when a
level exceeds its size threshold. The merge processes entries oldest-to-newest so that
newer values overwrite older ones. Old source files are deleted after a successful merge.

The compaction path per namespace:

```text
DB::compact()
  for each namespace with L0 SSTables:
    1. Merge L0 + L1 → new L1 SSTable
    2. For level in 1..max_levels-1:
         if level_total_size <= level_max_size: stop
         Merge level + (level+1) → new (level+1) SSTable
         Drop tombstones if target is the bottommost level
```

Level size thresholds:

| Level | Threshold          | Config field       |
| ----- | ------------------ | ------------------ |
| L0    | n/a (count-based)  | `l0_max_count`     |
| L1    | `l1_max_size`      | `l1_max_size`      |
| L2+   | `default_max_size` | `default_max_size` |

Tombstones are preserved at intermediate levels because they may shadow data in
deeper levels. At the bottommost level (`max_levels - 1`), tombstones are dropped
because no deeper level exists to shadow.

Compaction is idempotent — calling it when L0 is empty is a no-op. After compaction,
new flushes continue writing to L0 and a subsequent compact merges them into L1 again.
When `max_levels` is 1, compaction is a no-op (no merge target available).

##### Auto-Compaction

After each `flush()`, the engine signals the background compaction thread.
The thread checks whether any namespace's L0 level exceeds the configured
thresholds (`l0_max_count` or `l0_max_size`). If either threshold is met,
compaction runs automatically. This eliminates the need for manual compaction
in typical workloads while still allowing explicit `compact()` calls.

##### Background Compaction Thread

Compaction runs on a dedicated background thread, keeping `flush()` and
read/write paths non-blocking. The thread uses a Condvar-based signaling
mechanism:

1. **Signal**: Every `flush()` sets a pending flag and wakes the thread.
2. **Drain loop**: The thread compacts repeatedly until all levels are
   within their thresholds, then sleeps. This prevents L0 pile-up under
   sustained write workloads.
3. **Safety-net poll**: The thread also wakes every 30 seconds to catch
   any missed signals.

Manual `compact()` calls serialize with the background thread via a shared
mutex — both paths use the same static compaction helpers, so behavior is
identical.

`wait_for_compaction()` signals the thread and blocks until its current
cycle completes. This is intended for deterministic testing — production
callers should not need it.

Shutdown (`close()` / `Drop`) sets a stop flag, wakes the thread, and
joins it, ensuring all in-progress compaction finishes before the `DB`
handle is released.

##### Bin Object GC

After all level merges complete for a namespace, compaction runs a
garbage-collection sweep over the bin object store:

1. Collect all live `ValuePointer` hashes from every surviving SSTable.
2. Walk `<db>/objects/<namespace>/` and list all object files on disk.
3. Delete any object whose hash is not in the live set.

This handles overwrites (old Pointer orphaned), tombstones (shadowed
Pointer orphaned), and dedup safely (an object is kept as long as at
least one SSTable entry still references it).

The CLI exposes compaction via the `compact` REPL command.

### Data Integrity

Every WAL entry and SSTable block carries a CRC32C checksum. On write the engine computes
the checksum over the raw data; on read the engine recomputes and compares to detect
corruption caused by bit rot, partial writes, or disk errors.

Bin objects use BLAKE3 content hashes via `ValuePointer` — a separate, complementary
integrity mechanism (see Value Separation above).

#### Checksum Format

Each checksum is 5 bytes on disk:

| Field   | Type  | Bytes | Description                     |
| ------- | ----- | ----- | ------------------------------- |
| `algo`  | `u8`  | 1     | Algorithm tag (`0x01` = CRC32C) |
| `value` | `u32` | 4     | Big-endian checksum value       |

The algorithm tag allows future extension to stronger checksums without breaking
existing data files.

#### Read-Time Verification

When `verify_checksums` is enabled (default: `true`), every block and WAL entry read
from disk is verified against its stored checksum. A mismatch produces a `Corruption`
error. Disabling verification trades safety for read speed — useful for bulk scans
where occasional corruption is acceptable.

#### Offline Recovery

`DB::repair(path)` performs an offline scan of a database directory and returns a
`RecoveryReport`:

| Field                      | Type          | Description                                  |
| -------------------------- | ------------- | -------------------------------------------- |
| `wal_records_scanned`      | `u64`         | WAL records examined                         |
| `wal_records_skipped`      | `u64`         | WAL records skipped due to checksum mismatch |
| `sstable_blocks_scanned`   | `u64`         | SSTable blocks examined                      |
| `sstable_blocks_corrupted` | `u64`         | SSTable blocks with checksum mismatch        |
| `objects_scanned`          | `u64`         | Bin objects examined                         |
| `objects_corrupted`        | `u64`         | Bin objects with hash mismatch               |
| `keys_recovered`           | `u64`         | Keys recovered from redundant sources        |
| `keys_lost`                | `u64`         | Keys permanently lost (no redundant copy)    |
| `warnings`                 | `Vec<String>` | Human-readable warnings from the repair pass |

Helper methods on `RecoveryReport`:

- `is_clean()` — all corruption counters are zero.
- `total_corrupted()` — sum of skipped + corrupted counters.
- `has_data_loss()` — `keys_lost > 0`.

Recovery is best-effort: the engine replays valid WAL entries, removes corrupted SSTable
files, and deletes bin objects that fail BLAKE3 verification. The AOL is rewritten to
exclude corrupted records. After repair, the database can be reopened normally. Silent
self-healing from bit-flips is not possible without redundancy and is out of scope.

### LSM-Tree Storage

Data is organized in levels. Fresh writes land in an in-memory buffer (MemTable) and are
flushed to sorted L0 SSTable files on disk via `DB::flush()`. `DB::compact()` merges L0
files into a single L1 SSTable. The read path checks the MemTable first, then searches
SSTables from newest to oldest across all levels.

#### Block Compression

SSTable data blocks can be compressed to reduce disk usage and I/O bandwidth. The `compression`
config field selects the algorithm applied when blocks are flushed to disk:

| Algorithm | Enum variant        | Characteristics                            |
| --------- | ------------------- | ------------------------------------------ |
| `none`    | `Compression::None` | No compression — lowest CPU, largest files |
| `lz4`     | `Compression::LZ4`  | Fast with moderate ratio **(default)**     |
| `zstd`    | `Compression::Zstd` | Better ratio, higher CPU cost              |

Compression is applied per block at flush time and reversed on read. The block cache stores
**decompressed** blocks, so the CPU cost is paid once per cache miss, not per read.

Block compression is independent of bin object compression (`compress` config field), which
controls LZ4 compression of large values in the object store.

#### LRU Block Cache

An LRU (Least Recently Used) block cache stores decompressed and parsed SSTable data blocks
in memory, keyed by `(sst_id, block_index)`. This avoids redundant decompression and parsing
when the same block is read repeatedly (e.g., hot keys, repeated scans).

The `cache_size` config field controls the total byte budget (default 8 MB). Set to `0` to
disable the cache entirely — all operations remain functionally correct but may be slower
for workloads with repeated block access.

**Behavior:**

- **Lookup**: On each `read_block()` call, the cache is checked first. A hit returns a clone
  of the parsed entries and promotes the block to MRU (most recently used) position.
- **Insert**: On a cache miss, after decompression and parsing, the block is inserted into the
  cache. If the cache exceeds its capacity, LRU entries are evicted until the budget is met.
  Blocks larger than the total capacity are silently skipped to prevent thrashing.
- **Compaction eviction**: When SSTables are merged during compaction, all cached blocks for
  the old (replaced) SSTables are evicted, freeing memory for the new merged SSTable's blocks.
- **Restart**: The cache is in-memory only. On `DB::open()`, the cache starts empty and warms
  up naturally through reads.

**Size estimation** per cached block: `64 + Σ(key_bytes.len() + 1 + value_data.len() + 48)`
bytes, where the sum runs over all entries in the block.

#### SSTable File Format

An SSTable is a read-only file of sorted key-value entries. The file is divided into three
regions written sequentially: data blocks, an index block, and a fixed-size footer.

```text
┌──────────────────────────────┐
│  Data Block 0                │  ← compressed entries + checksum
│  Data Block 1                │
│  ...                         │
│  Data Block N                │
├──────────────────────────────┤
│  Index Block                 │  ← one entry per data block
├──────────────────────────────┤
│  Footer (48 bytes)           │  ← magic, version, metadata, checksum
└──────────────────────────────┘
```

**Data block on-disk layout:**

```text
[compression_tag: u8][compressed_payload][checksum: 5B CRC32C]
```

The `compression_tag` identifies the algorithm (0x00 = none, 0x01 = LZ4, 0x02 = Zstd).
The checksum covers the tag byte plus the compressed payload.

**Entry encoding (within a decompressed block):**

```text
[key_len: u16 BE][key_bytes][value_tag: u8][value_len: u32 BE][value_data]
```

Entries are stored in sorted key order. `key_bytes` uses the same memcmp-preserving
serialization as `Key::to_bytes()`. `value_tag` encodes the Value variant (0x00 = Data,
0x01 = Null, 0x02 = Tombstone, 0x03 = Pointer).

**Index block layout:**

```text
repeated: [key_len: u16 BE][last_key_bytes][offset: u64 BE][size: u32 BE]
```

Each entry records the last key in a data block plus the block's file offset and on-disk
size. Point lookups binary-search the index to find the candidate block, then linear-scan
entries within that block.

**Footer layout (48 bytes):**

```text
[magic: 4B "rKVS"][version: u16 BE][entry_count: u64 BE]
[index_offset: u64 BE][index_size: u32 BE]
[data_blocks: u32 BE][reserved: 13B][checksum: 5B CRC32C]
```

The footer checksum covers the first 43 bytes. The reader verifies magic, version, and
checksum before parsing the index.

#### SSTable Read Path

Point lookups (`get`) check the MemTable first, then search SSTables level by level
(L0 newest-first, then L1, L2, ...). The first match wins:

```text
Namespace::get(key)
  1. MemTable lookup — if found, return value
  2. For each level (L0, L1, L2, ...):
     For each SSTable in the level (L0: newest first; L1+: ascending):
       a. Bloom filter check — if key is definitely absent, skip SSTable
       b. Binary search index for candidate block
       c. Decompress + verify checksum
       d. Linear scan entries for key
       e. If found:
          - Tombstone → return KeyNotFound
          - Pointer → resolve via ObjectStore
          - Data/Null → return value
  3. Not found in any SSTable → return KeyNotFound
```

#### Merged Scan

`scan` and `rscan` query across both the MemTable and all SSTable levels to produce
a unified, deduplicated result. This ensures that data remains visible after `flush()`
and `compact()` — not just for point lookups, but for prefix/range iteration too.

**Merge strategy** (oldest-to-newest, newest wins):

```text
Namespace::scan(prefix, limit, offset)
  1. Collect SSTable entries matching prefix (merge order below)
  2. Collect MemTable raw entries matching prefix (includes tombstones)
  3. Insert all into BTreeMap<Key, Value> — newer entries overwrite older
  4. Filter out tombstones from merged result
  5. Apply offset, then limit
  6. For rscan: reverse iteration order before offset/limit
```

**SSTable merge order** ensures newer values shadow older ones:

1. Deepest level first (L_max, L_max-1, ..., L1) — ascending key order within each
2. L0: oldest SSTable first → newest SSTable last (so newest overwrites oldest)
3. MemTable entries inserted last (always win over any SSTable)

**Scan modes**:

- **Ordered mode** (Int keys): Uses the block index to find the starting block, then
  reads forward (scan) or backward (rscan) from the prefix key. Stops when keys move
  out of range.
- **Unordered mode** (Str keys): Serializes the prefix using `Key::to_prefix_bytes()`
  (omitting the trailing null terminator), then checks each SSTable entry with
  `starts_with`. All matching blocks must be read.

**Tombstone shadowing**: MemTable raw entries include tombstones. When a MemTable
tombstone overwrites an SSTable value in the merge map, the tombstone is filtered out
in step 4 — correctly hiding the deleted key from the final result.

#### Bloom Filters

Each SSTable embeds a bloom filter that enables skipping SSTables during point lookups.
The filter is built from all keys at flush/compaction time and serialized into the SSTable
file between the data blocks and the index block.

**Hash function**: LevelDB-compatible murmur-inspired 32-bit hash with double-hashing
probe strategy (`h.rotate_left(15)` per probe). The number of hash probes is computed as
`k = ln(2) * bits_per_key`, clamped to `[1, 30]`.

**Serialization**: `[num_hashes: u8][bit_array...]`. Stored in the SSTable footer via
`filter_offset` and `filter_size` fields (formerly reserved bytes). Old SSTables with
`filter_size = 0` are backwards compatible — `may_contain()` returns `true`.

**Configuration**: `bloom_bits` (default 10) controls the bits-per-key. At 10 bits/key,
the false-positive rate is approximately 1%. Set to 0 to disable bloom filters entirely.

On `DB::open()`, the engine scans `<db>/sst/<namespace>/L<n>/` directories and opens all
`.sst` files into an in-memory reader cache. L0 readers are ordered newest-first; L1+
readers are ordered by ascending sequence number.

#### Prefix Bloom Filter

In addition to the per-key bloom filter used for point lookups, each SSTable can embed
a **prefix bloom filter** that accelerates scan operations. During scan, the prefix bloom
allows skipping SSTables that definitely contain no keys matching the query prefix.

**Configuration**: `bloom_prefix_len` (default 0 = disabled). When > 0, the first
`bloom_prefix_len` bytes of each key's serialized form (`Key::to_bytes()`) are hashed into
a second bloom filter at flush/compaction time. On scan, the query prefix is truncated to
`bloom_prefix_len` bytes before checking the filter.

**Key prefix semantics**:

- Str keys: prefix bytes are `[0x02][first N-1 chars]` (the tag byte plus string bytes)
- Int keys: all share the same tag byte `0x01`, so the prefix bloom is less selective for
  Int-heavy workloads but still harmless

**Compound filter format**: To store both blooms in a single filter block, the SSTable uses
a compound format identified by a footer byte:

| Format byte | Layout                                                                     |
| ----------- | -------------------------------------------------------------------------- |
| `0x00`      | Legacy — filter block contains key bloom only                              |
| `0x01`      | Compound — `[key_bloom_len: u32 LE][key_bloom][prefix_len: u8][pfx_bloom]` |

Old SSTables with format byte `0x00` are backwards compatible — `may_contain_prefix()`
returns `true` (no prefix bloom available, so no skipping).

**Sizing**: The prefix bloom uses the same `bloom_bits` (bits-per-key) setting as the key
bloom. For workloads with many distinct prefixes, 10 bits/key provides ~1% false-positive
rate on prefix checks.

#### WriteBuffer (MemTable)

The WriteBuffer is the first component in the write path. It is an in-memory sorted store
backed by a `BTreeMap<Key, Vec<MemEntry>>` — each key maps to its full revision history
(oldest entry at index 0). The MemTable provides:

- **put/get/delete/exists** — core key-value operations
- **scan/rscan** — ordered iteration with offset/limit pagination (range queries in ordered mode,
  prefix matching in unordered mode)
- **count** — live key count (excludes tombstones and expired entries)
- **rev_count/rev_get** — revision history access
- **ttl** — remaining time-to-live for a key
- **auto-upgrade** — when the first `Str` key is inserted, all existing `Int` keys are widened to `Str`

Each namespace has its own independent MemTable. The `DB` struct holds a
`RwLock<HashMap<String, Mutex<MemTable>>>` for per-namespace memtables, created lazily
on first access. A shared `RevisionGen` produces candidate RevisionIDs; individual MemTables
enforce per-key monotonicity.

**Current status**: The MemTable serves as the write buffer. On startup, the AOL is replayed
to reconstruct memtable state (see Append-Only Log below). `DB::flush()` calls
`drain_latest()` to extract the latest value per key in sorted order, writes an L0 SSTable,
and truncates the AOL. After flush, the MemTable is empty and ready for new writes.

#### Append-Only Log (AOL)

The AOL is the durability layer in the write path. Every mutation is appended to the AOL
**before** being applied to the MemTable. On crash recovery, `DB::open()` replays the AOL
to reconstruct the in-memory state.

**Write path**: `Client API -> AOL (append + flush) -> MemTable -> Response`

##### AOL File Format

The AOL is a single file (`aol`) in the database directory. It begins with an 8-byte header
followed by a sequence of variable-length records.

**Header (8 bytes, written once)**:

| Offset | Size | Field    | Value                         |
| ------ | ---- | -------- | ----------------------------- |
| 0      | 4    | magic    | `0x724B564C` (ASCII `"rKVL"`) |
| 4      | 2    | version  | `0x0001` (u16 BE)             |
| 6      | 2    | reserved | `0x0000`                      |

**Record layout (repeated)**:

```text
[payload_len: u32 BE (4B)] [payload: var] [checksum: 5B]
```

- `payload_len`: byte count of the payload (excludes length prefix and checksum)
- `checksum`: CRC32C over the payload bytes (`Checksum::to_bytes()` format)
- Total overhead per record: 9 bytes

**Payload layout**:

```text
[ns_len: u16 BE] [namespace: ns_len bytes] [revision: u128 BE (16B)]
[expires_at_ms: u64 BE] [key_len: u16 BE] [key_bytes: key_len bytes]
[value_tag: u8] [value_data: remaining bytes]
```

- `revision`: candidate RevisionID from `RevisionGen` (MemTable enforces per-key monotonicity on replay)
- `expires_at_ms`: absolute expiry as ms since Unix epoch (0 = no expiry)
- `value_tag`: `0x00` = Data, `0x01` = Null, `0x02` = Tombstone, `0x03` = Pointer
- `value_data`: present for Data (raw bytes) and Pointer (36-byte `ValuePointer`); empty for Null/Tombstone

##### TTL Encoding

TTL is stored as an **absolute timestamp** (ms since Unix epoch) rather than a relative
duration. This ensures correct expiry semantics on replay — if a key was set to expire at
time T, it expires at time T regardless of when the database is reopened. Expired records
are skipped during replay.

##### Replay Semantics

On `DB::open()`, the engine replays the AOL sequentially:

1. Skip records where `expires_at_ms > 0` and `expires_at_ms <= now`
2. For surviving records, get-or-create the namespace's MemTable
3. Feed each record through `MemTable::put()` with the stored revision and remaining TTL
4. Per-key monotonicity is enforced by the MemTable (candidate revisions may be bumped)

Truncated or corrupted records at the tail of the file are silently skipped (counted in the
skip counter). This handles partial writes from crashes during append.

##### Limitations

- **No truncation**: The AOL grows without bound until flush/compaction is implemented.
  Once SSTable flushing lands, the AOL will be truncated after a successful flush.
- **Buffered flush**: The AOL buffers up to `aol_buffer_size` records (default 128) before
  flushing to the OS. A background thread flushes every 60 s if dirty data exists. On a
  hard crash, up to `aol_buffer_size` records (or 60 s of writes) may be lost. Set to 0
  for per-record flush (maximum durability). `DB::close()` always flushes remaining data.
- **No fsync on every write**: The implementation flushes the userspace buffer but does not
  call `fsync` per record. A future `sync_mode` config option will control this.

### Embeddable Library

The engine is a Rust library crate (`rkv`) that can be linked into any Rust program. The `rkv-ffi`
crate builds a C-compatible shared library (`cdylib`) with the following exported functions:

| Function          | Signature                                         | Description                   |
| ----------------- | ------------------------------------------------- | ----------------------------- |
| `rkv_open`        | `(path) -> *RkvDb`                                | Open database                 |
| `rkv_close`       | `(*RkvDb)`                                        | Close and free handle         |
| `rkv_put`         | `(*RkvDb, key, val) -> u128`                      | Put in default namespace      |
| `rkv_get`         | `(*RkvDb, key, out) -> i32`                       | Get from default namespace    |
| `rkv_delete`      | `(*RkvDb, key) -> i32`                            | Delete from default namespace |
| `rkv_put_ns`      | `(*RkvDb, ns, key, val, ttl) -> u128`             | Put with namespace and TTL    |
| `rkv_get_ns`      | `(*RkvDb, ns, key, out) -> i32`                   | Get with namespace            |
| `rkv_delete_ns`   | `(*RkvDb, ns, key) -> i32`                        | Delete with namespace         |
| `rkv_exists`      | `(*RkvDb, ns, key) -> i32`                        | Check key existence (1/0/-1)  |
| `rkv_ttl`         | `(*RkvDb, ns, key) -> i64`                        | Remaining TTL in ms           |
| `rkv_scan`        | `(*RkvDb, ns, prefix, limit, offset, out) -> i32` | Forward key scan              |
| `rkv_rscan`       | `(*RkvDb, ns, prefix, limit, offset, out) -> i32` | Reverse key scan              |
| `rkv_rev_count`   | `(*RkvDb, ns, key) -> i64`                        | Revision count                |
| `rkv_rev_get`     | `(*RkvDb, ns, key, index, out) -> i32`            | Get revision by index         |
| `rkv_flush`       | `(*RkvDb) -> i32`                                 | Flush memtables               |
| `rkv_compact`     | `(*RkvDb) -> i32`                                 | Trigger compaction            |
| `rkv_sync`        | `(*RkvDb) -> i32`                                 | Durable sync                  |
| `rkv_stats`       | `(*RkvDb) -> *char`                               | Stats as JSON string          |
| `rkv_free`        | `(*u8, len)`                                      | Free byte buffer              |
| `rkv_free_string` | `(*char)`                                         | Free CString buffer           |
| `rkv_last_error`  | `(buf, len) -> i32`                               | Copy last error message       |

All functions use `ns = NULL` for the default namespace. Errors are reported via
`rkv_last_error`. Scan results are encoded as length-prefixed key entries
(`[key_len: 4B LE][key_bytes]...`).

### CLI Tool

A REPL binary built on top of the library provides interactive access for debugging, exploration, and scripting.

#### File Input Syntax

The `put` command supports loading values from files:

- `put mykey @/path/to/file` — reads the file contents as the value (binary-safe)
- `put mykey @@literal` — escape: stores the literal string `@literal`

### HTTP Server

The optional HTTP server (`rkv serve`) exposes rKV as a networked JSON-over-HTTP service, enabling non-Rust clients
(browsers, scripts, microservices) to interact with the database without linking the library or FFI layer.

The server is built with Axum and gated behind the `server` Cargo feature flag (`--features server`). It is not
included in the default build to keep the core library dependency-free.

The REST API is namespace-aware: every data operation targets a specific namespace in its URL path. Supported
operations include key-value CRUD (`get`, `put`, `delete`), prefix scans with pagination, revision history queries,
TTL management, and administrative actions (flush, compact, stats). Custom response headers carry metadata such as
revision IDs and TTL information.

**Security**: By default the server binds to `127.0.0.1` (loopback only) and rejects requests from non-local IPs.
For network-accessible deployments, an IP allow-list (`--allow-ip`) restricts API access to explicitly trusted
addresses. Health endpoints (`/health`) are exempt from IP filtering so load balancers can probe without
authorization.

An embedded single-page web UI is available via the `--ui` flag. When enabled, the server serves a browser-based
dashboard at `/ui` for browsing keys, managing namespaces, and viewing database statistics — useful for debugging
and exploration without curl.

See the [README](README.md#http-server) for startup examples and curl recipes.

## Design Decisions

- **Interior mutability**: `DB` is `Send + Sync`. All mutable fields use `Mutex<T>` or `RwLock<T>` so public methods
  take `&self`.
- **Stub-first development**: The initial scaffold returns `NotImplemented` for all engine methods, allowing the CLI
  and test harness to be built before any storage logic exists.
- **Binary/library boundary**: The CLI (`main.rs`) is strictly binary-only. `lib.rs` exports only engine types
  (`DB`, `Config`, `Error`, `Result`). Nothing from the REPL leaks into the library or FFI surface.
