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
`DB` directly. `DB` is responsible for lifecycle (`open`, `close`, `path`) and namespace management (`namespace`).

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

| Field                 | Type       | Source                 | Description                           |
| --------------------- | ---------- | ---------------------- | ------------------------------------- |
| `total_keys`          | `u64`      | MemTable (live)        | Total number of live keys             |
| `data_size_bytes`     | `u64`      | MemTable (live)        | Approximate data size in bytes        |
| `namespace_count`     | `u64`      | MemTable map (live)    | Number of namespaces                  |
| `level_count`         | `usize`    | Config                 | Number of LSM levels (from config)    |
| `sstable_count`       | `u64`      | Stub (0)               | Total SSTable files across all levels |
| `write_buffer_bytes`  | `u64`      | MemTable (live)        | Current write buffer usage            |
| `pending_compactions` | `u64`      | Stub (0)               | Pending compaction tasks              |
| `op_puts`             | `u64`      | AtomicU64 (persistent) | Cumulative put operations             |
| `op_gets`             | `u64`      | AtomicU64 (persistent) | Cumulative get operations             |
| `op_deletes`          | `u64`      | AtomicU64 (persistent) | Cumulative delete operations          |
| `cache_hits`          | `u64`      | Stub (0)               | Block cache hits                      |
| `cache_misses`        | `u64`      | Stub (0)               | Block cache misses                    |
| `uptime`              | `Duration` | Instant (live)         | Time since `DB::open`                 |

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

Maintenance operations handle durability, recovery, backup, and storage optimization. All
maintenance methods return `Result` and are stubs (`NotImplemented`) during the scaffold phase.

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

L0 SSTable files are stored at `<db>/sst/<namespace>/<seq>.sst` where `<seq>` is a
zero-padded monotonically increasing counter (e.g., `000001.sst`). On `DB::open()`, the
engine scans these directories to recover the L0 reader cache and sequence counter.

`sync` calls `flush` followed by `fsync`, ensuring all data reaches durable storage
(currently a stub).

**Limitations (V1)**:

- TTL is not preserved across flush — keys with TTL become permanent once flushed
- `scan`, `rscan`, `count`, and `exists` only check the MemTable (not SSTables)
- Revision history is not flushed — only the latest value per key is written

#### Destroy / Repair

| Method    | Kind   | Signature                          | Description                  |
| --------- | ------ | ---------------------------------- | ---------------------------- |
| `destroy` | static | `(path) -> Result<()>`             | Delete database and all data |
| `repair`  | static | `(path) -> Result<RecoveryReport>` | Repair a corrupted database  |

Both are static methods — they operate on a path, not a live `DB` handle. `destroy` removes the
entire database directory. `repair` scans for structural corruption and rebuilds indices where
possible. It returns a `RecoveryReport` describing what was scanned, recovered, and lost
(see Data Integrity below).

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

Recovery is best-effort: the engine replays WAL entries and cross-references SSTable
levels to rebuild data where redundant copies exist. Data with no redundant copy is
reported as lost. Silent self-healing from bit-flips is not possible without redundancy
and is out of scope.

### LSM-Tree Storage

Data is organized in levels. Fresh writes land in an in-memory buffer (MemTable) and are
flushed to sorted L0 SSTable files on disk via `DB::flush()`. The read path checks the
MemTable first, then searches L0 SSTables from newest to oldest. Background merge
compaction (L0 to deeper levels) is planned but not yet implemented.

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

Point lookups (`get`) check the MemTable first, then search L0 SSTables from newest to
oldest. The first match wins:

```text
Namespace::get(key)
  1. MemTable lookup — if found, return value
  2. For each L0 SSTable (newest first):
     a. Binary search index for candidate block
     b. Decompress + verify checksum
     c. Linear scan entries for key
     d. If found:
        - Tombstone → return KeyNotFound
        - Pointer → resolve via ObjectStore
        - Data/Null → return value
  3. Not found in any SSTable → return KeyNotFound
```

On `DB::open()`, the engine scans `<db>/sst/<namespace>/` directories and opens all
`.sst` files into an in-memory reader cache. Readers are ordered newest-first based on
the sequence number in the filename.

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

The engine is a Rust library crate (`rkv`) that can be linked into any Rust program. FFI bindings expose the same API
to C, Python, and Go consumers.

### CLI Tool

A REPL binary built on top of the library provides interactive access for debugging, exploration, and scripting.

#### File Input Syntax

The `put` command supports loading values from files:

- `put mykey @/path/to/file` — reads the file contents as the value (binary-safe)
- `put mykey @@literal` — escape: stores the literal string `@literal`

## Design Decisions

- **Interior mutability**: `DB` is `Send + Sync`. All mutable fields use `Mutex<T>` or `RwLock<T>` so public methods
  take `&self`.
- **Stub-first development**: The initial scaffold returns `NotImplemented` for all engine methods, allowing the CLI
  and test harness to be built before any storage logic exists.
- **Binary/library boundary**: The CLI (`main.rs`) is strictly binary-only. `lib.rs` exports only engine types
  (`DB`, `Config`, `Error`, `Result`). Nothing from the REPL leaks into the library or FFI surface.
