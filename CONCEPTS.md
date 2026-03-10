# Concepts

## Overview

**rKV** is a persistent, revision-aware key-value store built on LSM-tree architecture. It is designed to be embedded
directly into Rust applications, accessed through FFI bindings (C/Python/Go), or used as a standalone CLI tool.

Every write produces a new **revision**, enabling history queries without external coordination.

## Architecture

### System Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                          CLIENT LAYER                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐   │
│  │ CLI REPL │  │ HTTP API │  │ FFI (C)  │  │ Rust Library API   │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬───────────┘   │
└───────┼──────────────┼─────────────┼─────────────────┼──────────────┘
        └──────────────┴─────────────┴─────────────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────────┐
│                          DB (Send+Sync)                             │
│  ┌────────────────────────────┼──────────────────────────────────┐  │
│  │                    Namespace Router                           │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                        │  │
│  │  │ default │  │  ns_a   │  │  ns_b   │  ...                   │  │
│  │  │ (plain) │  │(encrypt)│  │ (plain) │                        │  │
│  │  └────┬────┘  └────┬────┘  └────┬────┘                        │  │
│  └───────┼────────────┼────────────┼─────────────────────────────┘  │
│          │            │            │                                │
│  ┌───────┼────────────┼────────────┼─────────────────────────────┐  │
│  │       └────────────┴────────────┘                             │  │
│  │                Per-Namespace State                            │  │
│  │  ┌──────────────┐  ┌───────────────┐  ┌────────────────────┐  │  │
│  │  │  Mutex<AOL>  │  │Mutex<MemTable>│  │  ObjectStore (bin) │  │  │
│  │  │  (WAL file)  │  │  (BTreeMap)   │  │  (content-addr)    │  │  │
│  │  └──────────────┘  └───────────────┘  └────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    Shared Components                          │  │
│  │  ┌────────────┐  ┌────────────────┐  ┌─────────────────────┐  │  │
│  │  │RevisionGen │  │ ShardedCache   │  │ BG Compaction Thread│  │  │
│  │  │(ULID-like) │  │ (16-way LRU)   │  │ (Condvar-signaled)  │  │  │
│  │  └────────────┘  └────────────────┘  └─────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow (Write)

```text
put(key, value, ttl)
      │
      ▼
┌─────────────┐
│  Namespace  │──── encrypt(value) if password set
└──────┬──────┘
       │
       ▼
┌───────────────┐     value > object_size?
│ Value Separate├──── YES ──► ObjectStore.put(blake3, lz4(value))
│               │              returns ValuePointer(hash, size)
└──────┬────────┘
       │ NO (or ValuePointer)
       ▼
┌──────────────┐     [payload_len:4][key|rev|val|ttl][crc32c:5]
│  AOL Append  │──── Crash-safe: buffered write, periodic fsync
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   MemTable   │──── BTreeMap<Key, Vec<MemEntry>>
│   Insert     │     Enforces per-key revision monotonicity
└──────┬───────┘
       │
       ▼
approximate_size > write_buffer_size?
       │
  YES ──► Signal BG compaction thread (Condvar)
                  │
                  ▼
            ┌──────────┐
            │  Flush   │──► MemTable → SSTable L0
            └────┬─────┘    Truncate AOL
                 │
                 ▼
            ┌──────────┐
            │ Compact  │──► L0→L1→L2→... (size-tiered cascade)
            └──────────┘    Trivial move if target level empty
```

### Summary

- **Write path**: Client -> AOL (append-only log, fsync for durability) -> WriteBuffer (in-memory) -> respond to
  caller. Background flush moves WriteBuffer to L0 SSTable; merge compacts L0->L1->L2->L3.
- **Read path**: WriteBuffer -> SSTable files (newest first), with a block cache for decompressed blocks.
  Both point lookups (`get`) and range/prefix queries (`scan`, `rscan`) merge results across all sources.
  See [SSTable Read Path](docs/storage.md#sstable-read-path) for the full optimized lookup diagram.
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

**Scan behavior** depends on the database mode. Both `scan` and `rscan` accept
`(prefix, limit, offset, include_deleted)` where `offset` skips the first N matching keys before collecting up to
`limit` results (for pagination). When `include_deleted` is `true`, tombstoned keys are included in the results
alongside live keys; when `false` (default), tombstones are filtered out.

- **Ordered mode** (Int keys): `scan(prefix, limit, offset, include_deleted)` returns keys in ascending order
  starting from `prefix`; `rscan` returns keys in descending order.
- **Unordered mode** (Str keys): `scan(prefix, limit, offset, include_deleted)` returns keys whose string
  representation starts with `prefix`; `rscan` returns the same prefix-matched keys in reverse order.
  Ordering-based range queries are not available.

**Lazy iterators** provide an alternative to `scan`/`rscan` that avoids materializing all results into a `Vec`.
Four methods return standard Rust `Iterator` implementations:

- `ns.keys(prefix)` — forward `KeyIterator` yielding `Result<Key>`
- `ns.entries(prefix)` — forward `EntryIterator` yielding `Result<(Key, Value)>`
- `ns.rkeys(prefix)` — reverse `KeyIterator`
- `ns.rentries(prefix)` — reverse `EntryIterator`

All iterators skip tombstoned keys automatically. `EntryIterator` resolves `ValuePointer`s and decrypts
encrypted values transparently. Iterators are lazy — they only read blocks on demand, so `take(n)` is
efficient even on large datasets. Once an I/O error occurs, the iterator becomes sticky (returns `None`
on subsequent calls).

```rust
// Collect first 5 keys starting from prefix "user:"
let keys: Vec<Key> = ns.keys(&Key::from("user:"))?.take(5).collect::<Result<_>>()?;

// Iterate over all entries
for result in ns.entries(&Key::from(""))? {
    let (key, value) = result?;
    println!("{key}: {value:?}");
}
```

### Value

A Value is the payload associated with a key. It has three internal states:

- **Data** — arbitrary-length byte vector. An empty `Data` (zero bytes) is a valid, distinct value.
- **Null** — the key exists but carries no payload. `Null` is semantically different from empty `Data`.
- **Tombstone** — an internal deletion marker. When a key is deleted, the engine writes a tombstone instead of
  physically removing the entry. Tombstones are invisible to the public `get` API — it returns "key not found",
  indistinguishable from a key that never existed. The crate-internal `get_raw` method distinguishes tombstoned keys
  (`Some(Tombstone)`) from never-existed keys (`None`). The HTTP API uses this to return **410 Gone** for deleted
  keys vs **404 Not Found** for keys that never existed. Tombstones are resolved (garbage-collected) during
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

Objects are stored in **append-only pack files**, scoped per namespace:

```text
<db>/objects/
  <namespace>/
    pack-000001.pack
    pack-000002.pack
```

Each namespace gets its own isolated object store directory. New objects are appended
sequentially to the active pack file, avoiding the overhead of creating individual files
per object. Pack files are rotated at 256 MB. An in-memory index maps each hash to its
location within a pack file for O(1) lookups.

**Legacy format**: Older databases may contain **loose files** — one file per object in a
Git-style fan-out directory (`<ns>/ab/<hash>`). The read path checks pack files first,
then falls back to loose files. New writes always go to pack files.

For pack file format details, see [Bin Object Store](docs/storage.md#bin-object-store)
in the storage engine documentation.

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
in LSM   hash in pack index or loose file?
           │
         ┌─┴──┐
         │yes │ no
         │    ▼
         │  LZ4 compress (if enabled)
         │  append to active pack file
         ▼
         store ValuePointer in LSM
```

Deduplication happens naturally: if the object hash already exists in the pack index
or as a loose file (same hash), the write is skipped and only the LSM entry is created.

#### Read Path

`get(key)` → read `ValuePointer` from LSM → look up hash in pack index → seek + read
from pack file → verify CRC32C → decompress if needed → verify BLAKE3 → return value.
Falls back to loose file read if hash is not in the pack index.

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

After compaction, the engine runs a GC sweep over the bin object store. It collects all
live `ValuePointer` hashes from surviving SSTables, then repacks the pack files to exclude
dead objects. Because objects are content-addressed, an object is safe to delete only when
no `ValuePointer` in any SSTable references its hash. See
[Bin Object GC](docs/operations.md#bin-object-gc) for implementation details.

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

- The default namespace `_` **can** be dropped. It is automatically re-created (empty) on the next access,
  so the database always has a usable default namespace. This is useful for clearing all data without
  dropping individual keys.
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
`rev_get(key, index)` retrieves the value at a specific revision index (0 = oldest).

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

### WriteBatch (Atomic Multi-Key Writes)

`WriteBatch` buffers multiple put and delete operations and applies them atomically — all operations
succeed together or none are visible. This is not a transaction (no read isolation, no rollback); it
is an atomic write group.

```rust
use rkv::{DB, Config, WriteBatch};
use std::time::Duration;

let db = DB::open(Config::new("/tmp/batch_example")).unwrap();
let ns = db.namespace("_", None).unwrap();

let batch = WriteBatch::new()
    .put("key1", "value1", None)
    .put("key2", "value2", Some(Duration::from_secs(3600)))
    .delete("old_key");

let revisions = ns.write_batch(batch).unwrap();
assert_eq!(revisions.len(), 3);
```

Atomicity is achieved by holding the AOL lock for all record writes and then the memtable lock for
all in-memory applies. Lock ordering (AOL then memtable) matches single-key writes to prevent
deadlocks. Each operation gets its own revision ID and is individually broadcast to replicas — no
replication protocol changes are needed.

#### CLI: batch Command

The `batch` REPL command enters an interactive sub-REPL for building a write batch:

| Sub-command             | Description               |
| ----------------------- | ------------------------- |
| `put <key> <val> [ttl]` | Queue a put operation     |
| `del <key>`             | Queue a delete operation  |
| `show`                  | Display queued operations |
| `commit`                | Apply all ops atomically  |
| `abort`                 | Discard all ops and exit  |

The prompt changes to `[ns] batch (N)>` showing the namespace and operation count. Ctrl-C or
Ctrl-D aborts the batch.

#### HTTP: POST /api/{ns}/batch

```http
POST /api/{ns}/batch
Content-Type: application/json

{
  "ops": [
    {"op": "put", "key": "k1", "value": "v1"},
    {"op": "put", "key": "k2", "value": "v2", "ttl": 3600},
    {"op": "delete", "key": "k3"}
  ]
}
```

Response (200 OK):

```json
{
  "results": [
    { "key": "k1", "revision": "0193..." },
    { "key": "k2", "revision": "0193..." },
    { "key": "k3", "revision": "0193..." }
  ]
}
```

An empty `ops` array returns 400 Bad Request. Replica nodes return 403 Forbidden.

### Configuration

The `Config` struct controls database behavior and LSM tuning parameters:

| Field               | Type              | Default    | Description                                |
| ------------------- | ----------------- | ---------- | ------------------------------------------ |
| `path`              | `PathBuf`         | (required) | Database directory path                    |
| `create_if_missing` | `bool`            | `true`     | Create the directory if it doesn't exist   |
| `write_buffer_size` | `usize`           | 4 MB       | In-memory write buffer size before flush   |
| `max_levels`        | `usize`           | 3          | Maximum number of LSM levels               |
| `block_size`        | `usize`           | 4 KB       | SSTable block size                         |
| `cache_size`        | `usize`           | 8 MB       | Block cache size for decompressed blocks   |
| `object_size`       | `usize`           | 1 KB       | Bin object size threshold (see above)      |
| `compress`          | `bool`            | `true`     | LZ4-compress bin objects on disk           |
| `bloom_bits`        | `usize`           | 10         | Filter bits per key (0 = disabled)         |
| `filter_policy`     | `FilterPolicy`    | `Bloom`    | Filter type: `Bloom` or `Ribbon`           |
| `verify_checksums`  | `bool`            | `true`     | Verify checksums on read                   |
| `compression`       | `Compression`     | `LZ4`      | SSTable block compression                  |
| `io_model`          | `IoModel`         | `Mmap`     | File I/O strategy (see I/O Modes below)    |
| `cluster_id`        | `Option<u16>`     | `None`     | Cluster ID for RevisionID (random if None) |
| `aol_buffer_size`   | `usize`           | 128        | AOL flush threshold in records (0 = every) |
| `l0_max_count`      | `usize`           | 4          | Max L0 SSTable count before compaction     |
| `l0_max_size`       | `usize`           | 64 MB      | Max total L0 size before compaction        |
| `l1_max_size`       | `usize`           | 256 MB     | Max L1 size before merge to L2             |
| `default_max_size`  | `usize`           | 2 GB       | Default max size for L2+ levels            |
| `bloom_prefix_len`  | `usize`           | 0          | Prefix bloom filter length (0 = disabled)  |
| `write_stall_size`  | `usize`           | 8 MB       | Backpressure threshold (0 = disabled)      |
| `role`              | `Role`            | Standalone | Replication role                           |
| `repl_bind`         | `String`          | `0.0.0.0`  | Replication listen address                 |
| `repl_port`         | `u16`             | 8322       | Replication listen port                    |
| `primary_addr`      | `Option<String>`  | `None`     | Primary address for replica to connect to  |
| `peers`             | `Vec<String>`     | `[]`       | Peer addresses (peer mode only)            |
| `event_listener`    | `Option<Arc<..>>` | `None`     | Callback for flush/compaction events       |
| `shard_group`       | `u16`             | 0          | Shard group ID (0 = standalone)            |
| `owned_namespaces`  | `Vec<String>`     | `[]`       | Namespaces owned by this shard node        |
| `in_memory`         | `bool`            | `false`    | Pure in-memory mode (no disk I/O)          |

The CLI uses dot-notation keys for `config <key> <value>`:

| Config field        | CLI key                         |
| ------------------- | ------------------------------- |
| `path`              | `storage.path` (read-only)      |
| `create_if_missing` | `storage.create_if_missing`     |
| `write_buffer_size` | `lsm.write_buffer_size`         |
| `max_levels`        | `lsm.max_levels`                |
| `block_size`        | `lsm.block_size`                |
| `cache_size`        | `lsm.cache_size`                |
| `bloom_bits`        | `lsm.bloom_bits`                |
| `filter_policy`     | `lsm.filter_policy`             |
| `verify_checksums`  | `lsm.verify_checksums`          |
| `compression`       | `lsm.compression`               |
| `object_size`       | `object.size`                   |
| `compress`          | `object.compress`               |
| `io_model`          | `io.model`                      |
| `cluster_id`        | `revision.cluster_id`           |
| `aol_buffer_size`   | `aol.buffer_size`               |
| `l0_max_count`      | `lsm.l0_max_count`              |
| `l0_max_size`       | `lsm.l0_max_size`               |
| `l1_max_size`       | `lsm.l1_max_size`               |
| `default_max_size`  | `lsm.default_max_size`          |
| `bloom_prefix_len`  | `lsm.bloom_prefix_len`          |
| `write_stall_size`  | `lsm.write_stall_size`          |
| `role`              | `repl.role`                     |
| `repl_bind`         | `repl.bind`                     |
| `repl_port`         | `repl.port`                     |
| `primary_addr`      | `repl.primary_addr`             |
| `peers`             | `repl.peers`                    |
| `shard_group`       | `cluster.shard_group`           |
| `owned_namespaces`  | `cluster.owned_namespaces`      |
| `in_memory`         | `storage.in_memory` (read-only) |

`Config::new(path)` initializes all fields to their defaults. `Config::in_memory()` creates
a pure in-memory configuration. Fields can be overridden before passing the config to `DB::open`.

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

### In-Memory Mode

When `in_memory` is `true` (or `Config::in_memory()` is used), the database runs entirely
in RAM with zero disk I/O:

- No directories, AOL files, SSTables, or bin objects are created on disk
- All data lives in the memtable (`BTreeMap`) and grows unbounded
- No background threads (flush, compaction) are started
- `flush()`, `compact()`, and `sync()` are no-ops returning `Ok(())`
- `dump()` returns `Err(InvalidConfig)`
- Value separation is disabled — all values are stored inline regardless of `object_size`
- Write stall / backpressure is disabled

**Limitations:**

- Replication is incompatible with in-memory mode (rejected at validation time)
- Encryption is incompatible with in-memory mode (requires disk for salt/verification token)
- Data is lost when the database is dropped — there is no persistence

**CLI usage:**

```sh
rkv --in-memory          # REPL mode, no path needed
```

**Programmatic usage:**

```rust
let db = DB::open(Config::in_memory())?;
let ns = db.namespace("_", None)?;
ns.put("key", "value", None)?;
// Data lives only in memory — no files on disk
```

### Statistics and Observability

`db.stats()` returns a `Stats` snapshot with live counters (keys, cache hits, write buffer usage)
and persistent operation counters (puts, gets, deletes). `db.prometheus_metrics()` returns
Prometheus-compatible text. The `EventListener` trait provides callbacks for flush and compaction
events.

See [Observability](docs/observability.md) for the full stats table, Prometheus metrics reference,
EventListener API, and gap analysis.

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
operations include key-value CRUD (`get`, `put`, `delete`), atomic batch writes, prefix scans with pagination,
revision history queries, TTL management, and administrative actions (flush, compact, stats). Custom response
headers carry metadata such as revision IDs and TTL information.

**Security**: By default the server binds to `127.0.0.1` (loopback only) and rejects requests from non-local IPs.
For network-accessible deployments, an IP allow-list (`--allow-ip`) restricts API access to explicitly trusted
addresses. Health endpoints (`/health`) are exempt from IP filtering so load balancers can probe without
authorization.

An embedded single-page web UI is available via the `--ui` flag. When enabled, the server serves a browser-based
dashboard at `/ui` for browsing keys, managing namespaces, and viewing database statistics — useful for debugging
and exploration without curl. The UI includes a "Show deleted" toggle that lists tombstoned keys alongside live ones.

**Key GET status codes**: `GET /api/{ns}/keys/{key}` returns **200** (data), **204** (null value), **410 Gone**
(tombstoned/deleted key), or **404** (key never existed). The 410 vs 404 distinction uses the internal `get_raw`
method to detect tombstones.

**Scan with deleted keys**: `GET /api/{ns}/keys?deleted=true` passes `include_deleted=true` to the engine scan,
returning tombstoned keys alongside live ones. Without `deleted=true`, tombstoned keys are hidden (default).

See the [README](README.md#http-server) for startup examples and curl recipes.

## Design Decisions

- **Interior mutability**: `DB` is `Send + Sync`. All mutable fields use `Mutex<T>` or `RwLock<T>` so public methods
  take `&self`.
- **Binary/library boundary**: The CLI (`main.rs`) is strictly binary-only. `lib.rs` exports only engine types
  (`DB`, `Config`, `Error`, `Result`). Nothing from the REPL leaks into the library or FFI surface.

## Further Reading

- **[Configuration](docs/config.md)** — config file (YAML/TOML), environment variable
  overrides (`RKV_*`), CLI flags, and precedence rules.
- **[Storage Engine](docs/storage.md)** — data integrity (checksums, recovery), LSM-tree
  internals (SSTables, block cache, bloom filters, MemTable, AOL), and bin object store.
- **[Maintenance Operations](docs/operations.md)** — flush, sync, destroy, repair, dump/load,
  compaction, and bin object GC.
- **[Observability](docs/observability.md)** — statistics, Prometheus metrics, EventListener
  callbacks, and gap analysis (dead code, known limitations).
- **[Replication](docs/replication.md)** — primary-replica replication (read scaling) and
  peer-peer replication (multi-writer with LWW conflict resolution).
- **[Cluster / Sharding](docs/cluster.md)** — namespace-level sharding with gateway routing
  for horizontal data distribution across shard groups.
