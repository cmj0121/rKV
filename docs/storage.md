# Storage Engine

> Internal storage details for [rKV](../CONCEPTS.md). For core concepts (keys, values,
> namespaces, revisions, configuration), see the main [Concepts](../CONCEPTS.md) document.

## Maintenance Operations

Maintenance operations handle durability, recovery, backup, and storage optimization.
All maintenance methods return `Result`.

### Flush / Sync

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
    1. drain_all() — extract all revisions per key (sorted, includes tombstones)
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

### Destroy / Repair

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

Returns a `RecoveryReport` describing what was scanned, recovered, and lost (see [Data Integrity](#data-integrity)
below). The database is openable after repair.

### Dump / Load

| Method               | Kind     | Signature                                | Description           |
| -------------------- | -------- | ---------------------------------------- | --------------------- |
| `dump`               | instance | `&self, path -> Result<()>`              | Export (V1)           |
| `dump_with_options`  | instance | `&self, path, DumpOptions -> Result<()>` | Export (V2)           |
| `load`               | static   | `(path) -> Result<DB>`                   | Import dump           |
| `load_with_password` | static   | `(path, &str) -> Result<DB>`             | Import encrypted dump |

`dump` flushes all in-memory write buffers, merges SSTable levels per namespace
(same strategy as compaction), filters tombstones, resolves `Pointer` values to
inline `Data`, and writes each entry to the dump file with a CRC32C checksum.
Encrypted namespaces are skipped.

`dump_with_options` accepts a `DumpOptions` struct:

- **`after_revision`**: Only include entries whose revision ID is greater than
  the given threshold. Enables incremental backups — dump only what changed
  since the last backup.
- **`password`**: Encrypt records with AES-256-GCM (key derived via Argon2id).
  The salt is stored in the V2 header; the password is required to restore.

When either option is set, the V2 format is used automatically.

`load` reads the dump file, creates a fresh DB at the stored path, replays all
records via `namespace.put()`, and flushes. Returns `InvalidConfig` if the target
path already contains data. Returns `EncryptionRequired` if the dump is encrypted.

`load_with_password` decrypts each record using the provided password. Returns
`Corruption` if the password is wrong (AES-GCM tag verification failure).

`load` / `load_with_password` are not exposed in the CLI because they would
require replacing the live DB handle mid-session.

#### Dump File Format

**V1** (produced by `dump`):

```text
Header:
  [magic: 4B "rKVD"]  [version: 2B BE = 1]
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

**V2** (produced by `dump_with_options`):

```text
Header:
  [magic: 4B "rKVD"]  [version: 2B BE = 2]
  [path_len: 2B BE]   [path: UTF-8 bytes]
  [flags: 1B]         [after_revision: 16B BE u128]
  [salt: 16B]         (only if flags & 0x01, for encryption)

Records (repeating):
  [payload_len: 4B BE] [payload or encrypted_payload] [checksum: 5B CRC32C]

V2 Payload (before encryption):
  [ns_len: 2B BE]  [namespace]
  [key_len: 2B BE] [key_bytes]
  [value_tag: 1B]  [value_data_len: 4B BE] [value_data]
  [expires_at_ms: 8B BE]
  [revision: 16B BE u128]

EOF sentinel:
  [payload_len: 4B = 0x00000000]
```

Flags: bit 0 = encrypted. When encrypted, each record payload is individually
encrypted with AES-256-GCM (12-byte nonce prepended to ciphertext + 16-byte tag).

The format mirrors the AOL record layout for consistency. Each record is
self-describing and independently verifiable via its checksum. V1 and V2 are
backward-compatible — the reader auto-detects the version from the header.

### Compaction

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

#### Auto-Compaction

After each `flush()`, the engine signals the background compaction thread.
The thread checks whether any namespace's L0 level exceeds the configured
thresholds (`l0_max_count` or `l0_max_size`). If either threshold is met,
compaction runs automatically. This eliminates the need for manual compaction
in typical workloads while still allowing explicit `compact()` calls.

#### Background Compaction Thread

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

#### Bin Object GC

After all level merges complete for a namespace, compaction runs a
garbage-collection sweep over the bin object store:

1. Skip GC if the MemTable has entries (concurrent puts may have written
   objects not yet reflected in the MemTable — deferring GC avoids
   deleting live objects).
2. Collect all live `ValuePointer` hashes from every surviving SSTable.
3. List all objects on disk (pack index + loose files).
4. Remove orphaned hashes from the pack index and delete orphaned loose files.
5. Call `repack_gc(live_hashes)` to physically remove dead records from pack
   files by rewriting them with only live entries.

This handles overwrites (old Pointer orphaned), tombstones (shadowed
Pointer orphaned), and dedup safely (an object is kept as long as at
least one SSTable entry still references it).

The CLI exposes compaction via the `compact` REPL command.

## Data Integrity

Every WAL entry and SSTable block carries a CRC32C checksum. On write the engine computes
the checksum over the raw data; on read the engine recomputes and compares to detect
corruption caused by bit rot, partial writes, or disk errors.

Bin objects use BLAKE3 content hashes via `ValuePointer` — a separate, complementary
integrity mechanism (see [Value Separation](../CONCEPTS.md#value-separation-bin-objects)).

### Checksum Format

Each checksum is 5 bytes on disk:

| Field   | Type  | Bytes | Description                     |
| ------- | ----- | ----- | ------------------------------- |
| `algo`  | `u8`  | 1     | Algorithm tag (`0x01` = CRC32C) |
| `value` | `u32` | 4     | Big-endian checksum value       |

The algorithm tag allows future extension to stronger checksums without breaking
existing data files.

### Read-Time Verification

When `verify_checksums` is enabled (default: `true`), every block and WAL entry read
from disk is verified against its stored checksum. A mismatch produces a `Corruption`
error. Disabling verification trades safety for read speed — useful for bulk scans
where occasional corruption is acceptable.

### Offline Recovery

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

## LSM-Tree Storage

Data is organized in levels. Fresh writes land in an in-memory buffer (MemTable) and are
flushed to sorted L0 SSTable files on disk via `DB::flush()`. `DB::compact()` merges L0
files into a single L1 SSTable. The read path checks the MemTable first, then searches
SSTables from newest to oldest across all levels.

```text
┌──────────────────────────────────────────────────┐
│                    MemTable                      │
│  BTreeMap<Key, Vec<MemEntry{rev, val, expires}>> │
│  approximate_size tracked for flush trigger      │
└────────────────────┬─────────────────────────────┘
               flush │
                     ▼
┌────────────────────────────────────────────────┐
│  Level 0 (unsorted)     max: 4 files           │
│  ┌────────┐ ┌────────┐ ┌────────┐              │
│  │ SST-07 │ │ SST-06 │ │ SST-05 │              │
│  │ newest │ │        │ │ oldest │              │
│  └────────┘ └────────┘ └────────┘              │
└────────────────────┬───────────────────────────┘
             compact │  merge-sort + dedup
                     ▼
┌─────────────────────────────────────────────────┐
│  Level 1             max: 10 MB (×10 per level) │
│  ┌──────────────────────────────────────────┐   │
│  │ SST-04  (sorted, non-overlapping ranges) │   │
│  └──────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────┘
             compact │  trivial move if target empty
                     ▼
┌─────────────────────────────────────────────────┐
│  Level 2             max: 100 MB                │
│  ┌──────────────────────────────────────────┐   │
│  │ SST-02  SST-03                           │   │
│  └──────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────┘
                     ▼
             Level 3, 4, ...  (×10 growth)
```

### Block Compression

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

### LRU Block Cache

An LRU (Least Recently Used) block cache stores **raw decompressed SSTable data blocks**
in memory, keyed by `(sst_id, block_index)`. This avoids redundant disk I/O and decompression
when the same block is read repeatedly (e.g., hot keys, repeated scans). The cache is sharded
16 ways to reduce mutex contention under concurrent reads.

```text
┌──────────────────────────────────────────────────────┐
│              ShardedBlockCache (16-way)              │
│                                                      │
│  shard = hash(sst_id, block_index) % 16              │
│                                                      │
│  ┌────────┐ ┌────────┐ ┌────────┐     ┌────────┐     │
│  │Shard 0 │ │Shard 1 │ │Shard 2 │ ... │Shard 15│     │
│  │Mutex<  │ │Mutex<  │ │Mutex<  │     │Mutex<  │     │
│  │LRU>    │ │LRU>    │ │LRU>    │     │LRU>    │     │
│  └────────┘ └────────┘ └────────┘     └────────┘     │
│                                                      │
│  Each LRU: slab-backed doubly-linked list            │
│  Value: Arc<Vec<u8>> (raw decompressed block bytes)  │
└──────────────────────────────────────────────────────┘
```

The `cache_size` config field controls the total byte budget (default 8 MB). Set to `0` to
disable the cache entirely — all operations remain functionally correct but may be slower
for workloads with repeated block access.

**Behavior:**

- **Lookup**: On each block access, the cache is checked first. A hit returns a clone of the
  raw decompressed bytes (`Arc<Vec<u8>>`) and promotes the block to MRU position. Point lookups
  then use restart-point binary search directly on the cached bytes — no parsing step needed.
- **Insert**: On a cache miss, after decompression, the raw bytes are inserted into the cache.
  If the cache exceeds its capacity, LRU entries are evicted until the budget is met. Blocks
  larger than the total capacity are silently skipped to prevent thrashing.
- **Compaction eviction**: When SSTables are merged during compaction, all cached blocks for
  the old (replaced) SSTables are evicted, freeing memory for the new merged SSTable's blocks.
- **Restart**: The cache is in-memory only. On `DB::open()`, the cache starts empty and warms
  up naturally through reads.

**Size estimation** per cached block: `data.len() + 64` bytes, where `data.len()` is the
decompressed block size and 64 bytes covers the slab node, Arc overhead, and hash entry.

### SSTable File Format

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

V4 entries additionally include revision and TTL fields after the value data:

```text
[key_len: u16 BE][key_bytes][value_tag: u8][value_len: u32 BE][value_data]
[revision: u128 BE (16B)][expires_at_ms: u64 BE (8B)]
```

Entries are stored in sorted key order. `key_bytes` uses the same memcmp-preserving
serialization as `Key::to_bytes()`. `value_tag` encodes the Value variant (0x00 = Data,
0x01 = Null, 0x02 = Tombstone, 0x03 = Pointer).

**Restart points (per-block index):**

Every 16th entry within a data block records its byte offset as a **restart point**.
These offsets are appended as a trailer at the end of the decompressed block, before
compression:

```text
[entry_0][entry_1]...[entry_N][restart_0: u32 LE]...[restart_R: u32 LE][num_restarts: u32 LE]
```

Point lookups binary-search the restart keys to jump directly to the right 16-entry
interval, then parse only that interval instead of scanning the entire block. This
reduces within-block search from O(N) to O(log N + 16).

The `FEATURE_RESTART_POINTS` flag (`0x01`) in the footer's features field signals that
blocks contain restart trailers. Old readers reject unknown feature flags (safe forward
compatibility). New readers handle both old blocks (no trailer) and new blocks.

**Index block layout:**

```text
repeated: [key_len: u16 BE][last_key_bytes][offset: u64 BE][size: u32 BE]
```

Each entry records the last key in a data block plus the block's file offset and on-disk
size. Point lookups binary-search the index to find the candidate block, then use restart
points for O(log N) search within the block.

**Footer layout (48 bytes):**

```text
[magic: 4B "rKVS"][version: u16 BE][entry_count: u64 BE]
[index_offset: u64 BE][index_size: u32 BE]
[data_blocks: u32 BE][features: u32 BE][reserved: 9B][checksum: 5B CRC32C]
```

The `features` bitmask signals optional format extensions. Currently defined flags:

| Bit    | Flag                     | Description                          |
| ------ | ------------------------ | ------------------------------------ |
| `0x01` | `FEATURE_RESTART_POINTS` | Data blocks contain restart trailers |

The footer checksum covers the first 43 bytes. The reader verifies magic, version, and
checksum before parsing the index. Unknown feature flags cause the reader to reject the
file (safe forward compatibility).

### SSTable Read Path

Point lookups (`get`) check the MemTable first, then search SSTables level by level
(L0 newest-first, then L1, L2, ...). The first match wins:

```text
get(key)
  │
  ▼
┌──────────────────┐
│ 1. MemTable      │  BTreeMap lookup: O(log N)
│    Lookup        │  Found? → resolve ValuePointer, decrypt, return
└───────┬──────────┘
        │ miss
        ▼
┌──────────────────────────────────────────────────────┐
│ 2. SSTable Search (newest → oldest, L0 → Lmax)       │
│                                                      │
│   For each SSTable:                                  │
│   ┌────────────────────────────┐                     │
│   │ 2a. Key-Range Pre-Filter   │  2 byte cmps        │
│   │     key < first_key? SKIP  │  96% skip rate      │
│   │     key > last_key?  SKIP  │  at 1M keys         │
│   └───────────┬────────────────┘                     │
│               │ in range                             │
│   ┌───────────▼────────────────┐                     │
│   │ 2b. Bloom Filter Check     │  ~7 hash probes     │
│   │     not in set? SKIP       │  ~1% FPR            │
│   └───────────┬────────────────┘                     │
│               │ maybe present                        │
│   ┌───────────▼────────────────┐                     │
│   │ 2c. Index Binary Search    │ O(log B)            │
│   │     find candidate block   │ B = block count     │
│   └───────────┬────────────────┘                     │
│               │                                      │
│   ┌───────────▼────────────────┐                     │
│   │ 2d. Block Load             │                     │
│   │     Cache hit? → raw bytes │  ShardedLRU         │
│   │     Miss? → decompress     │  16-way, 8 MB       │
│   │            + cache insert  │                     │
│   └───────────┬────────────────┘                     │
│               │                                      │
│   ┌───────────▼────────────────┐                     │
│   │ 2e. Restart-Point Search   │  O(log R + 16)      │
│   │     Zero-copy RestartIndex │  R = restart count  │
│   │     Binary search on keys  │  (every 16 entries) │
│   └───────────┬────────────────┘                     │
│               │                                      │
│   Found? → break (newest revision wins)              │
└──────────────────────────────────────────────────────┘
  │
  ▼
┌──────────────────┐
│ 3. Resolve       │  ValuePointer? → ObjectStore.get(hash)
│    + Decrypt     │  Encrypted? → AES-256-GCM decrypt
└──────────────────┘
```

### Merged Scan (MergeIterator)

`scan`, `rscan`, `count`, `delete_range`, and `delete_prefix` all use a lazy
**MergeIterator** that streams deduplicated entries from the MemTable and all SSTable
levels via a min-heap. This avoids materializing all matching entries into memory,
enabling early termination when a `limit` is reached.

**Architecture**:

```text
Namespace::scan(prefix, limit, offset, include_deleted)
  1. Build MergeIterator from:
     - SSTableScanIter per SSTable (lazy block-by-block reading)
     - VecSource wrapping a MemTable snapshot
  2. Stream (Key, Value) pairs from the iterator:
     a. Skip tombstones (unless include_deleted)
     b. Skip offset entries
     c. Collect up to limit keys
     d. Break early once limit reached
```

**Priority-based dedup** — when multiple sources contain the same key, the source
with the highest priority wins:

1. Deepest level (L_max) gets lowest priority
2. L0: oldest SSTable → newest SSTable (increasing priority)
3. MemTable snapshot gets highest priority (`u32::MAX`)

The heap pops the minimum key; ties are broken by highest priority. Duplicate keys
are drained and only the highest-priority version is emitted.

**SSTableScanIter** captures `Arc<IoBytes>` at construction time, so the SSTable
`RwLock` is released immediately. Blocks are decompressed and parsed on demand. In
ordered mode, out-of-range blocks are skipped via the block index. Prefix bloom
filters skip entire SSTables when the prefix is definitely absent.

**Scan modes**:

- **Ordered mode** (Int keys): Uses the block index to find the starting block, then
  reads forward (scan) or backward (rscan) from the prefix key. Stops when keys move
  out of range.
- **Unordered mode** (Str keys): Serializes the prefix using `Key::to_prefix_bytes()`
  (omitting the trailing null terminator), then checks each SSTable entry with
  `starts_with`. All matching blocks must be read.

**Reverse scan**: `rscan` uses an `RScanAdapter` that drains the forward merge
iterator, reverses the collected entries, then yields them with offset/limit. True
lazy reverse iteration over an LSM merge is deferred to a future optimization.

**Tombstone handling**: The merge iterator emits tombstones — callers decide whether
to filter them. This ensures tombstones correctly shadow values from deeper levels.

### Key Filters

Each SSTable embeds a key filter that enables skipping SSTables during point lookups.
The filter is built from all keys at flush/compaction time and serialized into the SSTable
file between the data blocks and the index block. The `filter_policy` config selects
between **Bloom** (default) and **Ribbon** filters.

**Configuration**: `bloom_bits` (default 10) controls the bits-per-key. `filter_policy`
(default `Bloom`) selects the algorithm. Set `bloom_bits` to 0 to disable filters entirely.

#### Bloom Filter

**Hash function**: LevelDB-compatible murmur-inspired 32-bit hash with double-hashing
probe strategy (`h.rotate_left(15)` per probe). The number of hash probes is computed as
`k = ln(2) * bits_per_key`, clamped to `[1, 30]`.

**Serialization**: `[num_hashes: u8][bit_array...]`. At 10 bits/key, the false-positive
rate is approximately 1%.

#### Ribbon Filter

Ribbon (Rapid Incremental Boolean Banding ON the fly) is an alternative filter based on
solving a banded linear system over GF(2) (Dillinger & Walzer, 2021). It achieves ~30%
smaller space than Bloom at the same false-positive rate.

**Characteristics**: Width = 64 bits (u64 operations). Result bits `r` controls FPR =
2^(-r). At `bloom_bits = 10`, r = 7 gives ~0.8% FPR. Build time is higher than Bloom;
query time is comparable.

**Serialization**: `[0x02][result_bits: u8][num_rows: u32 LE][solution...]`. The `0x02`
tag byte enables auto-detection during deserialization.

**Backward compatibility**: Old Bloom SSTables remain readable. A DB can contain mixed
filter types — `KeyFilter::from_bytes()` auto-detects by the first byte tag. Stored in
the SSTable footer via `filter_offset` and `filter_size` fields. Old SSTables with
`filter_size = 0` are backwards compatible — `may_contain()` returns `true`.

On `DB::open()`, the engine scans `<db>/sst/<namespace>/L<n>/` directories and opens all
`.sst` files into an in-memory reader cache. L0 readers are ordered newest-first; L1+
readers are ordered by ascending sequence number.

### Prefix Bloom Filter

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

### WriteBuffer (MemTable)

The WriteBuffer is the first component in the write path. It is an in-memory sorted store
backed by a `BTreeMap<Key, Vec<MemEntry>>` — each key maps to its full revision history
(oldest entry at index 0). The MemTable provides:

- **put/get/delete/exists** — core key-value operations
- **scan/rscan** — ordered iteration with offset/limit pagination and optional `include_deleted` flag
  (range queries in ordered mode, prefix matching in unordered mode)
- **count** — live key count (excludes tombstones and expired entries)
- **rev_count/rev_get** — revision history access
- **ttl** — remaining time-to-live for a key
- **auto-upgrade** — when the first `Str` key is inserted, all existing `Int` keys are widened to `Str`

Each namespace has its own independent MemTable. The `DB` struct holds a
`RwLock<HashMap<String, Mutex<MemTable>>>` for per-namespace memtables, created lazily
on first access. A shared `RevisionGen` produces candidate RevisionIDs; individual MemTables
enforce per-key monotonicity.

**Current status**: The MemTable serves as the write buffer. On startup, the AOL is replayed
to reconstruct memtable state (see [Append-Only Log](#append-only-log-aol) below). `DB::flush()` calls
`drain_all()` to extract all revisions per key in sorted order, writes an L0 SSTable,
and truncates the AOL. After flush, the MemTable is empty and ready for new writes.

### Append-Only Log (AOL)

The AOL is the durability layer in the write path. Every mutation is appended to the AOL
**before** being applied to the MemTable. On crash recovery, `DB::open()` replays the AOL
to reconstruct the in-memory state.

**Write path**: `Client API -> AOL (append + flush) -> MemTable -> Response`

#### AOL File Format

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

#### TTL Encoding

TTL is stored as an **absolute timestamp** (ms since Unix epoch) rather than a relative
duration. This ensures correct expiry semantics on replay — if a key was set to expire at
time T, it expires at time T regardless of when the database is reopened. Expired records
are skipped during replay.

#### Replay Semantics

On `DB::open()`, the engine replays the AOL sequentially:

1. Skip records where `expires_at_ms > 0` and `expires_at_ms <= now`
2. For surviving records, get-or-create the namespace's MemTable
3. Feed each record through `MemTable::put()` with the stored revision and remaining TTL
4. Per-key monotonicity is enforced by the MemTable (candidate revisions may be bumped)

Truncated or corrupted records at the tail of the file are silently skipped (counted in the
skip counter). This handles partial writes from crashes during append.

#### Limitations

- **Buffered flush**: The AOL buffers up to `aol_buffer_size` records (default 128) before
  flushing to the OS. A background thread flushes every 60 s if dirty data exists. On a
  hard crash, up to `aol_buffer_size` records (or 60 s of writes) may be lost. Set to 0
  for per-record flush (maximum durability). `DB::close()` always flushes remaining data.
- **No fsync on every write**: The implementation flushes the userspace buffer but does not
  call `fsync` per record. A future `sync_mode` config option will control this.

## Bin Object Store

Large values (> `object_size`) are stored in a content-addressable **bin object store**
rather than inline in the LSM-tree. For an overview of value separation, see
[Value Separation](../CONCEPTS.md#value-separation-bin-objects) in the Concepts document.
This section covers the on-disk format and internal implementation.

### Pack File Format

Objects are stored in append-only **pack files** — multiple objects batched into a single
file for reduced I/O syscalls. Each namespace has its own set of pack files at
`<db>/objects/<namespace>/pack-NNNNNN.pack` (zero-padded sequence number).

```text
Header (6 bytes):
  [magic: 4B "rKVO"]  [version: u16 BE = 1]

Records (repeated, append-only):
  [hash: 32B]               BLAKE3 content hash
  [original_size: u32 BE]   uncompressed data size
  [flags: u8]               bit 0 = LZ4 compressed
  [data_len: u32 BE]        compressed data length
  [data: data_len bytes]    payload (raw or LZ4)
  [checksum: 5B CRC32C]     covers hash through end of data
```

Record overhead: 46 bytes (41-byte header + 5-byte checksum). At 4 KB objects
compressed to ~3 KB, each record is approximately 3046 bytes.

Pack files are self-describing — the index is rebuilt by scanning records on open,
so no separate index file is needed. This simplifies crash recovery: truncated or
corrupted tail records are detected by checksum verification and silently skipped,
exactly like AOL recovery.

### In-Memory Index

On `ObjectStore::open()`, all pack files are scanned in sequence order to build an
in-memory `HashMap<[u8; 32], PackEntry>`:

```text
PackEntry {
    pack_seq: u64,      // which pack file
    offset: u64,        // byte offset of record start
    data_len: u32,      // compressed data length
    original_size: u32, // uncompressed size
    flags: u8,          // compression flags
}
```

Duplicate hashes across packs are resolved by sequence order — later packs overwrite
earlier entries, ensuring the latest version wins.

### Write Path

New objects are appended to the active pack file:

```text
ObjectStore::write(data, compress)
  1. BLAKE3 hash the data
  2. Check pack index for dedup — if hash exists, return existing ValuePointer
  3. Check loose files for dedup (backward compat)
  4. LZ4 compress if enabled
  5. Rotate pack file if current one >= 256 MB
  6. Append record to active pack file (BufWriter + flush + fsync)
  7. Insert into in-memory index
  8. Return ValuePointer(hash, size)
```

**Fsync**: Every record append calls `sync_all()` after flushing the BufWriter,
ensuring the record is durable on the storage device before the `ValuePointer` is
inserted into the MemTable. This matches the AOL's durability guarantee.

**Size limit**: Objects larger than 4 GB are rejected (pack format uses `u32` for
`data_len` and `original_size`). The compressed payload size is also checked after
LZ4 compression, since LZ4 can produce output slightly larger than the input for
incompressible data.

**Pack rotation**: When the active pack file reaches 256 MB, it is closed and a
new pack file is created with the next sequence number. This bounds memory usage
during `scan_pack_file()` and keeps repack overhead manageable.

### Read Path

```text
ObjectStore::read(vp, verify)
  1. Look up hash in pack index (hold Mutex briefly, clone PackEntry)
  2. If found: seek to offset in pack file, read record, verify CRC32C
  3. If not found: fall back to loose file read (legacy format)
  4. Decompress if FLAG_LZ4 set
  5. If verify: recompute BLAKE3 hash, compare against ValuePointer
  6. Return data
```

The Mutex is held only for the index lookup (clone + release), not during file I/O.
This keeps read latency predictable under concurrent access.

### GC Repacking

When compaction triggers bin object GC (see [Bin Object GC](#bin-object-gc) above),
orphaned records are physically removed by rewriting pack files:

```text
repack_gc(live_hashes)
  1. Close active pack writer (flush all buffered data)
  2. Scan all pack files on disk (not the in-memory index)
  3. For each record: keep if hash is in live_hashes, count as dead otherwise
  4. If no dead records: return early (no-op)
  5. Write a new pack file with only live records
  6. Update in-memory index to point to new pack (BEFORE deleting old packs)
  7. Delete old pack files
```

**Crash safety**: The in-memory index is updated before old packs are deleted. If the
process crashes between steps 6 and 7, the old packs remain on disk as harmless
leftovers — they are re-scanned on the next `ObjectStore::open()` and their entries
merge harmlessly with the new pack's entries (same hashes, same data).

The repack scans pack files on disk rather than the in-memory index because
`delete_object()` may have already removed entries from the index. Scanning the
physical files ensures all orphaned records are found and removed.

### Backward Compatibility

The object store supports two storage formats:

| Format          | Layout             | When used                            |
| --------------- | ------------------ | ------------------------------------ |
| **Pack files**  | `pack-NNNNNN.pack` | All new writes (current format)      |
| **Loose files** | `<fan_out>/<hash>` | Legacy databases (read-only support) |

**Loose file format** (1-byte header + payload):

```text
[flags: 1B]  bit 0 = LZ4 compressed
[payload]    raw or LZ4-compressed data
```

The read path checks the pack index first, then falls back to loose files. Dedup
checks both formats — a loose file with the same hash prevents a duplicate pack write.
GC handles both: loose files are deleted directly, packed records are removed via repack.
