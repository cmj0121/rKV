# Maintenance Operations

> Administrative operations for [rKV](../CONCEPTS.md). For LSM-tree storage internals
> (SSTables, block cache, filters, MemTable, AOL), see the
> [Storage Engine](storage.md) document.

Maintenance operations handle durability, recovery, backup, and storage optimization.
All maintenance methods return `Result`.

## Flush / Sync

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

## Destroy / Repair

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

Returns a `RecoveryReport` describing what was scanned, recovered, and lost (see
[Data Integrity](storage.md#data-integrity)). The database is openable after repair.

## Dump / Load

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

### Dump File Format

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

## Compaction

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

### Auto-Compaction

After each `flush()`, the engine signals the background compaction thread.
The thread checks whether any namespace's L0 level exceeds the configured
thresholds (`l0_max_count` or `l0_max_size`). If either threshold is met,
compaction runs automatically. This eliminates the need for manual compaction
in typical workloads while still allowing explicit `compact()` calls.

### Background Compaction Thread

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

### Bin Object GC

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
