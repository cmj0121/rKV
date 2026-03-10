# Observability

> Statistics, metrics, and diagnostics for [rKV](../CONCEPTS.md). For core concepts
> (keys, values, namespaces, revisions, configuration), see the main
> [Concepts](../CONCEPTS.md) document.

## Statistics

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
| `role`                | `String`         | Config                 | Replication role                        |
| `peer_count`          | `u64`            | PeerSessions (live)    | Connected peer sessions                 |
| `conflicts_resolved`  | `u64`            | AtomicU64 (live)       | LWW conflicts resolved (peer mode)      |

`LevelStat` contains `file_count: u64` and `size_bytes: u64`. The `level_stats` vector has
`max_levels` entries; `level_stats[i]` aggregates all namespaces at level `i`.

`pending_compactions` counts L0 levels where file count >= `l0_max_count` or total size >=
`l0_max_size`, plus L1+ levels where total size >= the level's max size threshold.

`stats()` returns `Stats` directly (not `Result<Stats>`) — it cannot fail. Live fields are derived
from MemTable state on each call. Operation counters are tracked via `AtomicU64` (Relaxed ordering)
and persisted to `stats.meta` on `DB::close()` / `Drop`, so they accumulate across restarts.

### Stats Persistence

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

### Analyze Command

`db.analyze()` re-derives all statistics from current engine state and persists operation counters
to disk. In the CLI, the `analyze` command calls this method and prints the results. Useful as an
admin recovery tool when stats may have drifted.

## Prometheus Metrics

rKV exposes Prometheus-compatible metrics via an HTTP endpoint and a programmatic API.

### Prometheus Endpoint

When running the HTTP server (`rkv serve`), `GET /metrics` returns all metrics in
Prometheus exposition text format. From the library API, call `db.prometheus_metrics()`
to get the same text as a `String`.

### Available Metrics

**Counters** (monotonically increasing):

| Metric                         | Labels                | Description                       |
| ------------------------------ | --------------------- | --------------------------------- |
| `rkv_ops_total`                | `op={put,get,delete}` | Total database operations         |
| `rkv_cache_total`              | `result={hit,miss}`   | Block cache operations            |
| `rkv_flush_total`              |                       | Total flush operations            |
| `rkv_compaction_total`         |                       | Total compaction operations       |
| `rkv_bytes_flushed_total`      |                       | Total bytes flushed to SSTables   |
| `rkv_bytes_compacted_total`    |                       | Total bytes written by compaction |
| `rkv_conflicts_resolved_total` |                       | Replication conflicts resolved    |

**Gauges** (current value):

| Metric                    | Labels            | Description                     |
| ------------------------- | ----------------- | ------------------------------- |
| `rkv_keys`                |                   | Current total keys in memtables |
| `rkv_data_size_bytes`     |                   | Current data size on disk       |
| `rkv_namespaces`          |                   | Current namespace count         |
| `rkv_sstables`            |                   | Current SSTable count           |
| `rkv_write_buffer_bytes`  |                   | Current write buffer memory     |
| `rkv_pending_compactions` |                   | Namespaces needing compaction   |
| `rkv_uptime_seconds`      |                   | Time since database opened      |
| `rkv_level_files`         | `level={0,1,...}` | SSTable files per level         |
| `rkv_level_bytes`         | `level={0,1,...}` | Bytes per level                 |

**Histograms** (latency distribution with 14 buckets from 10 us to 60 s):

| Metric                            | Labels                     | Description             |
| --------------------------------- | -------------------------- | ----------------------- |
| `rkv_op_duration_seconds`         | `op={put,get,delete,scan}` | Operation latency       |
| `rkv_flush_duration_seconds`      |                            | Flush operation latency |
| `rkv_compaction_duration_seconds` |                            | Compaction latency      |

### EventListener

The `EventListener` trait provides callbacks for database lifecycle events.
Set `Config.event_listener` to receive notifications:

```rust
use rkv::{EventListener, FlushEvent, CompactionEvent};
use std::sync::Arc;

struct MyListener;
impl EventListener for MyListener {
    fn on_flush_complete(&self, event: FlushEvent) {
        println!("flushed {} entries ({} bytes) in {:?}",
            event.entries, event.bytes, event.duration);
    }
    fn on_compaction_complete(&self, event: CompactionEvent) {
        println!("compacted L{}->L{} ({} bytes) in {:?}",
            event.source_level, event.target_level, event.bytes, event.duration);
    }
}

let mut config = Config::default();
config.event_listener = Some(Arc::new(MyListener));
```

`FlushEvent` fields: `namespace`, `entries`, `bytes`, `duration`.
`CompactionEvent` fields: `namespace`, `source_level`, `target_level`, `bytes`, `duration`.

Both callback methods have default no-op implementations, so you only need to implement
the events you care about.

## Gap Analysis

This section catalogs known dead code, missing API exposure, and architectural limitations.

### Remaining Dead Code

| Item                             | File         | Reason                                     |
| -------------------------------- | ------------ | ------------------------------------------ |
| `IoBackend::write_file_atomic()` | `io.rs`      | Trait stub, no backend implements it yet   |
| `IoBackend::sync_file()`         | `io.rs`      | Trait stub, no backend implements it yet   |
| `PackEntry::original_size`       | `objects.rs` | Pack format field reserved for future use  |
| `DumpRecord::expires_at_ms`      | `dump.rs`    | Format spec field, consumed when TTL added |
| `replay_peer_record()`           | `mod.rs`     | Peer AOL replay, not yet wired in          |
| `SSTableReader::features`        | `sstable.rs` | Accessed via `#[cfg(test)]` method only    |

### Missing API Exposure

- **`DB::load()`** is implemented but has no CLI command or HTTP endpoint. Users must
  call it from Rust code to restore a dump file.
- **Prometheus metrics** are served at `GET /metrics` but there is no CLI `metrics`
  command for local inspection without the HTTP server.

### Known Limitations

- **4 GB object size limit**: `ValuePointer.size` is `u32`, capping individual bin
  objects at ~4 GB. Values larger than this will overflow.
- **AOL durability gap**: The AOL buffers writes (default: flush every 128 records).
  A crash before the buffer is flushed loses uncommitted records. Set
  `aol_buffer_size = 0` for every-record flush at the cost of throughput.
- **Unbounded key size**: Keys are limited to `u16::MAX` (65,535) bytes by the SSTable
  entry format, but there is no Config-level limit or early validation.
