# Benchmark

> Performance of core rKV operations (MemTable and SSTable paths).

## Environment

| Field  | Value                           |
| ------ | ------------------------------- |
| OS     | linux x86_64                    |
| CPU    | AMD EPYC 7763 64-Core Processor |
| Cores  | 4                               |
| Memory | 15 GB                           |
| Rust   | 1.94.0 (4a4ef493e 2026-03-02)   |
| Date   | 2026-03-08                      |

## Methodology

Each operation runs against a fresh temporary DB in release mode.
Wall-clock time is measured via `std::time::Instant`.

| Operation | Description                                                   |
| --------- | ------------------------------------------------------------- |
| put       | Sequential inserts of N keys with 64-byte values              |
| get       | Random reads of N existing keys (shuffled order)              |
| delete    | Sequential deletes of N existing keys                         |
| scan      | Forward scan of all keys (limit=N, offset=0)                  |
| batch     | WriteBatch inserts in chunks of 100                           |
| keys      | Lazy KeyIterator full drain of N keys                         |
| flush     | Flush N keys from MemTable to L0 SSTable                      |
| get_sst   | Random reads of N keys from SSTable (after flush)             |
| get_cpt   | Random reads of N keys after flush + compaction (multi-level) |
| put_obj   | Sequential inserts of N keys with 4 KB values via ObjectStore |
| get_obj   | Random reads of N keys resolved from ObjectStore              |

**In-memory** variants run the same operations with `Config::in_memory()` (no disk).

## Results (Disk)

| Operation | 1K        | 8K       | 16K       | 1M        |
| --------- | --------- | -------- | --------- | --------- |
| put       | 1.06 ms   | 8.16 ms  | 15.86 ms  | 3.02 s    |
| get       | 288 µs    | 2.78 ms  | 6.25 ms   | 13.77 s   |
| delete    | 514 µs    | 4.58 ms  | 9.81 ms   | 922.00 ms |
| scan      | 156 µs    | 1.17 ms  | 2.59 ms   | 229.68 ms |
| batch     | 584 µs    | 4.21 ms  | 8.59 ms   | 2.17 s    |
| keys      | 134 µs    | 1.27 ms  | 2.62 ms   | 241.41 ms |
| flush     | 1.32 ms   | 4.42 ms  | 8.13 ms   | 7.85 ms   |
| get_sst   | 3.71 ms   | 26.20 ms | 54.87 ms  | 14.46 s   |
| get_cpt   | 3.39 ms   | 25.11 ms | 50.52 ms  | 14.29 s   |
| put_obj   | 414.96 ms | 3.27 s   | 6.33 s    | 413.45 s  |
| get_obj   | 7.99 ms   | 64.36 ms | 130.55 ms | 26.29 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 528 µs | 3.41 ms | 7.50 ms | 761.11 ms |
| get       | 252 µs | 3.08 ms | 6.18 ms | 842.20 ms |
| delete    | 300 µs | 2.74 ms | 5.91 ms | 550.51 ms |
| scan      | 127 µs | 1.22 ms | 2.54 ms | 201.99 ms |
| batch     | 434 µs | 2.64 ms | 5.19 ms | 463.63 ms |
| keys      | 133 µs | 1.17 ms | 2.55 ms | 166.20 ms |

## Reproduce

```sh
make bench
```

## Comparison

> rKV vs redb vs sled vs fjall — same pre-defined dataset.

### Environment

| Field  | Value                           |
| ------ | ------------------------------- |
| OS     | linux x86_64                    |
| CPU    | AMD EPYC 7763 64-Core Processor |
| Cores  | 4                               |
| Memory | 15 GB                           |
| Rust   | 1.94.0 (4a4ef493e 2026-03-02)   |
| Date   | 2026-03-08                      |

### Sequential Put

| N   | rKV      | redb     | sled     | fjall    |
| --- | -------- | -------- | -------- | -------- |
| 1K  | 746 µs   | 1.92 ms  | 3.23 ms  | 1.98 ms  |
| 8K  | 6.71 ms  | 13.43 ms | 27.37 ms | 15.20 ms |
| 16K | 12.49 ms | 26.09 ms | 58.87 ms | 30.35 ms |
| 1M  | 2.40 s   | 1.98 s   | 4.58 s   | 2.07 s   |

### Random Get

| N   | rKV     | redb    | sled     | fjall    |
| --- | ------- | ------- | -------- | -------- |
| 1K  | 271 µs  | 219 µs  | 452 µs   | 391 µs   |
| 8K  | 3.32 ms | 2.71 ms | 4.52 ms  | 4.78 ms  |
| 16K | 7.29 ms | 5.96 ms | 11.03 ms | 12.04 ms |
| 1M  | 15.12 s | 1.19 s  | 2.22 s   | 6.88 s   |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 572 µs    | 1.72 ms  | 1.91 ms  | 2.02 ms  |
| 8K  | 4.88 ms   | 12.08 ms | 19.59 ms | 16.63 ms |
| 16K | 10.70 ms  | 24.82 ms | 39.63 ms | 33.27 ms |
| 1M  | 827.83 ms | 1.82 s   | 6.05 s   | 2.00 s   |

### Forward Scan

| N   | rKV       | redb      | sled      | fjall     |
| --- | --------- | --------- | --------- | --------- |
| 1K  | 189 µs    | 77 µs     | 343 µs    | 161 µs    |
| 8K  | 1.50 ms   | 598 µs    | 2.47 ms   | 1.30 ms   |
| 16K | 3.23 ms   | 1.19 ms   | 4.87 ms   | 2.52 ms   |
| 1M  | 230.94 ms | 102.93 ms | 353.82 ms | 328.71 ms |

### Batch Write

| N   | rKV     | redb      | sled     | fjall     |
| --- | ------- | --------- | -------- | --------- |
| 1K  | 775 µs  | 5.79 ms   | 4.75 ms  | 567 µs    |
| 8K  | 4.86 ms | 61.31 ms  | 43.13 ms | 4.50 ms   |
| 16K | 9.58 ms | 113.64 ms | 84.98 ms | 8.60 ms   |
| 1M  | 2.24 s  | 8.50 s    | 5.74 s   | 638.20 ms |

### Reproduce

```sh
make bench-compare
```
