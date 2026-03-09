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
| Date   | 2026-03-09                      |

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
| put       | 757 µs    | 8.01 ms  | 15.54 ms  | 2.16 s    |
| get       | 253 µs    | 2.27 ms  | 4.66 ms   | 9.10 s    |
| delete    | 477 µs    | 4.00 ms  | 7.95 ms   | 862.13 ms |
| scan      | 124 µs    | 944 µs   | 1.86 ms   | 229.90 ms |
| batch     | 568 µs    | 4.22 ms  | 8.23 ms   | 2.06 s    |
| keys      | 162 µs    | 1.02 ms  | 1.93 ms   | 235.78 ms |
| flush     | 1.08 ms   | 3.58 ms  | 6.34 ms   | 6.55 ms   |
| get_sst   | 3.52 ms   | 25.91 ms | 53.14 ms  | 9.38 s    |
| get_cpt   | 3.25 ms   | 25.63 ms | 51.28 ms  | 8.61 s    |
| put_obj   | 292.66 ms | 2.29 s   | 4.58 s    | 283.54 s  |
| get_obj   | 7.77 ms   | 63.48 ms | 127.25 ms | 20.02 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 553 µs | 3.40 ms | 7.25 ms | 695.00 ms |
| get       | 245 µs | 2.26 ms | 4.57 ms | 687.18 ms |
| delete    | 311 µs | 2.46 ms | 4.93 ms | 468.67 ms |
| scan      | 138 µs | 947 µs  | 1.84 ms | 195.78 ms |
| batch     | 423 µs | 2.69 ms | 5.17 ms | 410.87 ms |
| keys      | 129 µs | 955 µs  | 1.90 ms | 165.31 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)    | 1K (R)    | 8K (B)   | 8K (R)   | 16K (B)   | 16K (R)   |
| --------- | --------- | --------- | -------- | -------- | --------- | --------- |
| put       | 714 µs    | 705 µs    | 5.39 ms  | 5.33 ms  | 10.74 ms  | 10.72 ms  |
| get       | 256 µs    | 228 µs    | 2.18 ms  | 2.17 ms  | 4.51 ms   | 4.49 ms   |
| delete    | 511 µs    | 506 µs    | 3.99 ms  | 3.94 ms  | 8.73 ms   | 8.13 ms   |
| scan      | 140 µs    | 126 µs    | 994 µs   | 920 µs   | 1.90 ms   | 1.81 ms   |
| batch     | 560 µs    | 588 µs    | 4.31 ms  | 4.14 ms  | 8.29 ms   | 8.43 ms   |
| keys      | 146 µs    | 127 µs    | 993 µs   | 960 µs   | 2.02 ms   | 1.89 ms   |
| flush     | 1.18 ms   | 1.76 ms   | 3.60 ms  | 7.31 ms  | 6.40 ms   | 13.49 ms  |
| get_sst   | 3.62 ms   | 3.88 ms   | 26.63 ms | 29.58 ms | 52.77 ms  | 59.59 ms  |
| get_cpt   | 3.56 ms   | 3.89 ms   | 25.64 ms | 29.53 ms | 50.40 ms  | 58.40 ms  |
| put_obj   | 281.87 ms | 284.66 ms | 2.27 s   | 2.30 s   | 4.62 s    | 4.57 s    |
| get_obj   | 7.79 ms   | 7.85 ms   | 63.05 ms | 63.27 ms | 126.42 ms | 126.45 ms |

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
| Date   | 2026-03-09                      |

### Sequential Put

| N   | rKV      | redb     | sled     | fjall    |
| --- | -------- | -------- | -------- | -------- |
| 1K  | 753 µs   | 1.67 ms  | 3.11 ms  | 1.93 ms  |
| 8K  | 7.23 ms  | 12.20 ms | 26.74 ms | 15.18 ms |
| 16K | 12.22 ms | 25.06 ms | 54.17 ms | 30.12 ms |
| 1M  | 2.27 s   | 1.97 s   | 4.04 s   | 2.03 s   |

### Random Get

| N   | rKV     | redb      | sled    | fjall   |
| --- | ------- | --------- | ------- | ------- |
| 1K  | 259 µs  | 230 µs    | 425 µs  | 408 µs  |
| 8K  | 2.39 ms | 2.74 ms   | 4.02 ms | 4.11 ms |
| 16K | 5.25 ms | 5.98 ms   | 8.64 ms | 8.87 ms |
| 1M  | 10.87 s | 954.35 ms | 1.81 s  | 6.29 s  |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 561 µs    | 1.55 ms  | 1.89 ms  | 1.98 ms  |
| 8K  | 4.13 ms   | 12.08 ms | 18.02 ms | 16.13 ms |
| 16K | 8.30 ms   | 24.84 ms | 36.69 ms | 32.70 ms |
| 1M  | 770.97 ms | 1.81 s   | 5.14 s   | 1.95 s   |

### Forward Scan

| N   | rKV       | redb     | sled      | fjall     |
| --- | --------- | -------- | --------- | --------- |
| 1K  | 140 µs    | 80 µs    | 326 µs    | 151 µs    |
| 8K  | 1.05 ms   | 572 µs   | 2.14 ms   | 1.13 ms   |
| 16K | 2.14 ms   | 1.17 ms  | 4.04 ms   | 2.20 ms   |
| 1M  | 233.65 ms | 98.77 ms | 313.60 ms | 323.12 ms |

### Batch Write

| N   | rKV     | redb     | sled     | fjall     |
| --- | ------- | -------- | -------- | --------- |
| 1K  | 731 µs  | 5.05 ms  | 4.64 ms  | 563 µs    |
| 8K  | 4.55 ms | 44.84 ms | 39.86 ms | 4.42 ms   |
| 16K | 8.58 ms | 91.64 ms | 80.91 ms | 8.78 ms   |
| 1M  | 2.09 s  | 6.45 s   | 5.25 s   | 661.92 ms |

### Reproduce

```sh
make bench-compare
```
