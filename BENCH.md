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
| put       | 757 µs    | 8.06 ms  | 15.43 ms  | 2.19 s    |
| get       | 234 µs    | 2.23 ms  | 4.68 ms   | 9.65 s    |
| delete    | 533 µs    | 4.04 ms  | 8.10 ms   | 864.36 ms |
| scan      | 127 µs    | 998 µs   | 2.01 ms   | 231.77 ms |
| batch     | 559 µs    | 4.22 ms  | 8.37 ms   | 2.09 s    |
| keys      | 128 µs    | 982 µs   | 2.18 ms   | 237.03 ms |
| flush     | 1.33 ms   | 3.95 ms  | 7.06 ms   | 6.72 ms   |
| get_sst   | 3.43 ms   | 26.98 ms | 53.14 ms  | 9.43 s    |
| get_cpt   | 3.46 ms   | 27.48 ms | 50.61 ms  | 8.69 s    |
| put_obj   | 508.62 ms | 3.81 s   | 7.52 s    | 454.97 s  |
| get_obj   | 7.91 ms   | 63.55 ms | 126.88 ms | 20.22 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 546 µs | 3.46 ms | 7.01 ms | 523.14 ms |
| get       | 241 µs | 2.21 ms | 4.58 ms | 649.78 ms |
| delete    | 312 µs | 2.44 ms | 4.95 ms | 469.09 ms |
| scan      | 143 µs | 919 µs  | 1.84 ms | 204.00 ms |
| batch     | 514 µs | 2.72 ms | 5.23 ms | 405.82 ms |
| keys      | 148 µs | 960 µs  | 1.88 ms | 165.86 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)    | 1K (R)    | 16K (B)   | 16K (R)   | 100K (B)  | 100K (R)  |
| --------- | --------- | --------- | --------- | --------- | --------- | --------- |
| put       | 741 µs    | 703 µs    | 10.86 ms  | 10.77 ms  | 104.92 ms | 144.29 ms |
| get       | 232 µs    | 244 µs    | 4.53 ms   | 4.53 ms   | 512.87 ms | 590.45 ms |
| delete    | 503 µs    | 499 µs    | 8.40 ms   | 7.97 ms   | 59.38 ms  | 58.14 ms  |
| scan      | 120 µs    | 135 µs    | 2.15 ms   | 1.82 ms   | 22.08 ms  | 22.24 ms  |
| batch     | 549 µs    | 560 µs    | 8.39 ms   | 8.34 ms   | 87.97 ms  | 127.88 ms |
| keys      | 126 µs    | 128 µs    | 2.75 ms   | 1.93 ms   | 22.42 ms  | 22.88 ms  |
| flush     | 1.33 ms   | 1.82 ms   | 6.75 ms   | 13.70 ms  | 3.14 ms   | 6.75 ms   |
| get_sst   | 3.81 ms   | 4.02 ms   | 52.71 ms  | 59.82 ms  | 612.60 ms | 667.23 ms |
| get_cpt   | 3.44 ms   | 3.76 ms   | 51.06 ms  | 59.07 ms  | 591.53 ms | 650.99 ms |
| put_obj   | 347.16 ms | 394.31 ms | 6.84 s    | 6.96 s    | 41.41 s   | 41.47 s   |
| get_obj   | 7.82 ms   | 7.82 ms   | 127.44 ms | 127.47 ms | 1.19 s    | 1.24 s    |

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
| 1K  | 749 µs   | 1.80 ms  | 3.12 ms  | 1.93 ms  |
| 8K  | 6.02 ms  | 12.24 ms | 26.14 ms | 15.05 ms |
| 16K | 12.20 ms | 25.13 ms | 55.25 ms | 29.91 ms |
| 1M  | 2.31 s   | 1.99 s   | 4.10 s   | 2.02 s   |

### Random Get

| N   | rKV     | redb      | sled    | fjall   |
| --- | ------- | --------- | ------- | ------- |
| 1K  | 275 µs  | 224 µs    | 442 µs  | 380 µs  |
| 8K  | 2.33 ms | 2.66 ms   | 3.88 ms | 4.36 ms |
| 16K | 5.16 ms | 5.77 ms   | 8.46 ms | 8.98 ms |
| 1M  | 10.42 s | 990.24 ms | 1.73 s  | 5.98 s  |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 553 µs    | 1.70 ms  | 1.89 ms  | 1.95 ms  |
| 8K  | 4.07 ms   | 11.76 ms | 17.45 ms | 15.79 ms |
| 16K | 8.48 ms   | 24.08 ms | 35.20 ms | 32.12 ms |
| 1M  | 772.78 ms | 1.81 s   | 4.96 s   | 1.96 s   |

### Forward Scan

| N   | rKV       | redb     | sled      | fjall     |
| --- | --------- | -------- | --------- | --------- |
| 1K  | 166 µs    | 76 µs    | 355 µs    | 172 µs    |
| 8K  | 1.14 ms   | 565 µs   | 2.18 ms   | 1.13 ms   |
| 16K | 2.34 ms   | 1.16 ms  | 4.28 ms   | 2.33 ms   |
| 1M  | 234.17 ms | 99.02 ms | 329.31 ms | 316.09 ms |

### Batch Write

| N   | rKV     | redb      | sled     | fjall     |
| --- | ------- | --------- | -------- | --------- |
| 1K  | 632 µs  | 5.83 ms   | 4.68 ms  | 534 µs    |
| 8K  | 4.59 ms | 61.52 ms  | 38.68 ms | 4.17 ms   |
| 16K | 8.82 ms | 113.53 ms | 79.36 ms | 8.38 ms   |
| 1M  | 2.06 s  | 8.16 s    | 5.12 s   | 634.41 ms |

### Reproduce

```sh
make bench-compare
```
