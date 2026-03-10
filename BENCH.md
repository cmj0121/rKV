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
| Date   | 2026-03-10                      |

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

| Operation | 1K       | 8K        | 16K       | 1M        |
| --------- | -------- | --------- | --------- | --------- |
| put       | 645 µs   | 7.41 ms   | 14.26 ms  | 2.08 s    |
| get       | 265 µs   | 2.28 ms   | 4.74 ms   | 2.32 s    |
| delete    | 424 µs   | 3.37 ms   | 6.74 ms   | 833.82 ms |
| scan      | 126 µs   | 980 µs    | 2.70 ms   | 238.22 ms |
| batch     | 551 µs   | 3.97 ms   | 7.73 ms   | 1.65 s    |
| keys      | 160 µs   | 1.05 ms   | 2.35 ms   | 377.83 ms |
| flush     | 1.11 ms  | 3.91 ms   | 7.31 ms   | 454.56 ms |
| get_sst   | 756 µs   | 4.95 ms   | 10.69 ms  | 2.53 s    |
| get_cpt   | 636 µs   | 4.89 ms   | 10.44 ms  | 2.42 s    |
| put_obj   | 13.00 ms | 104.87 ms | 192.89 ms | 15.39 s   |
| get_obj   | 7.71 ms  | 63.25 ms  | 126.55 ms | 10.63 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 539 µs | 3.09 ms | 6.55 ms | 629.26 ms |
| get       | 245 µs | 2.32 ms | 4.66 ms | 739.90 ms |
| delete    | 257 µs | 2.15 ms | 4.26 ms | 448.63 ms |
| scan      | 149 µs | 927 µs  | 1.90 ms | 201.99 ms |
| batch     | 440 µs | 5.83 ms | 4.55 ms | 387.93 ms |
| keys      | 129 µs | 1.03 ms | 1.90 ms | 193.16 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)   | 1K (R)   | 16K (B)   | 16K (R)   | 100K (B)  | 100K (R)  |
| --------- | -------- | -------- | --------- | --------- | --------- | --------- |
| put       | 684 µs   | 608 µs   | 9.57 ms   | 9.49 ms   | 105.65 ms | 138.33 ms |
| get       | 239 µs   | 236 µs   | 4.56 ms   | 4.63 ms   | 82.19 ms  | 136.50 ms |
| delete    | 389 µs   | 389 µs   | 8.17 ms   | 8.37 ms   | 53.46 ms  | 53.88 ms  |
| scan      | 122 µs   | 123 µs   | 2.67 ms   | 2.05 ms   | 23.04 ms  | 22.14 ms  |
| batch     | 497 µs   | 471 µs   | 7.43 ms   | 7.57 ms   | 82.75 ms  | 130.23 ms |
| keys      | 128 µs   | 126 µs   | 2.84 ms   | 2.82 ms   | 21.84 ms  | 22.30 ms  |
| flush     | 1.19 ms  | 1.69 ms  | 7.75 ms   | 14.54 ms  | 3.45 ms   | 7.42 ms   |
| get_sst   | 551 µs   | 1.04 ms  | 10.48 ms  | 18.41 ms  | 102.45 ms | 164.68 ms |
| get_cpt   | 556 µs   | 1.03 ms  | 10.51 ms  | 19.30 ms  | 126.05 ms | 173.42 ms |
| put_obj   | 12.83 ms | 12.78 ms | 193.62 ms | 193.85 ms | 1.28 s    | 1.28 s    |
| get_obj   | 8.11 ms  | 8.17 ms  | 132.66 ms | 132.24 ms | 926.92 ms | 954.63 ms |

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
| Date   | 2026-03-10                      |

### Sequential Put

| N   | rKV      | redb     | sled     | fjall    |
| --- | -------- | -------- | -------- | -------- |
| 1K  | 669 µs   | 1.68 ms  | 3.67 ms  | 1.94 ms  |
| 8K  | 6.95 ms  | 12.34 ms | 25.85 ms | 15.12 ms |
| 16K | 10.68 ms | 25.16 ms | 55.02 ms | 30.09 ms |
| 1M  | 1.94 s   | 1.95 s   | 4.14 s   | 2.03 s   |

### Random Get

| N   | rKV     | redb    | sled    | fjall   |
| --- | ------- | ------- | ------- | ------- |
| 1K  | 252 µs  | 220 µs  | 404 µs  | 399 µs  |
| 8K  | 2.50 ms | 2.67 ms | 3.96 ms | 4.16 ms |
| 16K | 6.17 ms | 5.85 ms | 8.44 ms | 9.26 ms |
| 1M  | 2.73 s  | 1.04 s  | 1.83 s  | 6.45 s  |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 431 µs    | 1.57 ms  | 1.88 ms  | 1.94 ms  |
| 8K  | 3.73 ms   | 11.81 ms | 18.11 ms | 16.11 ms |
| 16K | 7.78 ms   | 24.23 ms | 38.14 ms | 32.66 ms |
| 1M  | 737.46 ms | 1.81 s   | 5.31 s   | 1.94 s   |

### Forward Scan

| N   | rKV       | redb      | sled      | fjall     |
| --- | --------- | --------- | --------- | --------- |
| 1K  | 135 µs    | 75 µs     | 336 µs    | 152 µs    |
| 8K  | 1.13 ms   | 655 µs    | 2.13 ms   | 1.22 ms   |
| 16K | 2.29 ms   | 1.15 ms   | 4.32 ms   | 2.26 ms   |
| 1M  | 236.35 ms | 101.28 ms | 341.00 ms | 335.81 ms |

### Batch Write

| N   | rKV     | redb      | sled     | fjall     |
| --- | ------- | --------- | -------- | --------- |
| 1K  | 617 µs  | 5.73 ms   | 5.11 ms  | 597 µs    |
| 8K  | 5.12 ms | 61.29 ms  | 41.13 ms | 4.20 ms   |
| 16K | 7.86 ms | 104.41 ms | 82.43 ms | 8.69 ms   |
| 1M  | 1.79 s  | 7.28 s    | 5.34 s   | 655.79 ms |

### Reproduce

```sh
make bench-compare
```
