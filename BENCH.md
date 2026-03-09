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
| put       | 720 µs    | 7.56 ms  | 14.34 ms  | 2.11 s    |
| get       | 279 µs    | 2.27 ms  | 4.62 ms   | 6.37 s    |
| delete    | 450 µs    | 3.46 ms  | 7.21 ms   | 784.74 ms |
| scan      | 127 µs    | 935 µs   | 1.93 ms   | 241.86 ms |
| batch     | 488 µs    | 3.89 ms  | 7.34 ms   | 1.64 s    |
| keys      | 127 µs    | 995 µs   | 1.96 ms   | 240.22 ms |
| flush     | 1.29 ms   | 3.65 ms  | 6.55 ms   | 5.15 ms   |
| get_sst   | 626 µs    | 5.90 ms  | 12.40 ms  | 7.39 s    |
| get_cpt   | 686 µs    | 6.23 ms  | 13.20 ms  | 6.34 s    |
| put_obj   | 332.54 ms | 2.30 s   | 4.51 s    | 290.59 s  |
| get_obj   | 8.14 ms   | 64.30 ms | 127.39 ms | 15.60 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 514 µs | 3.25 ms | 6.14 ms | 655.71 ms |
| get       | 242 µs | 2.28 ms | 4.60 ms | 658.91 ms |
| delete    | 255 µs | 2.13 ms | 4.33 ms | 421.22 ms |
| scan      | 124 µs | 905 µs  | 1.81 ms | 195.46 ms |
| batch     | 463 µs | 2.32 ms | 4.72 ms | 363.07 ms |
| keys      | 132 µs | 970 µs  | 1.90 ms | 166.89 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)    | 1K (R)    | 16K (B)   | 16K (R)   | 100K (B)  | 100K (R)  |
| --------- | --------- | --------- | --------- | --------- | --------- | --------- |
| put       | 672 µs    | 626 µs    | 11.36 ms  | 9.76 ms   | 96.76 ms  | 140.10 ms |
| get       | 239 µs    | 232 µs    | 4.59 ms   | 4.57 ms   | 285.23 ms | 358.57 ms |
| delete    | 455 µs    | 431 µs    | 7.30 ms   | 7.20 ms   | 50.01 ms  | 50.20 ms  |
| scan      | 121 µs    | 124 µs    | 2.05 ms   | 1.79 ms   | 22.11 ms  | 21.83 ms  |
| batch     | 446 µs    | 488 µs    | 7.36 ms   | 7.21 ms   | 72.30 ms  | 118.75 ms |
| keys      | 126 µs    | 122 µs    | 2.16 ms   | 1.88 ms   | 22.07 ms  | 22.30 ms  |
| flush     | 1.05 ms   | 1.67 ms   | 6.40 ms   | 12.78 ms  | 2.96 ms   | 6.23 ms   |
| get_sst   | 690 µs    | 1.12 ms   | 12.25 ms  | 19.76 ms  | 351.62 ms | 412.91 ms |
| get_cpt   | 641 µs    | 1.15 ms   | 12.85 ms  | 20.44 ms  | 333.32 ms | 373.45 ms |
| put_obj   | 287.65 ms | 282.90 ms | 4.64 s    | 4.59 s    | 28.94 s   | 28.89 s   |
| get_obj   | 7.72 ms   | 7.87 ms   | 126.25 ms | 126.84 ms | 924.74 ms | 1.00 s    |

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
| 1K  | 679 µs   | 1.72 ms  | 2.69 ms  | 1.93 ms  |
| 8K  | 6.63 ms  | 12.43 ms | 26.75 ms | 16.47 ms |
| 16K | 10.69 ms | 25.05 ms | 54.92 ms | 29.87 ms |
| 1M  | 1.95 s   | 1.94 s   | 3.95 s   | 2.02 s   |

### Random Get

| N   | rKV     | redb      | sled    | fjall   |
| --- | ------- | --------- | ------- | ------- |
| 1K  | 254 µs  | 218 µs    | 419 µs  | 377 µs  |
| 8K  | 2.31 ms | 2.67 ms   | 4.02 ms | 4.07 ms |
| 16K | 5.04 ms | 5.76 ms   | 8.49 ms | 9.03 ms |
| 1M  | 6.97 s  | 857.56 ms | 1.66 s  | 5.92 s  |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 442 µs    | 2.48 ms  | 1.92 ms  | 1.96 ms  |
| 8K  | 3.37 ms   | 11.93 ms | 17.42 ms | 16.04 ms |
| 16K | 7.65 ms   | 24.06 ms | 35.47 ms | 31.81 ms |
| 1M  | 685.94 ms | 1.79 s   | 4.97 s   | 1.96 s   |

### Forward Scan

| N   | rKV       | redb     | sled      | fjall     |
| --- | --------- | -------- | --------- | --------- |
| 1K  | 142 µs    | 77 µs    | 317 µs    | 166 µs    |
| 8K  | 957 µs    | 582 µs   | 2.10 ms   | 1.12 ms   |
| 16K | 1.99 ms   | 1.17 ms  | 4.06 ms   | 2.24 ms   |
| 1M  | 234.82 ms | 98.69 ms | 315.80 ms | 311.58 ms |

### Batch Write

| N   | rKV     | redb      | sled     | fjall     |
| --- | ------- | --------- | -------- | --------- |
| 1K  | 555 µs  | 5.37 ms   | 4.14 ms  | 500 µs    |
| 8K  | 3.93 ms | 46.71 ms  | 39.36 ms | 4.20 ms   |
| 16K | 7.76 ms | 103.56 ms | 79.48 ms | 8.34 ms   |
| 1M  | 2.00 s  | 6.36 s    | 5.22 s   | 644.91 ms |

### Reproduce

```sh
make bench-compare
```
