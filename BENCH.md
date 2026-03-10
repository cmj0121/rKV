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

| Operation | 1K        | 8K       | 16K       | 1M        |
| --------- | --------- | -------- | --------- | --------- |
| put       | 701 µs    | 7.66 ms  | 14.66 ms  | 2.39 s    |
| get       | 239 µs    | 2.43 ms  | 5.72 ms   | 3.32 s    |
| delete    | 424 µs    | 3.63 ms  | 7.48 ms   | 819.17 ms |
| scan      | 128 µs    | 996 µs   | 2.48 ms   | 240.00 ms |
| batch     | 561 µs    | 3.84 ms  | 7.58 ms   | 1.76 s    |
| keys      | 161 µs    | 1.01 ms  | 2.50 ms   | 237.12 ms |
| flush     | 1.30 ms   | 4.10 ms  | 6.80 ms   | 6.08 ms   |
| get_sst   | 697 µs    | 5.22 ms  | 11.12 ms  | 3.17 s    |
| get_cpt   | 571 µs    | 5.29 ms  | 11.24 ms  | 2.99 s    |
| put_obj   | 332.85 ms | 2.43 s   | 4.76 s    | 302.29 s  |
| get_obj   | 8.01 ms   | 64.26 ms | 129.15 ms | 12.38 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 604 µs | 3.09 ms | 6.92 ms | 707.70 ms |
| get       | 253 µs | 2.33 ms | 4.97 ms | 856.92 ms |
| delete    | 250 µs | 2.25 ms | 5.01 ms | 473.72 ms |
| scan      | 122 µs | 1.04 ms | 1.91 ms | 207.72 ms |
| batch     | 386 µs | 5.90 ms | 4.51 ms | 398.34 ms |
| keys      | 128 µs | 1.38 ms | 2.21 ms | 196.24 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)    | 1K (R)    | 16K (B)   | 16K (R)   | 100K (B)  | 100K (R)  |
| --------- | --------- | --------- | --------- | --------- | --------- | --------- |
| put       | 699 µs    | 695 µs    | 10.32 ms  | 9.88 ms   | 99.06 ms  | 146.19 ms |
| get       | 239 µs    | 256 µs    | 6.38 ms   | 7.35 ms   | 113.48 ms | 157.70 ms |
| delete    | 401 µs    | 417 µs    | 8.64 ms   | 7.89 ms   | 55.77 ms  | 58.38 ms  |
| scan      | 131 µs    | 125 µs    | 2.32 ms   | 2.38 ms   | 22.57 ms  | 22.46 ms  |
| batch     | 502 µs    | 562 µs    | 7.54 ms   | 7.31 ms   | 86.37 ms  | 125.76 ms |
| keys      | 127 µs    | 137 µs    | 2.39 ms   | 2.06 ms   | 22.87 ms  | 22.59 ms  |
| flush     | 1.19 ms   | 1.75 ms   | 7.47 ms   | 14.63 ms  | 3.37 ms   | 6.72 ms   |
| get_sst   | 580 µs    | 1.08 ms   | 10.85 ms  | 18.82 ms  | 131.50 ms | 203.85 ms |
| get_cpt   | 591 µs    | 1.08 ms   | 11.16 ms  | 18.85 ms  | 133.80 ms | 179.52 ms |
| put_obj   | 340.76 ms | 311.16 ms | 4.84 s    | 4.89 s    | 30.60 s   | 30.61 s   |
| get_obj   | 8.02 ms   | 8.02 ms   | 131.53 ms | 132.14 ms | 929.63 ms | 955.60 ms |

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
| 1K  | 710 µs   | 1.82 ms  | 3.18 ms  | 1.93 ms  |
| 8K  | 6.76 ms  | 12.44 ms | 26.66 ms | 15.37 ms |
| 16K | 11.26 ms | 25.67 ms | 58.15 ms | 30.51 ms |
| 1M  | 2.40 s   | 1.97 s   | 4.39 s   | 2.06 s   |

### Random Get

| N   | rKV     | redb    | sled    | fjall    |
| --- | ------- | ------- | ------- | -------- |
| 1K  | 260 µs  | 252 µs  | 426 µs  | 401 µs   |
| 8K  | 3.18 ms | 2.74 ms | 4.12 ms | 4.28 ms  |
| 16K | 6.79 ms | 6.02 ms | 9.05 ms | 13.34 ms |
| 1M  | 3.59 s  | 1.17 s  | 2.07 s  | 6.98 s   |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall    |
| --- | --------- | -------- | -------- | -------- |
| 1K  | 480 µs    | 1.72 ms  | 1.93 ms  | 1.97 ms  |
| 8K  | 3.77 ms   | 12.05 ms | 18.21 ms | 15.92 ms |
| 16K | 8.01 ms   | 24.25 ms | 37.62 ms | 32.43 ms |
| 1M  | 736.49 ms | 1.82 s   | 5.38 s   | 1.97 s   |

### Forward Scan

| N   | rKV       | redb      | sled      | fjall     |
| --- | --------- | --------- | --------- | --------- |
| 1K  | 149 µs    | 78 µs     | 327 µs    | 161 µs    |
| 8K  | 1.39 ms   | 668 µs    | 2.12 ms   | 1.14 ms   |
| 16K | 3.00 ms   | 1.20 ms   | 4.56 ms   | 2.44 ms   |
| 1M  | 235.76 ms | 101.92 ms | 336.14 ms | 325.26 ms |

### Batch Write

| N   | rKV     | redb      | sled     | fjall     |
| --- | ------- | --------- | -------- | --------- |
| 1K  | 599 µs  | 5.73 ms   | 4.72 ms  | 502 µs    |
| 8K  | 4.38 ms | 45.48 ms  | 41.59 ms | 4.23 ms   |
| 16K | 8.19 ms | 100.82 ms | 83.71 ms | 8.41 ms   |
| 1M  | 1.85 s  | 7.01 s    | 5.53 s   | 663.64 ms |

### Reproduce

```sh
make bench-compare
```
