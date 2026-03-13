# Benchmark

> Performance of core rKV operations (MemTable and SSTable paths).

## Environment

| Field  | Value                                         |
| ------ | --------------------------------------------- |
| OS     | linux x86_64                                  |
| CPU    | Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz |
| Cores  | 4                                             |
| Memory | 15 GB                                         |
| Rust   | 1.94.0 (4a4ef493e 2026-03-02)                 |
| Date   | 2026-03-13                                    |

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

| Operation | 1K      | 8K       | 16K       | 1M        |
| --------- | ------- | -------- | --------- | --------- |
| put       | 756 µs  | 6.56 ms  | 12.34 ms  | 2.06 s    |
| get       | 217 µs  | 2.32 ms  | 5.02 ms   | 2.37 s    |
| delete    | 431 µs  | 3.92 ms  | 7.63 ms   | 731.13 ms |
| scan      | 127 µs  | 940 µs   | 1.90 ms   | 238.81 ms |
| batch     | 498 µs  | 4.27 ms  | 7.42 ms   | 1.53 s    |
| keys      | 126 µs  | 1.03 ms  | 1.98 ms   | 233.91 ms |
| flush     | 1.23 ms | 3.90 ms  | 6.53 ms   | 6.13 ms   |
| get_sst   | 536 µs  | 4.63 ms  | 10.29 ms  | 2.43 s    |
| get_cpt   | 540 µs  | 4.67 ms  | 9.83 ms   | 2.34 s    |
| put_obj   | 9.35 ms | 65.46 ms | 128.36 ms | 9.07 s    |
| get_obj   | 3.78 ms | 30.97 ms | 63.32 ms  | 6.05 s    |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 602 µs | 3.67 ms | 7.73 ms | 632.48 ms |
| get       | 228 µs | 2.72 ms | 5.63 ms | 749.54 ms |
| delete    | 351 µs | 2.55 ms | 5.11 ms | 478.30 ms |
| scan      | 119 µs | 981 µs  | 2.06 ms | 166.06 ms |
| batch     | 427 µs | 4.36 ms | 5.11 ms | 441.29 ms |
| keys      | 129 µs | 986 µs  | 1.88 ms | 154.59 ms |

## Filter Comparison (Bloom vs Ribbon)

> Same operations with different filter policies.
> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).

| Operation | 1K (B)  | 1K (R)  | 16K (B)   | 16K (R)   | 100K (B)  | 100K (R)  |
| --------- | ------- | ------- | --------- | --------- | --------- | --------- |
| put       | 788 µs  | 697 µs  | 10.48 ms  | 10.02 ms  | 98.42 ms  | 151.27 ms |
| get       | 228 µs  | 224 µs  | 4.88 ms   | 4.85 ms   | 73.55 ms  | 126.20 ms |
| delete    | 422 µs  | 460 µs  | 7.33 ms   | 6.98 ms   | 57.12 ms  | 58.80 ms  |
| scan      | 121 µs  | 118 µs  | 1.99 ms   | 1.95 ms   | 20.91 ms  | 21.35 ms  |
| batch     | 480 µs  | 510 µs  | 7.79 ms   | 7.36 ms   | 77.36 ms  | 134.12 ms |
| keys      | 164 µs  | 164 µs  | 2.14 ms   | 1.93 ms   | 21.59 ms  | 21.44 ms  |
| flush     | 1.28 ms | 1.63 ms | 6.75 ms   | 15.84 ms  | 2.81 ms   | 7.16 ms   |
| get_sst   | 552 µs  | 1.07 ms | 9.86 ms   | 18.76 ms  | 84.99 ms  | 144.88 ms |
| get_cpt   | 532 µs  | 1.08 ms | 9.77 ms   | 18.65 ms  | 79.17 ms  | 135.85 ms |
| put_obj   | 8.79 ms | 9.01 ms | 129.47 ms | 131.40 ms | 845.67 ms | 882.18 ms |
| get_obj   | 3.72 ms | 3.71 ms | 64.85 ms  | 63.46 ms  | 452.07 ms | 504.11 ms |

## Reproduce

```sh
make bench
```

## Comparison

> rKV vs redb vs sled vs fjall — same pre-defined dataset.

### Environment

| Field  | Value                                         |
| ------ | --------------------------------------------- |
| OS     | linux x86_64                                  |
| CPU    | Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz |
| Cores  | 4                                             |
| Memory | 15 GB                                         |
| Rust   | 1.94.0 (4a4ef493e 2026-03-02)                 |
| Date   | 2026-03-13                                    |

### Sequential Put

| N   | rKV      | redb     | sled     | fjall    |
| --- | -------- | -------- | -------- | -------- |
| 1K  | 701 µs   | 2.14 ms  | 5.00 ms  | 1.03 ms  |
| 8K  | 5.99 ms  | 15.21 ms | 23.92 ms | 7.56 ms  |
| 16K | 11.01 ms | 31.70 ms | 51.48 ms | 15.23 ms |
| 1M  | 2.05 s   | 2.26 s   | 3.91 s   | 1.07 s   |

### Random Get

| N   | rKV     | redb      | sled    | fjall   |
| --- | ------- | --------- | ------- | ------- |
| 1K  | 245 µs  | 222 µs    | 458 µs  | 390 µs  |
| 8K  | 2.80 ms | 2.63 ms   | 4.12 ms | 4.15 ms |
| 16K | 5.97 ms | 5.48 ms   | 8.63 ms | 9.00 ms |
| 1M  | 2.51 s  | 926.72 ms | 1.68 s  | 5.50 s  |

### Sequential Delete

| N   | rKV       | redb     | sled     | fjall     |
| --- | --------- | -------- | -------- | --------- |
| 1K  | 458 µs    | 1.71 ms  | 1.84 ms  | 1.00 ms   |
| 8K  | 3.81 ms   | 13.90 ms | 16.57 ms | 8.12 ms   |
| 16K | 7.78 ms   | 28.32 ms | 34.80 ms | 16.74 ms  |
| 1M  | 709.51 ms | 2.06 s   | 6.04 s   | 988.00 ms |

### Forward Scan

| N   | rKV       | redb      | sled      | fjall     |
| --- | --------- | --------- | --------- | --------- |
| 1K  | 179 µs    | 94 µs     | 245 µs    | 148 µs    |
| 8K  | 1.11 ms   | 730 µs    | 2.02 ms   | 1.18 ms   |
| 16K | 2.06 ms   | 1.47 ms   | 3.85 ms   | 2.33 ms   |
| 1M  | 234.31 ms | 116.71 ms | 308.89 ms | 277.39 ms |

### Batch Write

| N   | rKV     | redb     | sled     | fjall     |
| --- | ------- | -------- | -------- | --------- |
| 1K  | 672 µs  | 4.74 ms  | 4.52 ms  | 496 µs    |
| 8K  | 4.20 ms | 40.31 ms | 38.08 ms | 4.00 ms   |
| 16K | 8.12 ms | 77.43 ms | 76.01 ms | 8.41 ms   |
| 1M  | 1.55 s  | 5.47 s   | 5.03 s   | 610.78 ms |

### Reproduce

```sh
make bench-compare
```
