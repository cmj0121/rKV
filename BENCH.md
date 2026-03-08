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
| put       | 778 µs    | 8.03 ms  | 15.53 ms  | 2.31 s    |
| get       | 236 µs    | 2.26 ms  | 4.81 ms   | 12.57 s   |
| delete    | 517 µs    | 4.23 ms  | 8.82 ms   | 880.02 ms |
| scan      | 126 µs    | 1.01 ms  | 2.21 ms   | 228.29 ms |
| batch     | 621 µs    | 4.40 ms  | 8.84 ms   | 2.11 s    |
| keys      | 130 µs    | 1.24 ms  | 2.60 ms   | 236.22 ms |
| flush     | 1.61 ms   | 3.98 ms  | 6.99 ms   | 6.97 ms   |
| get_sst   | 3.76 ms   | 26.41 ms | 52.40 ms  | 11.24 s   |
| get_cpt   | 3.34 ms   | 25.68 ms | 50.59 ms  | 11.12 s   |
| put_obj   | 517.24 ms | 3.07 s   | 6.99 s    | 459.94 s  |
| get_obj   | 7.93 ms   | 64.27 ms | 128.05 ms | 20.43 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 545 µs | 3.57 ms | 7.07 ms | 702.52 ms |
| get       | 255 µs | 2.25 ms | 4.61 ms | 700.69 ms |
| delete    | 306 µs | 2.45 ms | 4.96 ms | 472.10 ms |
| scan      | 134 µs | 963 µs  | 1.89 ms | 195.21 ms |
| batch     | 490 µs | 2.86 ms | 5.26 ms | 416.77 ms |
| keys      | 141 µs | 965 µs  | 1.94 ms | 163.57 ms |

## Reproduce

```sh
make bench
```
