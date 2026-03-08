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
| flush     | Flush N keys from MemTable to L0 SSTable                      |
| get_sst   | Random reads of N keys from SSTable (after flush)             |
| put_obj   | Sequential inserts of N keys with 4 KB values via ObjectStore |
| get_obj   | Random reads of N keys resolved from ObjectStore              |

**In-memory** variants run the same operations with `Config::in_memory()` (no disk).

## Results (Disk)

| Operation | 1K        | 8K       | 16K       | 1M        |
| --------- | --------- | -------- | --------- | --------- |
| put       | 751 µs    | 8.22 ms  | 15.30 ms  | 2.20 s    |
| get       | 241 µs    | 2.21 ms  | 4.70 ms   | 9.42 s    |
| delete    | 509 µs    | 4.11 ms  | 8.24 ms   | 872.31 ms |
| scan      | 128 µs    | 1.02 ms  | 1.96 ms   | 234.96 ms |
| flush     | 1.34 ms   | 5.41 ms  | 9.43 ms   | 6.61 ms   |
| get_sst   | 4.01 ms   | 26.41 ms | 54.78 ms  | 10.54 s   |
| put_obj   | 289.52 ms | 2.28 s   | 4.55 s    | 281.11 s  |
| get_obj   | 7.80 ms   | 63.03 ms | 126.75 ms | 23.54 s   |

## Results (In-Memory)

> Pure in-memory mode — no disk I/O, no AOL, no SSTables.

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 633 µs | 3.60 ms | 7.53 ms | 733.87 ms |
| get       | 270 µs | 2.52 ms | 6.14 ms | 782.47 ms |
| delete    | 293 µs | 2.62 ms | 5.54 ms | 538.49 ms |
| scan      | 123 µs | 1.08 ms | 2.14 ms | 203.64 ms |

## Reproduce

```sh
make bench
```
