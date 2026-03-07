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
| Date   | 2026-03-07                      |

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

## Results

| Operation | 1K        | 8K       | 16K       | 1M        |
| --------- | --------- | -------- | --------- | --------- |
| put       | 759 µs    | 8.13 ms  | 15.44 ms  | 2.23 s    |
| get       | 234 µs    | 2.31 ms  | 4.73 ms   | 8.60 s    |
| delete    | 489 µs    | 3.85 ms  | 8.08 ms   | 857.16 ms |
| scan      | 157 µs    | 959 µs   | 1.93 ms   | 234.63 ms |
| flush     | 1.30 ms   | 3.67 ms  | 6.49 ms   | 6.51 ms   |
| get_sst   | 3.72 ms   | 26.07 ms | 52.47 ms  | 9.48 s    |
| put_obj   | 371.34 ms | 2.34 s   | 4.62 s    | 290.52 s  |
| get_obj   | 7.92 ms   | 63.74 ms | 128.04 ms | 20.34 s   |

## Reproduce

```sh
make bench
```
