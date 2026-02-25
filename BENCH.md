# Benchmark

> MemTable-backed (in-memory) performance of core rKV operations.

## Environment

| Field  | Value                           |
| ------ | ------------------------------- |
| OS     | linux x86_64                    |
| CPU    | AMD EPYC 7763 64-Core Processor |
| Cores  | 2                               |
| Memory | 7 GB                            |
| Rust   | 1.93.1 (01f6ddf75 2026-02-11)   |
| Date   | 2026-02-25                      |

## Methodology

Each operation runs against a fresh temporary DB in release mode.
Wall-clock time is measured via `std::time::Instant`.

| Operation | Description                                                   |
| --------- | ------------------------------------------------------------- |
| put       | Sequential inserts of N keys with 64-byte values              |
| get       | Random reads of N existing keys (shuffled order)              |
| delete    | Sequential deletes of N existing keys                         |
| scan      | Forward scan of all keys (limit=N, offset=0)                  |
| put_obj   | Sequential inserts of N keys with 4 KB values via ObjectStore |
| get_obj   | Random reads of N keys resolved from ObjectStore              |

## Results

| Operation | 1K       | 8K        | 16K       | 1M        |
| --------- | -------- | --------- | --------- | --------- |
| put       | 664 µs   | 6.51 ms   | 12.41 ms  | 837.29 ms |
| get       | 158 µs   | 1.96 ms   | 3.44 ms   | 663.95 ms |
| delete    | 381 µs   | 2.83 ms   | 5.67 ms   | 518.17 ms |
| scan      | 11 µs    | 199 µs    | 201 µs    | 37.11 ms  |
| put_obj   | 56.82 ms | 410.45 ms | 857.89 ms | 147.13 s  |
| get_obj   | 11.31 ms | 93.35 ms  | 189.17 ms | 169.13 s  |

## Reproduce

```sh
make bench
```
