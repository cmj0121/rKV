# Benchmark

> MemTable-backed (in-memory) performance of core rKV operations.

## Environment

| Field  | Value                                    |
| ------ | ---------------------------------------- |
| OS     | macos aarch64                            |
| CPU    | Apple M2 Pro                             |
| Cores  | 10                                       |
| Memory | 16 GB                                    |
| Rust   | 1.90.0 (1159e78c4 2025-09-14) (Homebrew) |
| Date   | 2026-02-25                               |

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

| Operation | 1K        | 8K        | 16K       | 1M        |
| --------- | --------- | --------- | --------- | --------- |
| put       | 460 µs    | 6.53 ms   | 14.72 ms  | 918.81 ms |
| get       | 141 µs    | 4.91 ms   | 7.21 ms   | 723.60 ms |
| delete    | 238 µs    | 2.22 ms   | 4.10 ms   | 727.89 ms |
| scan      | 16 µs     | 138 µs    | 343 µs    | 28.27 ms  |
| put_obj   | 176.85 ms | 1.32 s    | 2.97 s    | 441.35 s  |
| get_obj   | 21.59 ms  | 180.89 ms | 516.36 ms | 352.86 s  |

## Reproduce

```sh
make bench
```
