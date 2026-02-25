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
| put       | 454 µs    | 4.75 ms   | 6.76 ms   | 774.73 ms |
| get       | 129 µs    | 2.44 ms   | 6.13 ms   | 718.19 ms |
| delete    | 250 µs    | 4.47 ms   | 4.59 ms   | 476.48 ms |
| scan      | 41 µs     | 161 µs    | 309 µs    | 26.51 ms  |
| put_obj   | 169.83 ms | 1.53 s    | 2.78 s    | 454.84 s  |
| get_obj   | 21.73 ms  | 181.40 ms | 917.89 ms | 392.21 s  |

## Reproduce

```sh
make bench
```
