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
| Date   | 2026-02-24                               |

## Methodology

Each operation runs against a fresh temporary DB in release mode.
Wall-clock time is measured via `std::time::Instant`.

| Operation | Description                                      |
| --------- | ------------------------------------------------ |
| put       | Sequential inserts of N keys with 64-byte values |
| get       | Random reads of N existing keys (shuffled order) |
| delete    | Sequential deletes of N existing keys            |
| scan      | Forward scan of all keys (limit=N, offset=0)     |

## Results

| Operation | 1K     | 8K      | 16K     | 1M        |
| --------- | ------ | ------- | ------- | --------- |
| put       | 307 µs | 2.43 ms | 4.50 ms | 346.02 ms |
| get       | 142 µs | 1.07 ms | 2.51 ms | 672.57 ms |
| delete    | 99 µs  | 851 µs  | 1.93 ms | 274.37 ms |
| scan      | 33 µs  | 84 µs   | 156 µs  | 16.47 ms  |

## Reproduce

```sh
make bench
```
