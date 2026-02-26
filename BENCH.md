# Benchmark

> Performance of core rKV operations (MemTable and SSTable paths).

## Environment

| Field  | Value                           |
| ------ | ------------------------------- |
| OS     | linux x86_64                    |
| CPU    | AMD EPYC 7763 64-Core Processor |
| Cores  | 2                               |
| Memory | 7 GB                            |
| Rust   | 1.93.1 (01f6ddf75 2026-02-11)   |
| Date   | 2026-02-26                      |

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

| Operation | 1K       | 8K        | 16K       | 1M        |
| --------- | -------- | --------- | --------- | --------- |
| put       | 685 µs   | 7.14 ms   | 13.71 ms  | 909.07 ms |
| get       | 197 µs   | 2.09 ms   | 4.74 ms   | 700.01 ms |
| delete    | 372 µs   | 3.06 ms   | 6.17 ms   | 555.23 ms |
| scan      | 165 µs   | 2.34 ms   | 4.89 ms   | 329.60 ms |
| flush     | 618 µs   | 3.16 ms   | 5.51 ms   | 463.91 ms |
| get_sst   | 4.22 ms  | 33.70 ms  | 67.72 ms  | 15.27 s   |
| put_obj   | 58.32 ms | 440.27 ms | 888.74 ms | 148.92 s  |
| get_obj   | 12.88 ms | 104.78 ms | 210.07 ms | 195.77 s  |

## Reproduce

```sh
make bench
```
