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
| Date   | 2026-03-04                      |

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
| put       | 647 µs   | 6.93 ms   | 13.90 ms  | 2.60 s    |
| get       | 158 µs   | 1.57 ms   | 3.31 ms   | 10.33 s   |
| delete    | 372 µs   | 3.12 ms   | 5.75 ms   | 1.09 s    |
| scan      | 154 µs   | 1.31 ms   | 2.68 ms   | 601.99 ms |
| flush     | 3.67 ms  | 3.81 ms   | 6.53 ms   | 8.40 ms   |
| get_sst   | 3.68 ms  | 26.86 ms  | 54.37 ms  | 11.67 s   |
| put_obj   | 55.63 ms | 407.43 ms | 877.21 ms | 151.18 s  |
| get_obj   | 20.60 ms | 170.43 ms | 341.64 ms | 226.91 s  |

## Reproduce

```sh
make bench
```
