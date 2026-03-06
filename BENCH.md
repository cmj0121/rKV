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
| Date   | 2026-03-06                      |

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
| put       | 770 µs    | 7.96 ms  | 15.55 ms  | 2.19 s    |
| get       | 243 µs    | 2.25 ms  | 4.74 ms   | 8.72 s    |
| delete    | 470 µs    | 3.96 ms  | 7.94 ms   | 868.32 ms |
| scan      | 117 µs    | 868 µs   | 1.72 ms   | 218.70 ms |
| flush     | 1.59 ms   | 3.96 ms  | 6.68 ms   | 5.93 ms   |
| get_sst   | 3.38 ms   | 25.90 ms | 53.39 ms  | 9.22 s    |
| put_obj   | 276.44 ms | 2.10 s   | 4.39 s    | 274.39 s  |
| get_obj   | 7.86 ms   | 63.13 ms | 126.87 ms | 20.46 s   |

## Reproduce

```sh
make bench
```
