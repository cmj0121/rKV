# Rill Benchmark

> Push and pop throughput for the rill message queue (in-memory rKV backend).

## Environment

| Field  | Value                                         |
| ------ | --------------------------------------------- |
| OS     | linux x86_64                                  |
| CPU    | Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz |
| Cores  | 4                                             |
| Memory | 15 GB                                         |
| Rust   | 1.94.0 (4a4ef493e 2026-03-02)                 |
| Date   | 2026-03-13                                    |

## Methodology

Each operation runs against a fresh in-memory rKV database in release mode.
Wall-clock time measured via `std::time::Instant`.

| Operation | Description                                       |
| --------- | ------------------------------------------------- |
| push      | Sequential single-message pushes                  |
| push(×N)  | Batch push with N messages per WriteBatch         |
| pop       | Sequential single-message pops (pre-filled queue) |
| pop(×N)   | Batch pop of N messages per call                  |
| mixed     | 60% push / 40% pop random interleaved workload    |

## Push Rate

| Operation  | 1K       | 10K      | 100K     | 1M       |
| ---------- | -------- | -------- | -------- | -------- |
| push       | 802.5K/s | 808.1K/s | 775.8K/s | 638.4K/s |
| push(×10)  | 1.5M/s   | 349.0K/s | 1.1M/s   | 851.3K/s |
| push(×50)  | 1.5M/s   | 1.5M/s   | 1.3M/s   | 971.5K/s |
| push(×100) | 1.5M/s   | 1.5M/s   | 1.4M/s   | 980.7K/s |

## Pop Rate

> Pop uses smaller sizes because `pop_first` rebuilds the merge iterator each call,
> scanning accumulated tombstones — O(n²) for draining a full queue.

| Operation | 100     | 500     | 1K     | 5K     |
| --------- | ------- | ------- | ------ | ------ |
| pop       | 47.8K/s | 11.5K/s | 5.4K/s | 1.1K/s |
| pop(×10)  | 47.2K/s | 10.4K/s | 5.4K/s | 1.1K/s |
| pop(×50)  | 48.1K/s | 10.5K/s | 5.4K/s | 1.1K/s |
| pop(×100) | 48.8K/s | 10.4K/s | 5.4K/s | 1.1K/s |

### Timing Details

#### Push

| Operation  | 1K      | 10K      | 100K      | 1M     |
| ---------- | ------- | -------- | --------- | ------ |
| push       | 1.25 ms | 12.37 ms | 128.90 ms | 1.57 s |
| push(×10)  | 658 µs  | 28.65 ms | 93.60 ms  | 1.17 s |
| push(×50)  | 682 µs  | 6.73 ms  | 75.27 ms  | 1.03 s |
| push(×100) | 657 µs  | 6.66 ms  | 73.49 ms  | 1.02 s |

#### Pop

| Operation | 100     | 500      | 1K        | 5K     |
| --------- | ------- | -------- | --------- | ------ |
| pop       | 2.09 ms | 43.40 ms | 186.06 ms | 4.66 s |
| pop(×10)  | 2.12 ms | 47.89 ms | 184.94 ms | 4.73 s |
| pop(×50)  | 2.08 ms | 47.43 ms | 184.61 ms | 4.64 s |
| pop(×100) | 2.05 ms | 47.90 ms | 185.11 ms | 4.71 s |

## Mixed Workload

| Operation | 1K                 | 5K                 | 10K             |
| --------- | ------------------ | ------------------ | --------------- |
| mixed     | 23.43 ms (42.7K/s) | 614.94 ms (8.1K/s) | 2.50 s (4.0K/s) |

## Reproduce

```sh
cargo run --release --bin rill-bench
```
