# Rill Benchmark

> Push and pop throughput for the rill message queue (in-memory rKV backend).

## Environment

| Field  | Value                                    |
| ------ | ---------------------------------------- |
| OS     | macos aarch64                            |
| CPU    | Apple M2 Pro                             |
| Cores  | 10                                       |
| Memory | 16 GB                                    |
| Rust   | 1.90.0 (1159e78c4 2025-09-14) (Homebrew) |
| Date   | 2026-03-13                               |

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

| Operation  | 1K       | 10K    | 100K   | 1M     |
| ---------- | -------- | ------ | ------ | ------ |
| push       | 978.0K/s | 1.1M/s | 1.3M/s | 1.1M/s |
| push(×10)  | 1.6M/s   | 2.2M/s | 2.1M/s | 1.7M/s |
| push(×50)  | 1.2M/s   | 2.5M/s | 2.3M/s | 1.9M/s |
| push(×100) | 1.2M/s   | 2.7M/s | 2.6M/s | 2.0M/s |

## Pop Rate

> Pop uses smaller sizes because `pop_first` rebuilds the merge iterator each call,
> scanning accumulated tombstones — O(n²) for draining a full queue.

| Operation | 100      | 500     | 1K      | 5K     |
| --------- | -------- | ------- | ------- | ------ |
| pop       | 137.0K/s | 36.1K/s | 17.9K/s | 3.7K/s |
| pop(×10)  | 161.6K/s | 37.6K/s | 18.3K/s | 3.5K/s |
| pop(×50)  | 170.1K/s | 36.9K/s | 18.4K/s | 3.7K/s |
| pop(×100) | 159.3K/s | 37.2K/s | 18.2K/s | 3.7K/s |

### Timing Details

#### Push

| Operation  | 1K      | 10K     | 100K     | 1M        |
| ---------- | ------- | ------- | -------- | --------- |
| push       | 1.02 ms | 8.95 ms | 78.81 ms | 943.96 ms |
| push(×10)  | 608 µs  | 4.45 ms | 47.00 ms | 598.09 ms |
| push(×50)  | 834 µs  | 3.96 ms | 43.18 ms | 527.13 ms |
| push(×100) | 813 µs  | 3.71 ms | 38.92 ms | 496.37 ms |

#### Pop

| Operation | 100    | 500      | 1K       | 5K     |
| --------- | ------ | -------- | -------- | ------ |
| pop       | 730 µs | 13.85 ms | 55.95 ms | 1.34 s |
| pop(×10)  | 618 µs | 13.30 ms | 54.58 ms | 1.44 s |
| pop(×50)  | 587 µs | 13.54 ms | 54.40 ms | 1.36 s |
| pop(×100) | 627 µs | 13.45 ms | 54.99 ms | 1.36 s |

## Mixed Workload

| Operation | 1K                 | 5K                  | 10K                 |
| --------- | ------------------ | ------------------- | ------------------- |
| mixed     | 7.00 ms (142.9K/s) | 168.94 ms (29.6K/s) | 650.89 ms (15.4K/s) |

## Reproduce

```sh
cargo run --release --bin rill-bench
```
