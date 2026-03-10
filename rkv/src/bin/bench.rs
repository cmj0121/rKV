use std::io::Write;
use std::time::Instant;

use rkv::{Config, FilterPolicy, Key, WriteBatch, DB, DEFAULT_NAMESPACE};
use sysinfo::System;

const SIZES: &[usize] = &[1_000, 8_000, 16_000, 1_000_000];
const VALUE: &[u8; 64] = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn collect_machine_info() -> Vec<(&'static str, String)> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let os = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    let cpu = sys
        .cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "unknown".into());
    let cores = sys.cpus().len().to_string();
    let memory_gb = sys.total_memory() / (1024 * 1024 * 1024);
    let memory = format!("{memory_gb} GB");

    let rust_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().replace("rustc ", ""))
        .unwrap_or_else(|| "unknown".into());

    let date = chrono_free_date();

    vec![
        ("OS", os),
        ("CPU", cpu),
        ("Cores", cores),
        ("Memory", memory),
        ("Rust", rust_version),
        ("Date", date),
    ]
}

/// Returns today's date as YYYY-MM-DD without pulling in chrono.
fn chrono_free_date() -> String {
    let output = std::process::Command::new("date")
        .arg("+%Y-%m-%d")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();
    output.trim().to_string()
}

fn format_duration(d: std::time::Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us} µs")
    } else if us < 1_000_000 {
        format!("{:.2} ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2} s", us as f64 / 1_000_000.0)
    }
}

fn make_config(path: &std::path::Path, policy: FilterPolicy) -> Config {
    let mut config = Config::new(path);
    config.filter_policy = policy;
    config
}

fn bench_put(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }
    start.elapsed()
}

fn bench_get(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    // Build a shuffled index array for random reads
    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    // Verify all keys exist before timing
    let mut missing = 0;
    for &i in &indices {
        if ns.get(i).is_err() {
            if missing == 0 {
                eprintln!("  MISSING key: {i}");
            }
            missing += 1;
        }
    }
    if missing > 0 {
        panic!("{missing} keys missing out of {n}");
    }

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_delete(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    for i in 0..n {
        ns.delete(i as i64).unwrap();
    }
    start.elapsed()
}

/// Build a distinct 4 KB value for key index `i`.
/// First 8 bytes encode the index so each key produces a unique BLAKE3 hash,
/// preventing ObjectStore dedup.
fn make_object_value(i: usize) -> Vec<u8> {
    let mut v = vec![0u8; 4096];
    v[..8].copy_from_slice(&(i as u64).to_le_bytes());
    v
}

/// Sequential inserts of N distinct 4 KB values through the ObjectStore path.
/// Each value has a unique BLAKE3 hash, so N object files are created on disk.
fn bench_put_objects(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = make_config(tmp.path(), policy);
    config.object_size = 0; // force all values to bin objects
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for i in 0..n {
        let value = make_object_value(i);
        ns.put(i as i64, value.as_slice(), None).unwrap();
    }
    start.elapsed()
}

/// Random reads of N keys, each resolving a distinct bin object from disk.
fn bench_get_objects(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = make_config(tmp.path(), policy);
    config.object_size = 0;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        let value = make_object_value(i);
        ns.put(i as i64, value.as_slice(), None).unwrap();
    }

    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_flush(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    db.flush().unwrap();
    start.elapsed()
}

fn bench_get_sst(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }
    db.flush().unwrap();

    // MemTable is now empty — all reads go through SSTable
    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_scan(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let _ = ns.scan(&Key::Int(0), n, 0, false).unwrap();
    start.elapsed()
}

fn bench_get_compact(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write in batches and flush multiple times to create multiple L0 SSTables,
    // then compact to push data into deeper levels.
    let chunk = n / 4;
    for c in 0..4 {
        for i in (c * chunk)..((c + 1) * chunk) {
            ns.put(i as i64, VALUE.as_slice(), None).unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();
    db.wait_for_compaction();

    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_batch(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    // Write in batches of 100
    for chunk_start in (0..n).step_by(100) {
        let chunk_end = (chunk_start + 100).min(n);
        let mut batch = WriteBatch::new();
        for i in chunk_start..chunk_end {
            batch = batch.put(Key::Int(i as i64), VALUE.as_slice(), None);
        }
        ns.write_batch(batch).unwrap();
    }
    start.elapsed()
}

fn bench_keys(n: usize, policy: FilterPolicy) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(make_config(tmp.path(), policy)).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let count = ns.keys(&Key::Int(0)).unwrap().count();
    assert_eq!(count, n);
    start.elapsed()
}

// ---------------------------------------------------------------------------
// In-memory bench functions
// ---------------------------------------------------------------------------

const MEM_SIZES: &[usize] = &[1_000, 8_000, 16_000, 1_000_000];

fn bench_mem_put(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }
    start.elapsed()
}

fn bench_mem_get(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_mem_delete(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    for i in 0..n {
        ns.delete(i as i64).unwrap();
    }
    start.elapsed()
}

fn bench_mem_scan(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let _ = ns.scan(&Key::Int(0), n, 0, false).unwrap();
    start.elapsed()
}

fn bench_mem_batch(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for chunk_start in (0..n).step_by(100) {
        let chunk_end = (chunk_start + 100).min(n);
        let mut batch = WriteBatch::new();
        for i in chunk_start..chunk_end {
            batch = batch.put(Key::Int(i as i64), VALUE.as_slice(), None);
        }
        ns.write_batch(batch).unwrap();
    }
    start.elapsed()
}

fn bench_mem_keys(n: usize) -> std::time::Duration {
    let config = Config::in_memory();
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let count = ns.keys(&Key::Int(0)).unwrap().count();
    assert_eq!(count, n);
    start.elapsed()
}

const FILTER_SIZES: &[usize] = &[1_000, 16_000, 100_000];

fn format_size(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else {
        format!("{}K", n / 1_000)
    }
}

/// Run profiling benchmark: put N keys, then random get N keys, then print
/// sub-operation breakdown from profiling histograms.
#[cfg(feature = "profiling")]
fn bench_profiling(n: usize) -> (std::time::Duration, Vec<(&'static str, u64, u64)>) {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    // Flush to SSTables so reads hit disk path, not just memtable
    db.flush().unwrap();
    db.wait_for_compaction();

    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    let elapsed = start.elapsed();

    let report = db.profiling_report();
    (elapsed, report)
}

#[cfg(feature = "profiling")]
fn format_nanos(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}

#[allow(unreachable_code)]
fn main() {
    // Profiling mode: run profiling benchmark and exit
    #[cfg(feature = "profiling")]
    {
        let prof_sizes: &[usize] = &[1_000, 16_000, 1_000_000];
        for &n in prof_sizes {
            eprintln!("Profiling get n={n}...");
            let (elapsed, report) = bench_profiling(n);
            let total_ns = elapsed.as_nanos() as u64;

            eprintln!(
                "\n## Profiling (get, {} keys, total: {})\n",
                format_size(n),
                format_nanos(total_ns)
            );
            eprintln!(
                "| {:<20} | {:>10} | {:>12} | {:>10} | {:>8} |",
                "Probe", "Count", "Total", "Avg", "% of get"
            );
            eprintln!(
                "|{:-<22}|{:-<12}|{:-<14}|{:-<12}|{:-<10}|",
                "", "", "", "", ""
            );
            for (name, count, sum_ns) in &report {
                if *count == 0 {
                    continue;
                }
                let avg_ns = sum_ns / count;
                let pct = if total_ns > 0 {
                    *sum_ns as f64 / total_ns as f64 * 100.0
                } else {
                    0.0
                };
                eprintln!(
                    "| {:<20} | {:>10} | {:>12} | {:>10} | {:>7.1}% |",
                    name,
                    count,
                    format_nanos(*sum_ns),
                    format_nanos(avg_ns),
                    pct
                );
            }
            eprintln!();
        }
        return;
    }

    eprintln!("Collecting machine info...");
    let info = collect_machine_info();

    type BenchFn = fn(usize, FilterPolicy) -> std::time::Duration;
    let operations: Vec<(&str, BenchFn)> = vec![
        ("put", bench_put),
        ("get", bench_get),
        ("delete", bench_delete),
        ("scan", bench_scan),
        ("batch", bench_batch),
        ("keys", bench_keys),
        ("flush", bench_flush),
        ("get_sst", bench_get_sst),
        ("get_cpt", bench_get_compact),
        ("put_obj", bench_put_objects),
        ("get_obj", bench_get_objects),
    ];

    // header row
    let size_headers: Vec<String> = SIZES.iter().map(|&s| format_size(s)).collect();

    type MemBenchFn = fn(usize) -> std::time::Duration;
    let mem_operations: Vec<(&str, MemBenchFn)> = vec![
        ("put", bench_mem_put),
        ("get", bench_mem_get),
        ("delete", bench_mem_delete),
        ("scan", bench_mem_scan),
        ("batch", bench_mem_batch),
        ("keys", bench_mem_keys),
    ];

    let mem_size_headers: Vec<String> = MEM_SIZES.iter().map(|&s| format_size(s)).collect();
    let default_policy = FilterPolicy::default();

    // run disk benchmarks
    eprintln!("Running disk benchmarks...");
    let mut results: Vec<Vec<String>> = Vec::new();
    for (name, func) in &operations {
        let mut row = Vec::new();
        for &size in SIZES {
            eprintln!("  {name:<8} n={size}...");
            let elapsed = func(size, default_policy);
            row.push(format_duration(elapsed));
        }
        results.push(row);
    }

    // run in-memory benchmarks
    eprintln!("Running in-memory benchmarks...");
    let mut mem_results: Vec<Vec<String>> = Vec::new();
    for (name, func) in &mem_operations {
        let mut row = Vec::new();
        for &size in MEM_SIZES {
            eprintln!("  mem_{name:<8} n={size}...");
            let elapsed = func(size);
            row.push(format_duration(elapsed));
        }
        mem_results.push(row);
    }

    // Build markdown
    let mut md = String::new();
    md.push_str("# Benchmark\n\n");
    md.push_str("> Performance of core rKV operations (MemTable and SSTable paths).\n\n");

    // Machine table
    md.push_str("## Environment\n\n");
    md.push_str("| Field  | Value |\n");
    md.push_str("|--------|-------|\n");
    for (field, value) in &info {
        md.push_str(&format!("| {field:<6} | {value} |\n"));
    }

    // Methodology
    md.push_str("\n## Methodology\n\n");
    md.push_str("Each operation runs against a fresh temporary DB in release mode.\n");
    md.push_str("Wall-clock time is measured via `std::time::Instant`.\n\n");
    md.push_str("| Operation | Description |\n");
    md.push_str("|-----------|-------------|\n");
    md.push_str("| put       | Sequential inserts of N keys with 64-byte values |\n");
    md.push_str("| get       | Random reads of N existing keys (shuffled order) |\n");
    md.push_str("| delete    | Sequential deletes of N existing keys |\n");
    md.push_str("| scan      | Forward scan of all keys (limit=N, offset=0) |\n");
    md.push_str("| batch     | WriteBatch inserts in chunks of 100 |\n");
    md.push_str("| keys      | Lazy KeyIterator full drain of N keys |\n");
    md.push_str("| flush     | Flush N keys from MemTable to L0 SSTable |\n");
    md.push_str("| get_sst   | Random reads of N keys from SSTable (after flush) |\n");
    md.push_str("| get_cpt   | Random reads of N keys after flush + compaction (multi-level) |\n");
    md.push_str("| put_obj   | Sequential inserts of N keys with 4 KB values via ObjectStore |\n");
    md.push_str("| get_obj   | Random reads of N keys resolved from ObjectStore |\n");
    md.push_str(
        "\n**In-memory** variants run the same operations with `Config::in_memory()` (no disk).\n",
    );

    md.push_str("\n## Results (Disk)\n\n");

    // Results table header
    md.push_str("| Operation |");
    for h in &size_headers {
        md.push_str(&format!(" {h:<10} |"));
    }
    md.push('\n');

    md.push_str("|-----------|");
    for _ in &size_headers {
        md.push_str("------------|");
    }
    md.push('\n');

    // Results rows
    for (i, (name, _)) in operations.iter().enumerate() {
        md.push_str(&format!("| {name:<9} |"));
        for cell in &results[i] {
            md.push_str(&format!(" {cell:<10} |"));
        }
        md.push('\n');
    }

    md.push_str("\n## Results (In-Memory)\n\n");
    md.push_str("> Pure in-memory mode — no disk I/O, no AOL, no SSTables.\n\n");

    md.push_str("| Operation |");
    for h in &mem_size_headers {
        md.push_str(&format!(" {h:<10} |"));
    }
    md.push('\n');

    md.push_str("|-----------|");
    for _ in &mem_size_headers {
        md.push_str("------------|");
    }
    md.push('\n');

    for (i, (name, _)) in mem_operations.iter().enumerate() {
        md.push_str(&format!("| {name:<9} |"));
        for cell in &mem_results[i] {
            md.push_str(&format!(" {cell:<10} |"));
        }
        md.push('\n');
    }

    // Filter comparison (Bloom vs Ribbon) — reuses the same bench functions
    let filter_size_headers: Vec<String> = FILTER_SIZES.iter().map(|&s| format_size(s)).collect();

    eprintln!("Running filter comparison benchmarks...");
    let mut bloom_results: Vec<Vec<String>> = Vec::new();
    let mut ribbon_results: Vec<Vec<String>> = Vec::new();
    for (name, func) in &operations {
        let mut bloom_row = Vec::new();
        let mut ribbon_row = Vec::new();
        for &size in FILTER_SIZES {
            eprintln!("  {name:<8} bloom  n={size}...");
            bloom_row.push(format_duration(func(size, FilterPolicy::Bloom)));
            eprintln!("  {name:<8} ribbon n={size}...");
            ribbon_row.push(format_duration(func(size, FilterPolicy::Ribbon)));
        }
        bloom_results.push(bloom_row);
        ribbon_results.push(ribbon_row);
    }

    md.push_str("\n## Filter Comparison (Bloom vs Ribbon)\n\n");
    md.push_str("> Same operations with different filter policies.\n");
    md.push_str("> Bloom: ~10 bits/key, Ribbon: ~7 bits/key (both target ~1% FPR).\n\n");

    // Header: Operation | 1K (B) | 1K (R) | 8K (B) | 8K (R) | ...
    md.push_str("| Operation |");
    for h in &filter_size_headers {
        md.push_str(&format!(" {h} (B) | {h} (R) |"));
    }
    md.push('\n');

    md.push_str("|-----------|");
    for _ in &filter_size_headers {
        md.push_str("---------|---------|");
    }
    md.push('\n');

    for (i, (name, _)) in operations.iter().enumerate() {
        md.push_str(&format!("| {name:<9} |"));
        for j in 0..filter_size_headers.len() {
            md.push_str(&format!(
                " {} | {} |",
                bloom_results[i][j], ribbon_results[i][j]
            ));
        }
        md.push('\n');
    }

    // Reproduce
    md.push_str("\n## Reproduce\n\n");
    md.push_str("```sh\nmake bench\n```\n");

    // Write to BENCH.md at project root
    let bench_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("BENCH.md");

    let mut file = std::fs::File::create(&bench_path).unwrap();
    file.write_all(md.as_bytes()).unwrap();

    eprintln!("Wrote {}", bench_path.display());
}
