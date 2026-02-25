use std::io::Write;
use std::time::Instant;

use rkv::{Config, Key, DB, DEFAULT_NAMESPACE};
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

fn bench_put(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }
    start.elapsed()
}

fn bench_get(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    // Build a shuffled index array for random reads
    let mut indices: Vec<i64> = (0..n as i64).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    for &i in &indices {
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_delete(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
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
fn bench_put_objects(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
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
fn bench_get_objects(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
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

fn bench_scan(n: usize) -> std::time::Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..n {
        ns.put(i as i64, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let _ = ns.scan(&Key::Int(0), n, 0).unwrap();
    start.elapsed()
}

fn format_size(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else {
        format!("{}K", n / 1_000)
    }
}

fn main() {
    eprintln!("Collecting machine info...");
    let info = collect_machine_info();

    type BenchFn = fn(usize) -> std::time::Duration;
    let operations: Vec<(&str, BenchFn)> = vec![
        ("put", bench_put),
        ("get", bench_get),
        ("delete", bench_delete),
        ("scan", bench_scan),
        ("put_obj", bench_put_objects),
        ("get_obj", bench_get_objects),
    ];

    // header row
    let size_headers: Vec<String> = SIZES.iter().map(|&s| format_size(s)).collect();

    // run benchmarks
    let mut results: Vec<Vec<String>> = Vec::new();
    for (name, func) in &operations {
        let mut row = Vec::new();
        for &size in SIZES {
            eprintln!("  {name:<8} n={size}...");
            let elapsed = func(size);
            row.push(format_duration(elapsed));
        }
        results.push(row);
    }

    // Build markdown
    let mut md = String::new();
    md.push_str("# Benchmark\n\n");
    md.push_str("> MemTable-backed (in-memory) performance of core rKV operations.\n\n");

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
    md.push_str("| put_obj   | Sequential inserts of N keys with 4 KB values via ObjectStore |\n");
    md.push_str("| get_obj   | Random reads of N keys resolved from ObjectStore |\n");

    md.push_str("\n## Results\n\n");

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
