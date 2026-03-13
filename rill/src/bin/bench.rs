use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use rill::backend::Backend;
use rill::msgid::MsgIdGen;
use rkv::{Config, DB};
use sysinfo::System;

const PUSH_SIZES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];
const POP_SIZES: &[usize] = &[100, 500, 1_000, 5_000];
const MESSAGE: &str = "hello-world-message-payload-for-benchmarking-rill-queue";

fn embed_backend() -> Backend {
    let db = DB::open(Config::in_memory()).unwrap();
    Backend::Embed(Box::new(db), Arc::new(MsgIdGen::new()))
}

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

    let rust_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().replace("rustc ", ""))
        .unwrap_or_else(|| "unknown".into());

    let date = std::process::Command::new("date")
        .arg("+%Y-%m-%d")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    vec![
        ("OS", os),
        ("CPU", cpu),
        ("Cores", cores),
        ("Memory", format!("{memory_gb} GB")),
        ("Rust", rust_version),
        ("Date", date),
    ]
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

fn format_rate(count: usize, d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs == 0.0 {
        return "∞".to_string();
    }
    let rate = count as f64 / secs;
    if rate >= 1_000_000.0 {
        format!("{:.1}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}

fn format_size(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        format!("{n}")
    }
}

// ---------------------------------------------------------------------------
// Push benchmarks
// ---------------------------------------------------------------------------

fn bench_push(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    rt.block_on(backend.create_queue("bench")).unwrap();

    let start = Instant::now();
    for _ in 0..n {
        rt.block_on(backend.push_message("bench", MESSAGE, None))
            .unwrap();
    }
    start.elapsed()
}

fn bench_batch_push(n: usize, batch_size: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    rt.block_on(backend.create_queue("bench")).unwrap();

    let batch: Vec<(&str, Option<std::time::Duration>)> = vec![(MESSAGE, None); batch_size];

    let start = Instant::now();
    let mut remaining = n;
    while remaining > 0 {
        let chunk = remaining.min(batch_size);
        rt.block_on(backend.push_messages("bench", &batch[..chunk]))
            .unwrap();
        remaining -= chunk;
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Pop benchmarks
// ---------------------------------------------------------------------------

fn bench_pop(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    rt.block_on(backend.create_queue("bench")).unwrap();

    for _ in 0..n {
        rt.block_on(backend.push_message("bench", MESSAGE, None))
            .unwrap();
    }

    let start = Instant::now();
    for _ in 0..n {
        rt.block_on(backend.pop_message("bench")).unwrap();
    }
    start.elapsed()
}

fn bench_batch_pop(n: usize, batch_size: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    rt.block_on(backend.create_queue("bench")).unwrap();

    let batch: Vec<(&str, Option<std::time::Duration>)> = vec![(MESSAGE, None); batch_size];
    let mut remaining = n;
    while remaining > 0 {
        let chunk = remaining.min(batch_size);
        rt.block_on(backend.push_messages("bench", &batch[..chunk]))
            .unwrap();
        remaining -= chunk;
    }

    let start = Instant::now();
    let mut popped = 0;
    while popped < n {
        let chunk = (n - popped).min(batch_size);
        let msgs = rt.block_on(backend.pop_messages("bench", chunk)).unwrap();
        if msgs.is_empty() {
            break;
        }
        popped += msgs.len();
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Mixed push/pop benchmark
// ---------------------------------------------------------------------------

fn bench_mixed(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    rt.block_on(backend.create_queue("bench")).unwrap();

    let mut rng = fastrand::Rng::with_seed(42);

    let start = Instant::now();
    for _ in 0..n {
        if rng.u32(0..100) < 60 {
            rt.block_on(backend.push_message("bench", MESSAGE, None))
                .unwrap();
        } else {
            rt.block_on(backend.pop_message("bench")).unwrap();
        }
    }
    start.elapsed()
}

fn main() {
    eprintln!("Collecting machine info...");
    let info = collect_machine_info();

    let push_headers: Vec<String> = PUSH_SIZES.iter().map(|&s| format_size(s)).collect();
    let pop_headers: Vec<String> = POP_SIZES.iter().map(|&s| format_size(s)).collect();

    // --- Push rate ---
    eprintln!("\n=== Push Rate ===");

    let mut push_rows: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();

    // Single push
    let mut times = Vec::new();
    let mut rates = Vec::new();
    for &size in PUSH_SIZES {
        eprint!("  push          n={size}...");
        let elapsed = bench_push(size);
        eprintln!(
            " {} ({})",
            format_duration(elapsed),
            format_rate(size, elapsed)
        );
        times.push(format_duration(elapsed));
        rates.push(format_rate(size, elapsed));
    }
    push_rows.push(("push".into(), times, rates));

    // Batch push
    for &bs in &[10, 50, 100] {
        let mut times = Vec::new();
        let mut rates = Vec::new();
        for &size in PUSH_SIZES {
            eprint!("  push(×{bs:<3})    n={size}...");
            let elapsed = bench_batch_push(size, bs);
            eprintln!(
                " {} ({})",
                format_duration(elapsed),
                format_rate(size, elapsed)
            );
            times.push(format_duration(elapsed));
            rates.push(format_rate(size, elapsed));
        }
        push_rows.push((format!("push(×{bs})"), times, rates));
    }

    // --- Pop rate ---
    eprintln!("\n=== Pop Rate ===");
    eprintln!("  (pop is O(n²) due to tombstone scan — smaller sizes used)");

    let mut pop_rows: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();

    // Single pop
    let mut times = Vec::new();
    let mut rates = Vec::new();
    for &size in POP_SIZES {
        eprint!("  pop           n={size}...");
        let elapsed = bench_pop(size);
        eprintln!(
            " {} ({})",
            format_duration(elapsed),
            format_rate(size, elapsed)
        );
        times.push(format_duration(elapsed));
        rates.push(format_rate(size, elapsed));
    }
    pop_rows.push(("pop".into(), times, rates));

    // Batch pop
    for &bs in &[10, 50, 100] {
        let mut times = Vec::new();
        let mut rates = Vec::new();
        for &size in POP_SIZES {
            eprint!("  pop(×{bs:<3})     n={size}...");
            let elapsed = bench_batch_pop(size, bs);
            eprintln!(
                " {} ({})",
                format_duration(elapsed),
                format_rate(size, elapsed)
            );
            times.push(format_duration(elapsed));
            rates.push(format_rate(size, elapsed));
        }
        pop_rows.push((format!("pop(×{bs})"), times, rates));
    }

    // --- Mixed ---
    eprintln!("\n=== Mixed (60% push / 40% pop) ===");

    let mixed_sizes: &[usize] = &[1_000, 5_000, 10_000];
    let mixed_headers: Vec<String> = mixed_sizes.iter().map(|&s| format_size(s)).collect();
    let mut mixed_times = Vec::new();
    let mut mixed_rates = Vec::new();
    for &size in mixed_sizes {
        eprint!("  mixed         n={size}...");
        let elapsed = bench_mixed(size);
        eprintln!(
            " {} ({})",
            format_duration(elapsed),
            format_rate(size, elapsed)
        );
        mixed_times.push(format_duration(elapsed));
        mixed_rates.push(format_rate(size, elapsed));
    }

    // --- Build markdown ---
    let mut md = String::new();
    md.push_str("# Rill Benchmark\n\n");
    md.push_str(
        "> Push and pop throughput for the rill message queue (in-memory rKV backend).\n\n",
    );

    // Environment
    md.push_str("## Environment\n\n");
    md.push_str("| Field  | Value |\n");
    md.push_str("|--------|-------|\n");
    for (field, value) in &info {
        md.push_str(&format!("| {field:<6} | {value} |\n"));
    }

    // Methodology
    md.push_str("\n## Methodology\n\n");
    md.push_str("Each operation runs against a fresh in-memory rKV database in release mode.\n");
    md.push_str("Wall-clock time measured via `std::time::Instant`.\n\n");
    md.push_str("| Operation | Description |\n");
    md.push_str("|-----------|-------------|\n");
    md.push_str("| push      | Sequential single-message pushes |\n");
    md.push_str("| push(×N)  | Batch push with N messages per WriteBatch |\n");
    md.push_str("| pop       | Sequential single-message pops (pre-filled queue) |\n");
    md.push_str("| pop(×N)   | Batch pop of N messages per call |\n");
    md.push_str("| mixed     | 60% push / 40% pop random interleaved workload |\n");

    // Push rate
    md.push_str("\n## Push Rate\n\n");
    md.push_str("| Operation |");
    for h in &push_headers {
        md.push_str(&format!(" {h} |"));
    }
    md.push('\n');
    md.push_str("|-----------|");
    for _ in &push_headers {
        md.push_str("------|");
    }
    md.push('\n');
    for (name, _times, rates) in &push_rows {
        md.push_str(&format!("| {name:<9} |"));
        for r in rates {
            md.push_str(&format!(" {r} |"));
        }
        md.push('\n');
    }

    // Pop rate
    md.push_str("\n## Pop Rate\n\n");
    md.push_str(
        "> Pop uses smaller sizes because `pop_first` rebuilds the merge iterator each call,\n",
    );
    md.push_str("> scanning accumulated tombstones — O(n²) for draining a full queue.\n\n");
    md.push_str("| Operation |");
    for h in &pop_headers {
        md.push_str(&format!(" {h} |"));
    }
    md.push('\n');
    md.push_str("|-----------|");
    for _ in &pop_headers {
        md.push_str("------|");
    }
    md.push('\n');
    for (name, _times, rates) in &pop_rows {
        md.push_str(&format!("| {name:<9} |"));
        for r in rates {
            md.push_str(&format!(" {r} |"));
        }
        md.push('\n');
    }

    // Timing details
    md.push_str("\n### Timing Details\n\n");
    md.push_str("#### Push\n\n");
    md.push_str("| Operation |");
    for h in &push_headers {
        md.push_str(&format!(" {h} |"));
    }
    md.push('\n');
    md.push_str("|-----------|");
    for _ in &push_headers {
        md.push_str("------|");
    }
    md.push('\n');
    for (name, times, _rates) in &push_rows {
        md.push_str(&format!("| {name:<9} |"));
        for t in times {
            md.push_str(&format!(" {t} |"));
        }
        md.push('\n');
    }

    md.push_str("\n#### Pop\n\n");
    md.push_str("| Operation |");
    for h in &pop_headers {
        md.push_str(&format!(" {h} |"));
    }
    md.push('\n');
    md.push_str("|-----------|");
    for _ in &pop_headers {
        md.push_str("------|");
    }
    md.push('\n');
    for (name, times, _rates) in &pop_rows {
        md.push_str(&format!("| {name:<9} |"));
        for t in times {
            md.push_str(&format!(" {t} |"));
        }
        md.push('\n');
    }

    // Mixed
    md.push_str("\n## Mixed Workload\n\n");
    md.push_str("| Operation |");
    for h in &mixed_headers {
        md.push_str(&format!(" {h} |"));
    }
    md.push('\n');
    md.push_str("|-----------|");
    for _ in &mixed_headers {
        md.push_str("------|");
    }
    md.push('\n');
    md.push_str("| mixed     |");
    for i in 0..mixed_headers.len() {
        md.push_str(&format!(" {} ({}) |", mixed_times[i], mixed_rates[i]));
    }
    md.push('\n');

    // Reproduce
    md.push_str("\n## Reproduce\n\n");
    md.push_str("```sh\ncargo run --release --bin rill-bench\n```\n");

    // Write
    let bench_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("BENCH.md");
    let mut file = std::fs::File::create(&bench_path).unwrap();
    file.write_all(md.as_bytes()).unwrap();

    eprintln!("\nWrote {}", bench_path.display());
}
