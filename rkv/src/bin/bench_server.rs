//! HTTP server benchmark — measures handler throughput via `tower::ServiceExt::oneshot`.
//!
//! No TCP involved: requests go directly through the Axum router, isolating
//! HTTP layer overhead from network latency.
//!
//! Run: `cargo run --features server --bin bench_server --release`

use std::io::Write;
use std::time::Instant;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use rkv::{Config, DB};
use sysinfo::System;
use tower::ServiceExt;

const SIZES: &[usize] = &[100, 500, 1_000, 5_000];
const VALUE_JSON: &str = "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"";

// ---------------------------------------------------------------------------
// Machine info helpers (same pattern as bench.rs)
// ---------------------------------------------------------------------------

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

fn format_size(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        n.to_string()
    }
}

// ---------------------------------------------------------------------------
// Router helper
// ---------------------------------------------------------------------------

fn fresh_router() -> (tempfile::TempDir, axum::Router) {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::new(dir.path());
    let db = DB::open(config).unwrap();
    let router = rkv::server::build_router(db);
    (dir, router)
}

// ---------------------------------------------------------------------------
// Bench functions
// ---------------------------------------------------------------------------

fn bench_put(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    let start = Instant::now();
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    start.elapsed()
}

fn bench_get(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });

    // Shuffled reads
    let mut indices: Vec<usize> = (0..n).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    rt.block_on(async {
        for &i in &indices {
            app.clone()
                .oneshot(
                    Request::get(format!("/api/_/keys/k{i}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    start.elapsed()
}

fn bench_delete(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });

    let start = Instant::now();
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::delete(format!("/api/_/keys/k{i}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    start.elapsed()
}

fn bench_scan(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i:06}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });

    // Paginated scan (40 per page)
    let start = Instant::now();
    rt.block_on(async {
        let mut offset = 0;
        loop {
            let resp = app
                .clone()
                .oneshot(
                    Request::get(format!("/api/_/keys?offset={offset}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let has_more = resp
                .headers()
                .get("x-rkv-has-more")
                .map(|v| v.to_str().unwrap() == "true")
                .unwrap_or(false);
            // Consume the body
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            let keys: Vec<String> = serde_json::from_slice(&bytes).unwrap();
            offset += keys.len();
            if !has_more {
                break;
            }
        }
    });
    start.elapsed()
}

fn bench_count(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });

    let start = Instant::now();
    rt.block_on(async {
        let resp = app
            .oneshot(Request::get("/api/_/count").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let count: u64 = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(count, n as u64);
    });
    start.elapsed()
}

fn bench_head(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data
    rt.block_on(async {
        for i in 0..n {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i}"))
                        .header("content-type", "application/json")
                        .body(Body::from(VALUE_JSON))
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });

    // Shuffled HEAD checks
    let mut indices: Vec<usize> = (0..n).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    rt.block_on(async {
        for &i in &indices {
            app.clone()
                .oneshot(
                    Request::head(format!("/api/_/keys/k{i}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    start.elapsed()
}

fn bench_rev_count(n: usize) -> std::time::Duration {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, app) = fresh_router();

    // Seed data — 2 revisions per key
    rt.block_on(async {
        for i in 0..n {
            for _ in 0..2 {
                app.clone()
                    .oneshot(
                        Request::put(format!("/api/_/keys/k{i}"))
                            .header("content-type", "application/json")
                            .body(Body::from(VALUE_JSON))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }
        }
    });

    // Shuffled rev_count checks
    let mut indices: Vec<usize> = (0..n).collect();
    fastrand::shuffle(&mut indices);

    let start = Instant::now();
    rt.block_on(async {
        for &i in &indices {
            app.clone()
                .oneshot(
                    Request::get(format!("/api/_/keys/k{i}/revisions"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let no_save = std::env::args().any(|a| a == "--no-save");

    eprintln!("Collecting machine info...");
    let info = collect_machine_info();

    type BenchFn = fn(usize) -> std::time::Duration;
    let operations: Vec<(&str, BenchFn)> = vec![
        ("put", bench_put),
        ("get", bench_get),
        ("delete", bench_delete),
        ("scan", bench_scan),
        ("count", bench_count),
        ("head", bench_head),
        ("rev_count", bench_rev_count),
    ];

    let size_headers: Vec<String> = SIZES.iter().map(|&s| format_size(s)).collect();

    // Run benchmarks
    let mut results: Vec<Vec<String>> = Vec::new();
    for (name, func) in &operations {
        let mut row = Vec::new();
        for &size in SIZES {
            eprintln!("  {name:<10} n={size}...");
            let elapsed = func(size);
            row.push(format_duration(elapsed));
        }
        results.push(row);
    }

    // Build markdown
    let mut md = String::new();
    md.push_str("# Server Benchmark\n\n");
    md.push_str(
        "> HTTP handler throughput via `tower::ServiceExt::oneshot` (no TCP overhead).\n\n",
    );

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
    md.push_str("Requests use `tower::ServiceExt::oneshot` — no TCP, pure handler throughput.\n");
    md.push_str("Wall-clock time is measured via `std::time::Instant`.\n\n");
    md.push_str("| Operation | Description |\n");
    md.push_str("|-----------|-------------|\n");
    md.push_str("| put       | Sequential PUT of N keys with 64-byte JSON string values |\n");
    md.push_str("| get       | Random GET of N existing keys (shuffled order) |\n");
    md.push_str("| delete    | Sequential DELETE of N existing keys |\n");
    md.push_str("| scan      | Full scan of all keys (paginated, 40 per page) |\n");
    md.push_str("| count     | Single COUNT request over N keys |\n");
    md.push_str("| head      | Random HEAD existence checks of N keys (shuffled) |\n");
    md.push_str("| rev_count | Random revision count queries (2 revisions per key) |\n");

    md.push_str("\n## Results\n\n");

    // Results table header
    md.push_str("| Operation  |");
    for h in &size_headers {
        md.push_str(&format!(" {h:<10} |"));
    }
    md.push('\n');

    md.push_str("|------------|");
    for _ in &size_headers {
        md.push_str("------------|");
    }
    md.push('\n');

    // Results rows
    for (i, (name, _)) in operations.iter().enumerate() {
        md.push_str(&format!("| {name:<10} |"));
        for cell in &results[i] {
            md.push_str(&format!(" {cell:<10} |"));
        }
        md.push('\n');
    }

    // Reproduce
    md.push_str("\n## Reproduce\n\n");
    md.push_str("```sh\ncargo run --features server --bin bench_server --release\n```\n");

    if no_save {
        print!("{md}");
    } else {
        let bench_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("BENCH_SERVER.md");

        let mut file = std::fs::File::create(&bench_path).unwrap();
        file.write_all(md.as_bytes()).unwrap();

        eprintln!("Wrote {}", bench_path.display());
    }
}
