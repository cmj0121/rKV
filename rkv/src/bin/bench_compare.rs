#![allow(unexpected_cfgs)]

use std::path::Path;
use std::time::{Duration, Instant};

use sysinfo::System;

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

struct Dataset {
    keys: Vec<[u8; 8]>,
    shuffled: Vec<usize>,
    n: usize,
}

fn load_dataset(path: &Path) -> Dataset {
    let compressed = std::fs::read(path).unwrap();
    let raw = zstd::decode_all(&compressed[..]).unwrap();

    assert!(raw.len() >= 21, "dataset too short");
    assert_eq!(&raw[0..4], b"rKVB", "bad magic");
    assert_eq!(raw[4], 1, "unsupported version");

    let n = u64::from_be_bytes(raw[5..13].try_into().unwrap()) as usize;
    let seed = u64::from_be_bytes(raw[13..21].try_into().unwrap());

    assert_eq!(raw.len(), 21 + n * 8, "truncated dataset");

    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let off = 21 + i * 8;
        let k: [u8; 8] = raw[off..off + 8].try_into().unwrap();
        keys.push(k);
    }

    // Regenerate shuffle from seed
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut shuffled: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rng.usize(0..=i);
        shuffled.swap(i, j);
    }

    Dataset { keys, shuffled, n }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const VALUE: &[u8; 64] = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// rKV benchmarks
// ---------------------------------------------------------------------------

fn bench_rkv_put(ds: &Dataset) -> Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(tmp.path());
    let db = rkv::DB::open(config).unwrap();
    let ns = db.namespace(rkv::DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for key in &ds.keys {
        let i = i64::from_be_bytes(*key);
        ns.put(i, VALUE.as_slice(), None).unwrap();
    }
    start.elapsed()
}

fn bench_rkv_get(ds: &Dataset) -> Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(tmp.path());
    let db = rkv::DB::open(config).unwrap();
    let ns = db.namespace(rkv::DEFAULT_NAMESPACE, None).unwrap();

    for key in &ds.keys {
        let i = i64::from_be_bytes(*key);
        ns.put(i, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    for &idx in &ds.shuffled {
        let i = i64::from_be_bytes(ds.keys[idx]);
        ns.get(i).unwrap();
    }
    start.elapsed()
}

fn bench_rkv_delete(ds: &Dataset) -> Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(tmp.path());
    let db = rkv::DB::open(config).unwrap();
    let ns = db.namespace(rkv::DEFAULT_NAMESPACE, None).unwrap();

    for key in &ds.keys {
        let i = i64::from_be_bytes(*key);
        ns.put(i, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    for key in &ds.keys {
        let i = i64::from_be_bytes(*key);
        ns.delete(i).unwrap();
    }
    start.elapsed()
}

fn bench_rkv_scan(ds: &Dataset) -> Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(tmp.path());
    let db = rkv::DB::open(config).unwrap();
    let ns = db.namespace(rkv::DEFAULT_NAMESPACE, None).unwrap();

    for key in &ds.keys {
        let i = i64::from_be_bytes(*key);
        ns.put(i, VALUE.as_slice(), None).unwrap();
    }

    let start = Instant::now();
    let result = ns.scan(&rkv::Key::Int(0), ds.n, 0, false).unwrap();
    assert_eq!(result.len(), ds.n);
    start.elapsed()
}

fn bench_rkv_batch(ds: &Dataset) -> Duration {
    let tmp = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(tmp.path());
    let db = rkv::DB::open(config).unwrap();
    let ns = db.namespace(rkv::DEFAULT_NAMESPACE, None).unwrap();

    let start = Instant::now();
    for chunk in ds.keys.chunks(BATCH_SIZE) {
        let mut batch = rkv::WriteBatch::new();
        for key in chunk {
            let i = i64::from_be_bytes(*key);
            batch = batch.put(rkv::Key::Int(i), VALUE.as_slice(), None);
        }
        ns.write_batch(batch).unwrap();
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// redb benchmarks
// ---------------------------------------------------------------------------

#[cfg(feature = "bench-compare")]
mod bench_redb {
    use super::*;

    use redb::{ReadableDatabase, ReadableTable};

    const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("bench");

    pub fn put(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = redb::Database::create(tmp.path().join("bench.redb")).unwrap();

        let start = Instant::now();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in &ds.keys {
                table.insert(key.as_slice(), VALUE.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        start.elapsed()
    }

    pub fn get(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = redb::Database::create(tmp.path().join("bench.redb")).unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in &ds.keys {
                table.insert(key.as_slice(), VALUE.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let start = Instant::now();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        for &idx in &ds.shuffled {
            table.get(ds.keys[idx].as_slice()).unwrap().unwrap();
        }
        start.elapsed()
    }

    pub fn delete(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = redb::Database::create(tmp.path().join("bench.redb")).unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in &ds.keys {
                table.insert(key.as_slice(), VALUE.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let start = Instant::now();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in &ds.keys {
                table.remove(key.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        start.elapsed()
    }

    pub fn scan(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = redb::Database::create(tmp.path().join("bench.redb")).unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in &ds.keys {
                table.insert(key.as_slice(), VALUE.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let start = Instant::now();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        let count = table.iter().unwrap().count();
        assert_eq!(count, ds.n);
        start.elapsed()
    }

    pub fn batch(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = redb::Database::create(tmp.path().join("bench.redb")).unwrap();

        let start = Instant::now();
        for chunk in ds.keys.chunks(BATCH_SIZE) {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(TABLE).unwrap();
                for key in chunk {
                    table.insert(key.as_slice(), VALUE.as_slice()).unwrap();
                }
            }
            txn.commit().unwrap();
        }
        start.elapsed()
    }
}

// ---------------------------------------------------------------------------
// sled benchmarks
// ---------------------------------------------------------------------------

#[cfg(feature = "bench-compare")]
mod bench_sled {
    use super::*;

    pub fn put(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = sled::open(tmp.path().join("bench.sled")).unwrap();

        let start = Instant::now();
        for key in &ds.keys {
            db.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn get(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = sled::open(tmp.path().join("bench.sled")).unwrap();

        for key in &ds.keys {
            db.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for &idx in &ds.shuffled {
            db.get(ds.keys[idx].as_slice()).unwrap().unwrap();
        }
        start.elapsed()
    }

    pub fn delete(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = sled::open(tmp.path().join("bench.sled")).unwrap();

        for key in &ds.keys {
            db.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for key in &ds.keys {
            db.remove(key.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn scan(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = sled::open(tmp.path().join("bench.sled")).unwrap();

        for key in &ds.keys {
            db.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        let count = db.iter().count();
        assert_eq!(count, ds.n);
        start.elapsed()
    }

    pub fn batch(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = sled::open(tmp.path().join("bench.sled")).unwrap();

        let start = Instant::now();
        for chunk in ds.keys.chunks(BATCH_SIZE) {
            let mut batch = sled::Batch::default();
            for key in chunk {
                batch.insert(key.as_slice(), VALUE.as_slice());
            }
            db.apply_batch(batch).unwrap();
        }
        start.elapsed()
    }
}

// ---------------------------------------------------------------------------
// fjall benchmarks
// ---------------------------------------------------------------------------

#[cfg(feature = "bench-compare")]
mod bench_fjall {
    use super::*;

    fn open_db_and_keyspace(path: &Path) -> (fjall::Database, fjall::Keyspace) {
        let db = fjall::Database::open(fjall::Config::new(path)).unwrap();
        let ks = db
            .keyspace("bench", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        (db, ks)
    }

    pub fn put(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let (_db, ks) = open_db_and_keyspace(&tmp.path().join("bench.fjall"));

        let start = Instant::now();
        for key in &ds.keys {
            ks.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn get(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let (_db, ks) = open_db_and_keyspace(&tmp.path().join("bench.fjall"));

        for key in &ds.keys {
            ks.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for &idx in &ds.shuffled {
            ks.get(ds.keys[idx].as_slice()).unwrap().unwrap();
        }
        start.elapsed()
    }

    pub fn delete(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let (_db, ks) = open_db_and_keyspace(&tmp.path().join("bench.fjall"));

        for key in &ds.keys {
            ks.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for key in &ds.keys {
            ks.remove(key.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn scan(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let (_db, ks) = open_db_and_keyspace(&tmp.path().join("bench.fjall"));

        for key in &ds.keys {
            ks.insert(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        let count = ks.iter().count();
        assert_eq!(count, ds.n);
        start.elapsed()
    }

    pub fn batch(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let (db, ks) = open_db_and_keyspace(&tmp.path().join("bench.fjall"));

        let start = Instant::now();
        for chunk in ds.keys.chunks(BATCH_SIZE) {
            let mut batch = db.batch();
            for key in chunk {
                batch.insert(&ks, key.as_slice(), VALUE.as_slice());
            }
            batch.commit().unwrap();
        }
        start.elapsed()
    }
}

// ---------------------------------------------------------------------------
// rocksdb benchmarks
// ---------------------------------------------------------------------------

#[cfg(feature = "bench-rocksdb")]
mod bench_rocksdb {
    use super::*;

    pub fn put(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(tmp.path().join("bench.rocks")).unwrap();

        let start = Instant::now();
        for key in &ds.keys {
            db.put(key.as_slice(), VALUE.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn get(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(tmp.path().join("bench.rocks")).unwrap();

        for key in &ds.keys {
            db.put(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for &idx in &ds.shuffled {
            db.get(ds.keys[idx].as_slice()).unwrap().unwrap();
        }
        start.elapsed()
    }

    pub fn delete(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(tmp.path().join("bench.rocks")).unwrap();

        for key in &ds.keys {
            db.put(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        for key in &ds.keys {
            db.delete(key.as_slice()).unwrap();
        }
        start.elapsed()
    }

    pub fn scan(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(tmp.path().join("bench.rocks")).unwrap();

        for key in &ds.keys {
            db.put(key.as_slice(), VALUE.as_slice()).unwrap();
        }

        let start = Instant::now();
        let count = db.iterator(rocksdb::IteratorMode::Start).count();
        assert_eq!(count, ds.n);
        start.elapsed()
    }

    pub fn batch(ds: &Dataset) -> Duration {
        let tmp = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(tmp.path().join("bench.rocks")).unwrap();

        let start = Instant::now();
        for chunk in ds.keys.chunks(BATCH_SIZE) {
            let mut batch = rocksdb::WriteBatch::default();
            for key in chunk {
                batch.put(key.as_slice(), VALUE.as_slice());
            }
            db.write(batch).unwrap();
        }
        start.elapsed()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us} \u{00b5}s")
    } else if us < 1_000_000 {
        format!("{:.2} ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2} s", us as f64 / 1_000_000.0)
    }
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

// ---------------------------------------------------------------------------
// BENCH.md output
// ---------------------------------------------------------------------------

fn update_bench_md(comparison_md: &str) {
    let bench_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("BENCH.md");

    let existing = std::fs::read_to_string(&bench_path).unwrap_or_default();

    // Strip old comparison section
    let base = match existing.find("\n## Comparison") {
        Some(pos) => &existing[..pos],
        None => existing.as_str(),
    };

    let mut output = base.trim_end().to_string();
    output.push_str("\n\n");
    output.push_str(comparison_md);
    output.push('\n');
    std::fs::write(&bench_path, output).unwrap();

    eprintln!("Updated {}", bench_path.display());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    eprintln!("Collecting machine info...");
    let info = collect_machine_info();

    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("bench/data");

    let sizes = [
        (1_000, "1k"),
        (8_000, "8k"),
        (16_000, "16k"),
        (1_000_000, "1m"),
    ];

    // Load datasets
    eprintln!("Loading datasets...");
    let datasets: Vec<(usize, String, Dataset)> = sizes
        .iter()
        .map(|(n, label)| {
            let path = data_dir.join(format!("{label}.zst"));
            let ds = load_dataset(&path);
            assert_eq!(ds.n, *n);
            (*n, label.to_uppercase(), ds)
        })
        .collect();

    // Store names and bench function types
    type BenchFn = fn(&Dataset) -> Duration;

    struct Store {
        name: &'static str,
        put: BenchFn,
        get: BenchFn,
        delete: BenchFn,
        scan: BenchFn,
        batch: BenchFn,
    }

    let mut stores: Vec<Store> = vec![Store {
        name: "rKV",
        put: bench_rkv_put,
        get: bench_rkv_get,
        delete: bench_rkv_delete,
        scan: bench_rkv_scan,
        batch: bench_rkv_batch,
    }];

    #[cfg(feature = "bench-compare")]
    {
        stores.push(Store {
            name: "redb",
            put: bench_redb::put,
            get: bench_redb::get,
            delete: bench_redb::delete,
            scan: bench_redb::scan,
            batch: bench_redb::batch,
        });
        stores.push(Store {
            name: "sled",
            put: bench_sled::put,
            get: bench_sled::get,
            delete: bench_sled::delete,
            scan: bench_sled::scan,
            batch: bench_sled::batch,
        });
        stores.push(Store {
            name: "fjall",
            put: bench_fjall::put,
            get: bench_fjall::get,
            delete: bench_fjall::delete,
            scan: bench_fjall::scan,
            batch: bench_fjall::batch,
        });
    }

    #[cfg(feature = "bench-rocksdb")]
    {
        stores.push(Store {
            name: "rocksdb",
            put: bench_rocksdb::put,
            get: bench_rocksdb::get,
            delete: bench_rocksdb::delete,
            scan: bench_rocksdb::scan,
            batch: bench_rocksdb::batch,
        });
    }

    type OpAccessor = fn(&Store) -> BenchFn;
    let operations: Vec<(&str, OpAccessor)> = vec![
        ("Sequential Put", |s: &Store| s.put),
        ("Random Get", |s: &Store| s.get),
        ("Sequential Delete", |s: &Store| s.delete),
        ("Forward Scan", |s: &Store| s.scan),
        ("Batch Write", |s: &Store| s.batch),
    ];

    // Run benchmarks: results[op_idx][size_idx][store_idx] = Duration
    let mut results: Vec<Vec<Vec<String>>> = Vec::new();
    for (op_name, get_fn) in &operations {
        let mut size_results: Vec<Vec<String>> = Vec::new();
        for (n, label, ds) in &datasets {
            let mut store_results: Vec<String> = Vec::new();
            for store in &stores {
                let bench_fn = get_fn(store);
                eprintln!(
                    "  {op_name} {label} ({store_name})...",
                    store_name = store.name
                );
                let elapsed = bench_fn(ds);
                eprintln!(
                    "    {} n={n}: {dur}",
                    store.name,
                    dur = format_duration(elapsed)
                );
                store_results.push(format_duration(elapsed));
            }
            size_results.push(store_results);
        }
        results.push(size_results);
    }

    // Build markdown
    let mut md = String::new();
    md.push_str("## Comparison\n\n");

    let store_names: Vec<&str> = stores.iter().map(|s| s.name).collect();
    let has_rocksdb = store_names.contains(&"rocksdb");
    if has_rocksdb {
        md.push_str(
            "> rKV vs redb vs sled vs fjall vs rocksdb \u{2014} same pre-defined dataset.\n\n",
        );
    } else {
        md.push_str("> rKV vs redb vs sled vs fjall \u{2014} same pre-defined dataset.\n\n");
    }

    // Environment table
    md.push_str("### Environment\n\n");
    md.push_str("| Field  | Value |\n");
    md.push_str("|--------|-------|\n");
    for (field, value) in &info {
        md.push_str(&format!("| {field:<6} | {value} |\n"));
    }
    md.push('\n');

    // Per-operation tables
    let size_labels: Vec<String> = datasets.iter().map(|(_, label, _)| label.clone()).collect();

    for (op_idx, (op_name, _)) in operations.iter().enumerate() {
        md.push_str(&format!("### {op_name}\n\n"));

        // Header
        md.push_str("| N    |");
        for name in &store_names {
            md.push_str(&format!(" {name:<12} |"));
        }
        md.push('\n');

        md.push_str("|------|");
        for _ in &store_names {
            md.push_str("--------------|");
        }
        md.push('\n');

        // Rows
        for (size_idx, label) in size_labels.iter().enumerate() {
            md.push_str(&format!("| {label:<4} |"));
            for cell in &results[op_idx][size_idx] {
                md.push_str(&format!(" {cell:<12} |"));
            }
            md.push('\n');
        }
        md.push('\n');
    }

    // Reproduce
    md.push_str("### Reproduce\n\n");
    md.push_str("```sh\nmake bench-compare\n```\n");

    update_bench_md(&md);
}
