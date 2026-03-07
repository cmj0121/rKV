//! Crash-recovery and property-based tests for rKV.
//!
//! Proves durability invariants by simulating crashes (dropping the DB without
//! calling `close()`), then reopening and verifying data integrity.
//!
//! # Test categories
//!
//! 1. **AOL replay**: Write data without flush, crash, reopen → all committed writes recovered
//! 2. **Flush + crash**: Write, flush, write more, crash, reopen → SSTable + AOL data intact
//! 3. **Scan ordering invariants**: Random ops with flush/compact → scan sorted, rscan reversed
//! 4. **Concurrent write safety**: Multi-threaded writes → reopen → no data loss
//!
//! # Environment variables
//!
//! - `RKV_CRASH_FUZZ_SECS`: runtime for property-based tests (default: 3)
//! - `RKV_CRASH_FUZZ_SEED`: RNG seed for reproducibility (default: random)

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use rkv::{Config, Key, WriteBatch, DB, DEFAULT_NAMESPACE};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open_db(path: &std::path::Path) -> DB {
    let mut config = Config::new(path);
    config.create_if_missing = true;
    // Small buffer to trigger more flushes in property tests.
    config.write_buffer_size = 32 * 1024;
    config.aol_buffer_size = 0; // flush AOL every record for durability
    DB::open(config).unwrap()
}

fn reopen_db(path: &std::path::Path) -> DB {
    let config = Config::new(path);
    DB::open(config).unwrap()
}

fn fuzz_seed() -> u64 {
    std::env::var("RKV_CRASH_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| fastrand::u64(..))
}

fn fuzz_secs() -> u64 {
    std::env::var("RKV_CRASH_FUZZ_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

// =========================================================================
// 1. AOL replay after crash
// =========================================================================

#[test]
fn aol_replay_recovers_all_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("aol_crash");

    // Write 200 keys without flush, then crash (drop without close)
    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 0..200 {
            ns.put(format!("key{i:04}"), format!("val{i}"), None)
                .unwrap();
        }
        // Crash: drop DB without close() — no flush, no graceful shutdown
        drop(db);
    }

    // Reopen — AOL replay should recover everything
    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 0..200 {
            let val = ns.get(format!("key{i:04}")).unwrap();
            assert_eq!(
                val.as_bytes().unwrap(),
                format!("val{i}").as_bytes(),
                "key{i:04} not recovered after AOL replay"
            );
        }
        assert_eq!(ns.count().unwrap(), 200);
        db.close().unwrap();
    }
}

#[test]
fn aol_replay_recovers_deletes() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("aol_del_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        // Write 50 keys, delete 25
        for i in 0..50 {
            ns.put(format!("k{i:02}"), "alive", None).unwrap();
        }
        for i in 0..25 {
            ns.delete(format!("k{i:02}")).unwrap();
        }
        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        // Keys 0–24 should be deleted (KeyNotFound)
        for i in 0..25 {
            assert!(
                ns.get(format!("k{i:02}")).is_err(),
                "k{i:02} should be deleted"
            );
        }
        // Keys 25–49 should be alive
        for i in 25..50 {
            let val = ns.get(format!("k{i:02}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"alive");
        }
        assert_eq!(ns.count().unwrap(), 25);
        db.close().unwrap();
    }
}

#[test]
fn aol_replay_multi_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("aol_multi_ns");

    {
        let db = open_db(&db_path);
        for ns_name in &["alpha", "beta", "gamma"] {
            let ns = db.namespace(ns_name, None).unwrap();
            for i in 0..30 {
                ns.put(format!("{ns_name}_{i}"), format!("v{i}"), None)
                    .unwrap();
            }
        }
        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        for ns_name in &["alpha", "beta", "gamma"] {
            let ns = db.namespace(ns_name, None).unwrap();
            assert_eq!(
                ns.count().unwrap(),
                30,
                "namespace {ns_name} count mismatch"
            );
            for i in 0..30 {
                let val = ns.get(format!("{ns_name}_{i}")).unwrap();
                assert_eq!(val.as_bytes().unwrap(), format!("v{i}").as_bytes());
            }
        }
        db.close().unwrap();
    }
}

// =========================================================================
// 2. Flush + crash
// =========================================================================

#[test]
fn flush_then_crash_preserves_sstable_data() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("flush_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Phase 1: write and flush (data persisted to SSTable)
        for i in 0..100 {
            ns.put(format!("flushed{i:03}"), format!("fv{i}"), None)
                .unwrap();
        }
        db.flush().unwrap();

        // Phase 2: write more (data only in memtable + AOL, not flushed)
        for i in 0..50 {
            ns.put(format!("unflushed{i:03}"), format!("uv{i}"), None)
                .unwrap();
        }

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Flushed data should be in SSTables
        for i in 0..100 {
            let val = ns.get(format!("flushed{i:03}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), format!("fv{i}").as_bytes());
        }

        // Unflushed data should be recovered from AOL
        for i in 0..50 {
            let val = ns.get(format!("unflushed{i:03}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), format!("uv{i}").as_bytes());
        }

        assert_eq!(ns.count().unwrap(), 150);
        db.close().unwrap();
    }
}

#[test]
fn overwrite_after_flush_then_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("overwrite_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Write initial values and flush to SSTable
        for i in 0..50 {
            ns.put(format!("k{i:02}"), "old", None).unwrap();
        }
        db.flush().unwrap();

        // Overwrite half with new values (in memtable/AOL only)
        for i in 0..25 {
            ns.put(format!("k{i:02}"), "new", None).unwrap();
        }

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Keys 0–24 should have "new" (AOL replay overrides SSTable)
        for i in 0..25 {
            let val = ns.get(format!("k{i:02}")).unwrap();
            assert_eq!(
                val.as_bytes().unwrap(),
                b"new",
                "k{i:02} should be overwritten to 'new'"
            );
        }

        // Keys 25–49 should still have "old" (from SSTable, no AOL override)
        for i in 25..50 {
            let val = ns.get(format!("k{i:02}")).unwrap();
            assert_eq!(
                val.as_bytes().unwrap(),
                b"old",
                "k{i:02} should still be 'old'"
            );
        }

        db.close().unwrap();
    }
}

#[test]
fn delete_after_flush_then_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("del_flush_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        for i in 0..40 {
            ns.put(format!("d{i:02}"), "alive", None).unwrap();
        }
        db.flush().unwrap();

        // Delete 20 keys after flush (tombstones in AOL only)
        for i in 0..20 {
            ns.delete(format!("d{i:02}")).unwrap();
        }

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        for i in 0..20 {
            assert!(
                ns.get(format!("d{i:02}")).is_err(),
                "d{i:02} should be deleted after recovery"
            );
        }
        for i in 20..40 {
            let val = ns.get(format!("d{i:02}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"alive");
        }
        assert_eq!(ns.count().unwrap(), 20);
        db.close().unwrap();
    }
}

#[test]
fn compact_then_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("compact_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Write enough to create multiple SSTable flushes
        for round in 0..3 {
            for i in 0..100 {
                ns.put(format!("r{round}_k{i:03}"), format!("r{round}_v{i}"), None)
                    .unwrap();
            }
            db.flush().unwrap();
        }

        db.compact().unwrap();
        db.wait_for_compaction();

        // Write more after compaction (in memtable/AOL)
        for i in 0..30 {
            ns.put(format!("post_compact_{i}"), "pc", None).unwrap();
        }

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // All compacted data should be intact
        for round in 0..3 {
            for i in 0..100 {
                let val = ns.get(format!("r{round}_k{i:03}")).unwrap();
                assert_eq!(val.as_bytes().unwrap(), format!("r{round}_v{i}").as_bytes());
            }
        }

        // Post-compaction data recovered from AOL
        for i in 0..30 {
            let val = ns.get(format!("post_compact_{i}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"pc");
        }

        assert_eq!(ns.count().unwrap(), 330);
        db.close().unwrap();
    }
}

// =========================================================================
// 3. Scan ordering property tests
// =========================================================================

#[test]
fn scan_ordering_invariant_with_flush() {
    let seed = fuzz_seed();
    let duration = fuzz_secs();
    eprintln!("scan_ordering_invariant: seed={seed} duration={duration}s");

    let mut rng = fastrand::Rng::with_seed(seed);
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("scan_order");

    let db = open_db(&db_path);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(duration);
    let mut op_count = 0u64;
    // Oracle tracks live keys (true = live, entry removed on delete)
    let mut oracle: BTreeMap<String, ()> = BTreeMap::new();

    while std::time::Instant::now() < deadline {
        let op = rng.u32(0..100);
        op_count += 1;

        match op {
            // put (45%)
            0..45 => {
                let key = format!("sk{:04}", rng.u32(0..200));
                let val_len = rng.usize(1..100);
                let mut val = vec![0u8; val_len];
                rng.fill(&mut val);
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
                ns.put(key.as_str(), val.as_slice(), None).unwrap();
                oracle.insert(key, ());
            }
            // delete (15%)
            45..60 => {
                let key = format!("sk{:04}", rng.u32(0..200));
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
                let _ = ns.delete(key.as_str());
                oracle.remove(&key);
            }
            // flush (10%)
            60..70 => {
                db.flush().unwrap();
            }
            // compact (5%)
            70..75 => {
                db.compact().unwrap();
                db.wait_for_compaction();
            }
            // verify scan correctness against oracle (25%)
            _ => {
                let prefix = format!("sk{:01}", rng.u32(0..10));
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

                // Oracle: expected live keys with this prefix, sorted
                let oracle_keys: Vec<&String> =
                    oracle.keys().filter(|k| k.starts_with(&prefix)).collect();

                // Forward scan
                let db_keys: Vec<String> = ns
                    .scan(&Key::from(prefix.as_str()), 1000, 0, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();

                assert_eq!(
                    db_keys.len(),
                    oracle_keys.len(),
                    "op#{op_count} scan(prefix={prefix}): count mismatch db={} oracle={}",
                    db_keys.len(),
                    oracle_keys.len()
                );

                for (db_k, oracle_k) in db_keys.iter().zip(oracle_keys.iter()) {
                    assert_eq!(
                        db_k, *oracle_k,
                        "op#{op_count} scan(prefix={prefix}): key mismatch"
                    );
                }

                // Reverse scan
                let rdb_keys: Vec<String> = ns
                    .rscan(&Key::from(prefix.as_str()), 1000, 0, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();

                let oracle_rkeys: Vec<&String> = oracle
                    .keys()
                    .rev()
                    .filter(|k| k.starts_with(&prefix))
                    .collect();

                assert_eq!(
                    rdb_keys.len(),
                    oracle_rkeys.len(),
                    "op#{op_count} rscan(prefix={prefix}): count mismatch db={} oracle={}",
                    rdb_keys.len(),
                    oracle_rkeys.len()
                );

                for (db_k, oracle_k) in rdb_keys.iter().zip(oracle_rkeys.iter()) {
                    assert_eq!(
                        db_k, *oracle_k,
                        "op#{op_count} rscan(prefix={prefix}): key mismatch"
                    );
                }
            }
        }
    }

    eprintln!("scan_ordering_invariant: completed {op_count} ops (seed={seed})");
    db.close().unwrap();
}

#[test]
fn scan_ordering_after_crash_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("scan_crash_order");
    let mut oracle: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    // Phase 1: write and flush
    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 0..100 {
            let key = format!("order{i:04}");
            let val = format!("v{i}");
            ns.put(key.as_str(), val.as_str(), None).unwrap();
            oracle.insert(key, val.into_bytes());
        }
        db.flush().unwrap();

        // Phase 2: write more, then crash
        for i in 100..150 {
            let key = format!("order{i:04}");
            let val = format!("v{i}");
            ns.put(key.as_str(), val.as_str(), None).unwrap();
            oracle.insert(key, val.into_bytes());
        }
        drop(db); // crash
    }

    // Reopen and verify scan ordering across SSTable + AOL-recovered data
    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        let db_keys: Vec<String> = ns
            .scan(&Key::from(""), 1000, 0, false)
            .unwrap()
            .into_iter()
            .map(|k| k.to_string())
            .collect();

        // Must be sorted
        for window in db_keys.windows(2) {
            assert!(
                window[0] <= window[1],
                "scan after crash not sorted: {:?} > {:?}",
                window[0],
                window[1]
            );
        }

        // Must match oracle count
        assert_eq!(db_keys.len(), oracle.len());

        // rscan must be reverse sorted
        let rdb_keys: Vec<String> = ns
            .rscan(&Key::from(""), 1000, 0, false)
            .unwrap()
            .into_iter()
            .map(|k| k.to_string())
            .collect();

        for window in rdb_keys.windows(2) {
            assert!(
                window[0] >= window[1],
                "rscan after crash not reverse-sorted: {:?} < {:?}",
                window[0],
                window[1]
            );
        }

        db.close().unwrap();
    }
}

// =========================================================================
// 4. Concurrent write safety
// =========================================================================

#[test]
fn concurrent_writes_same_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("concurrent_same_ns");

    let expected_count: usize;

    {
        let db = Arc::new(open_db(&db_path));
        let num_threads = 4;
        let keys_per_thread = 100;
        expected_count = num_threads * keys_per_thread;

        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();

        for t in 0..num_threads {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
                for i in 0..keys_per_thread {
                    // Each thread uses a distinct key prefix to avoid overwrites
                    ns.put(format!("t{t}_k{i:04}"), format!("t{t}_v{i}"), None)
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Drop without close — simulate crash
        // Arc prevents direct drop, so extract and drop
        let db = Arc::into_inner(db).unwrap();
        drop(db);
    }

    // Reopen and verify all data is present
    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(
            ns.count().unwrap(),
            expected_count as u64,
            "concurrent writes: expected {expected_count} keys"
        );

        for t in 0..4usize {
            for i in 0..100usize {
                let val = ns.get(format!("t{t}_k{i:04}")).unwrap();
                assert_eq!(
                    val.as_bytes().unwrap(),
                    format!("t{t}_v{i}").as_bytes(),
                    "thread {t} key {i} mismatch after recovery"
                );
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn concurrent_writes_multi_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("concurrent_multi_ns");
    let namespaces = ["ns_a", "ns_b", "ns_c", "ns_d"];

    {
        let db = Arc::new(open_db(&db_path));
        let barrier = Arc::new(Barrier::new(namespaces.len()));
        let mut handles = Vec::new();

        for (t, ns_name) in namespaces.iter().enumerate() {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let ns_name = ns_name.to_string();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let ns = db.namespace(&ns_name, None).unwrap();
                for i in 0..80 {
                    ns.put(format!("mk{i:03}"), format!("t{t}_v{i}"), None)
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let db = Arc::into_inner(db).unwrap();
        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        for (t, ns_name) in namespaces.iter().enumerate() {
            let ns = db.namespace(ns_name, None).unwrap();
            assert_eq!(ns.count().unwrap(), 80, "{ns_name} count mismatch");
            for i in 0..80 {
                let val = ns.get(format!("mk{i:03}")).unwrap();
                assert_eq!(
                    val.as_bytes().unwrap(),
                    format!("t{t}_v{i}").as_bytes(),
                    "{ns_name} key mk{i:03} mismatch"
                );
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn concurrent_writes_with_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("concurrent_flush");

    {
        let db = Arc::new(open_db(&db_path));
        let barrier = Arc::new(Barrier::new(5)); // 4 writers + 1 flusher
        let mut handles = Vec::new();

        // 4 writer threads
        for t in 0..4 {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
                for i in 0..100 {
                    ns.put(format!("cf_t{t}_k{i:03}"), format!("v{i}"), None)
                        .unwrap();
                }
            }));
        }

        // 1 flusher thread
        {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..5 {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    let _ = db.flush();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let db = Arc::into_inner(db).unwrap();
        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(
            ns.count().unwrap(),
            400,
            "concurrent_flush: expected 400 keys"
        );
        for t in 0..4 {
            for i in 0..100 {
                let val = ns.get(format!("cf_t{t}_k{i:03}")).unwrap();
                assert_eq!(val.as_bytes().unwrap(), format!("v{i}").as_bytes());
            }
        }
        db.close().unwrap();
    }
}

// =========================================================================
// 5. WriteBatch crash recovery
// =========================================================================

#[test]
fn write_batch_crash_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("batch_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Batch 1: 50 puts
        let mut batch = WriteBatch::new();
        for i in 0..50 {
            batch = batch.put(
                Key::from(format!("batch{i:03}").as_str()),
                format!("bv{i}"),
                None,
            );
        }
        ns.write_batch(batch).unwrap();

        // Batch 2: mixed puts and deletes
        let mut batch2 = WriteBatch::new();
        for i in 50..80 {
            batch2 = batch2.put(
                Key::from(format!("batch{i:03}").as_str()),
                format!("bv{i}"),
                None,
            );
        }
        // Delete first 10 from batch 1
        for i in 0..10 {
            batch2 = batch2.delete(Key::from(format!("batch{i:03}").as_str()));
        }
        ns.write_batch(batch2).unwrap();

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Keys 0–9 should be deleted
        for i in 0..10 {
            assert!(
                ns.get(format!("batch{i:03}")).is_err(),
                "batch{i:03} should be deleted"
            );
        }

        // Keys 10–79 should be alive
        for i in 10..80 {
            let val = ns.get(format!("batch{i:03}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), format!("bv{i}").as_bytes());
        }

        assert_eq!(ns.count().unwrap(), 70);
        db.close().unwrap();
    }
}

#[test]
fn write_batch_after_flush_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("batch_flush_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Regular puts, then flush
        for i in 0..20 {
            ns.put(format!("pre{i:02}"), "old", None).unwrap();
        }
        db.flush().unwrap();

        // Batch overwrites some flushed keys + adds new ones
        let mut batch = WriteBatch::new();
        for i in 0..10 {
            batch = batch.put(Key::from(format!("pre{i:02}").as_str()), "batch_new", None);
        }
        for i in 0..15 {
            batch = batch.put(
                Key::from(format!("post{i:02}").as_str()),
                "batch_post",
                None,
            );
        }
        ns.write_batch(batch).unwrap();

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // pre00–pre09 should have "batch_new"
        for i in 0..10 {
            let val = ns.get(format!("pre{i:02}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"batch_new");
        }
        // pre10–pre19 should still have "old"
        for i in 10..20 {
            let val = ns.get(format!("pre{i:02}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"old");
        }
        // post00–post14 should have "batch_post"
        for i in 0..15 {
            let val = ns.get(format!("post{i:02}")).unwrap();
            assert_eq!(val.as_bytes().unwrap(), b"batch_post");
        }

        assert_eq!(ns.count().unwrap(), 35);
        db.close().unwrap();
    }
}

// =========================================================================
// 6. TTL across restart
// =========================================================================

#[test]
fn ttl_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ttl_restart");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Key with long TTL (should survive restart)
        ns.put("long_ttl", "alive", Some(Duration::from_secs(3600)))
            .unwrap();

        // Key with no TTL
        ns.put("no_ttl", "permanent", None).unwrap();

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Long TTL key should still be alive
        let val = ns.get("long_ttl").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"alive");

        // No-TTL key should be permanent
        let val = ns.get("no_ttl").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"permanent");

        // TTL should still be set (>0 seconds remaining)
        let ttl = ns.ttl("long_ttl").unwrap();
        assert!(
            ttl.is_some(),
            "long_ttl should still have TTL after restart"
        );
        let remaining = ttl.unwrap();
        assert!(
            remaining.as_secs() > 3500,
            "TTL should be ~3600s, got {}s",
            remaining.as_secs()
        );

        db.close().unwrap();
    }
}

#[test]
fn expired_ttl_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ttl_expired");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Key with very short TTL
        ns.put("short", "will_expire", Some(Duration::from_millis(50)))
            .unwrap();

        // Key with no TTL for comparison
        ns.put("keeper", "stays", None).unwrap();

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(100));

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Expired key should not be found (AOL replay filters expired entries)
        assert!(
            ns.get("short").is_err(),
            "expired key should not be found after restart"
        );

        // Non-TTL key should still be there
        let val = ns.get("keeper").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"stays");

        db.close().unwrap();
    }
}

#[test]
fn ttl_after_flush_and_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ttl_flush_crash");

    {
        let db = open_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Flush TTL key to SSTable
        ns.put("flushed_ttl", "value", Some(Duration::from_secs(3600)))
            .unwrap();
        ns.put("flushed_no_ttl", "permanent", None).unwrap();
        db.flush().unwrap();

        // Add more in AOL only
        ns.put("aol_ttl", "aol_val", Some(Duration::from_secs(3600)))
            .unwrap();

        drop(db); // crash
    }

    {
        let db = reopen_db(&db_path);
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Both flushed and AOL keys should be present
        let val = ns.get("flushed_ttl").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"value");

        let val = ns.get("flushed_no_ttl").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"permanent");

        let val = ns.get("aol_ttl").unwrap();
        assert_eq!(val.as_bytes().unwrap(), b"aol_val");

        // TTL preserved for AOL-recovered key (in memtable)
        let ttl2 = ns.ttl("aol_ttl").unwrap();
        assert!(ttl2.is_some());
        // Note: ttl() only checks memtable, so flushed keys' TTL
        // is not queryable via this API (SSTable stores it internally
        // and uses it for expiry filtering, but doesn't expose it).

        db.close().unwrap();
    }
}
