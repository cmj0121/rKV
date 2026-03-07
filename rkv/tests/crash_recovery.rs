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

use rkv::{Config, Key, DB, DEFAULT_NAMESPACE};

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
            }
            // delete (15%)
            45..60 => {
                let key = format!("sk{:04}", rng.u32(0..200));
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
                let _ = ns.delete(key.as_str());
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
            // verify scan ordering invariants (25%)
            _ => {
                let prefix = format!("sk{:01}", rng.u32(0..10));
                let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

                // Forward scan
                let db_keys: Vec<String> = ns
                    .scan(&Key::from(prefix.as_str()), 1000, 0, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();

                // INVARIANT 1: scan always returns keys in sorted order
                for window in db_keys.windows(2) {
                    assert!(
                        window[0] <= window[1],
                        "op#{op_count} scan not sorted: {:?} > {:?}",
                        window[0],
                        window[1]
                    );
                }

                // INVARIANT 2: no duplicate keys in scan results
                for window in db_keys.windows(2) {
                    assert!(
                        window[0] != window[1],
                        "op#{op_count} scan has duplicate: {:?}",
                        window[0]
                    );
                }

                // Reverse scan
                let rdb_keys: Vec<String> = ns
                    .rscan(&Key::from(prefix.as_str()), 1000, 0, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();

                // INVARIANT 3: rscan always returns keys in reverse sorted order
                for window in rdb_keys.windows(2) {
                    assert!(
                        window[0] >= window[1],
                        "op#{op_count} rscan not reverse-sorted: {:?} < {:?}",
                        window[0],
                        window[1]
                    );
                }

                // INVARIANT 4: no duplicate keys in rscan results
                for window in rdb_keys.windows(2) {
                    assert!(
                        window[0] != window[1],
                        "op#{op_count} rscan has duplicate: {:?}",
                        window[0]
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
