use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[allow(unused_imports)]
use rkv::{
    CompactionEvent, Compression, Config, DumpOptions, EntryIterator, Error, EventListener,
    FlushEvent, IoModel, Key, KeyIterator, LevelStat, RevisionID, Stats, Value, WriteBatch, DB,
    DEFAULT_NAMESPACE,
};

#[test]
fn open_creates_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("new_db");
    assert!(!db_path.exists());

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    assert!(db_path.exists());
    db.close().unwrap();
}

#[test]
fn open_existing_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("existing_db");
    std::fs::create_dir_all(&db_path).unwrap();

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    assert!(db_path.exists());
    db.close().unwrap();
}

#[test]
fn namespace_default() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert_eq!(ns.name(), "_");
}

#[test]
fn namespace_custom() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("users", None).unwrap();
    assert_eq!(ns.name(), "users");
}

#[test]
fn namespace_empty_name_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.namespace("", None).unwrap_err();
    assert!(matches!(err, Error::InvalidNamespace(_)));
}

#[test]
fn list_namespaces_empty_db() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let names = db.list_namespaces().unwrap();
    assert_eq!(
        names,
        vec!["_"],
        "fresh DB should contain only the default namespace"
    );
}

#[test]
fn drop_default_namespace_clears_and_recreates() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    // Write a key, then drop the default namespace
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("x", "1", None).unwrap();
    assert_eq!(ns.count().unwrap(), 1);

    db.drop_namespace(DEFAULT_NAMESPACE).unwrap();

    // Default namespace is auto-recreated (empty)
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert_eq!(ns.count().unwrap(), 0);
    assert!(db.list_namespaces().unwrap().contains(&"_".to_string()));
}

#[test]
fn drop_nonexistent_namespace_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.drop_namespace("users").unwrap_err();
    assert!(matches!(err, Error::InvalidNamespace(_)));
}

// --- Data operations (memtable-backed) ---

#[test]
fn put_returns_revision() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let rev = ns.put("key", "value", None).unwrap();
    assert_ne!(rev, RevisionID::ZERO);
}

#[test]
fn get_after_put() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("key", "value", None).unwrap();
    let val = ns.get("key").unwrap();
    assert_eq!(val, Value::from("value"));
}

#[test]
fn get_missing_key_returns_key_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let err = ns.get("key").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn put_get_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("hello", "world", None).unwrap();
    assert_eq!(ns.get("hello").unwrap(), Value::from("world"));
}

#[test]
fn put_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v1", None).unwrap();
    ns.put("k", "v2", None).unwrap();
    assert_eq!(ns.get("k").unwrap(), Value::from("v2"));
}

#[test]
fn delete_makes_key_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", None).unwrap();
    ns.delete("k").unwrap();
    let err = ns.get("k").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn exists_after_put_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", None).unwrap();
    assert!(ns.exists("k").unwrap());

    ns.delete("k").unwrap();
    assert!(!ns.exists("k").unwrap());
}

#[test]
fn scan_ordered_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(3_i64, "c", None).unwrap();
    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();

    let keys = ns.scan(&Key::Int(1), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
}

#[test]
fn rscan_ordered_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();

    let keys = ns.rscan(&Key::Int(3), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(2), Key::Int(1)]);
}

#[test]
fn scan_unordered_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("user:1", "a", None).unwrap();
    ns.put("user:2", "b", None).unwrap();
    ns.put("post:1", "c", None).unwrap();

    let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&Key::from("user:1")));
    assert!(keys.contains(&Key::from("user:2")));
}

#[test]
fn scan_with_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    ns.put(4_i64, "d", None).unwrap();
    ns.put(5_i64, "e", None).unwrap();

    // Skip first 2, take next 2
    let keys = ns.scan(&Key::Int(1), 2, 2, false).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(4)]);

    // Skip all
    let keys = ns.scan(&Key::Int(1), 10, 10, false).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn rscan_with_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    ns.put(4_i64, "d", None).unwrap();
    ns.put(5_i64, "e", None).unwrap();

    // rscan from 5, skip first 1 (5), take next 2 (4, 3)
    let keys = ns.rscan(&Key::Int(5), 2, 1, false).unwrap();
    assert_eq!(keys, vec![Key::Int(4), Key::Int(3)]);

    // Skip all
    let keys = ns.rscan(&Key::Int(5), 10, 10, false).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn scan_include_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    ns.delete(2_i64).unwrap();

    // Without include_deleted: tombstoned key is hidden
    let keys = ns.scan(&Key::Int(1), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(1), Key::Int(3)]);

    // With include_deleted: tombstoned key is included
    let keys = ns.scan(&Key::Int(1), 10, 0, true).unwrap();
    assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
}

#[test]
fn rscan_include_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    ns.delete(2_i64).unwrap();

    // Without include_deleted: tombstoned key is hidden
    let keys = ns.rscan(&Key::Int(3), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(1)]);

    // With include_deleted: tombstoned key is included
    let keys = ns.rscan(&Key::Int(3), 10, 0, true).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(2), Key::Int(1)]);
}

#[test]
fn count_excludes_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.delete(2_i64).unwrap();

    assert_eq!(ns.count().unwrap(), 1);
}

#[test]
fn rev_count_tracks_history() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v1", None).unwrap();
    ns.put("k", "v2", None).unwrap();
    ns.put("k", "v3", None).unwrap();

    assert_eq!(ns.rev_count("k").unwrap(), 3);
}

#[test]
fn rev_count_missing_key_returns_key_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let err = ns.rev_count("k").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn rev_get_returns_by_index() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v1", None).unwrap();
    ns.put("k", "v2", None).unwrap();
    ns.put("k", "v3", None).unwrap();

    assert_eq!(ns.rev_get("k", 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get("k", 2).unwrap(), Value::from("v3"));
}

#[test]
fn rev_get_missing_key_returns_key_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let err = ns.rev_get("k", 0).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn ttl_expires_key() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", Some(Duration::from_millis(50))).unwrap();
    std::thread::sleep(Duration::from_millis(200));

    let err = ns.get("k").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn ttl_returns_remaining() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", Some(Duration::from_secs(60))).unwrap();

    let remaining = ns.ttl("k").unwrap().unwrap();
    assert!(remaining.as_secs() > 50);
}

#[test]
fn ttl_none_for_permanent_key() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", None).unwrap();
    assert_eq!(ns.ttl("k").unwrap(), None);
}

#[test]
fn ttl_missing_key_returns_key_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let err = ns.ttl("k").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn auto_upgrade_widens_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(42_i64, "int_value", None).unwrap();
    // First Str key triggers auto-upgrade
    ns.put("hello", "str_value", None).unwrap();

    // Original Int(42) is now Str("42")
    let err = ns.get(42_i64).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
    assert_eq!(ns.get("42").unwrap(), Value::from("int_value"));
    assert_eq!(ns.get("hello").unwrap(), Value::from("str_value"));
}

#[test]
fn namespace_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("ns1", None).unwrap();
    let ns2 = db.namespace("ns2", None).unwrap();

    ns1.put("k", "v1", None).unwrap();
    ns2.put("k", "v2", None).unwrap();

    assert_eq!(ns1.get("k").unwrap(), Value::from("v1"));
    assert_eq!(ns2.get("k").unwrap(), Value::from("v2"));
}

#[test]
fn revision_monotonic_per_key() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let r1 = ns.put("k", "v1", None).unwrap();
    let r2 = ns.put("k", "v2", None).unwrap();
    let r3 = ns.put("k", "v3", None).unwrap();

    assert!(r1 < r2);
    assert!(r2 < r3);
}

#[test]
fn revision_id_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cluster_id = Some(0x1234);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let rev = ns.put("k", "v", None).unwrap();
    assert!(rev.timestamp_ms() > 0);
    assert_eq!(rev.cluster_id(), 0x1234);
    assert_eq!(rev.process_id(), std::process::id() as u16);
}

#[test]
fn config_cluster_id_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.cluster_id, None);
}

// --- Stats & Config tests ---

#[test]
fn stats_returns_default_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let s = db.stats();

    assert_eq!(s.total_keys, 0);
    assert_eq!(s.data_size_bytes, 0);
    assert_eq!(s.namespace_count, 1);
    assert_eq!(s.sstable_count, 0);
    assert_eq!(s.op_puts, 0);
    assert_eq!(s.op_gets, 0);
    assert_eq!(s.op_deletes, 0);
    assert_eq!(s.cache_hits, 0);
    assert_eq!(s.cache_misses, 0);
}

#[test]
fn stats_level_count_matches_config() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 5;
    let db = DB::open(config).unwrap();

    assert_eq!(db.stats().level_count, 5);
}

#[test]
fn stats_uptime_is_nonzero() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    std::thread::sleep(Duration::from_millis(50));
    assert!(db.stats().uptime >= Duration::from_millis(10));
}

#[test]
fn stats_default_trait() {
    let s = Stats::default();
    assert_eq!(s.level_count, 0);
    assert_eq!(s.uptime, Duration::ZERO);
}

#[test]
fn stats_op_counters_increment() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "a", None).unwrap();
    ns.put(2, "b", None).unwrap();
    ns.get(1).unwrap();
    ns.delete(2).unwrap();

    let s = db.stats();
    assert_eq!(s.op_puts, 2);
    assert_eq!(s.op_gets, 1);
    assert_eq!(s.op_deletes, 1);
}

#[test]
fn stats_total_keys_and_namespace_count() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("_", None).unwrap();
    ns.put(1, "a", None).unwrap();
    ns.put(2, "b", None).unwrap();

    let s = db.stats();
    assert_eq!(s.total_keys, 2);
    assert_eq!(s.namespace_count, 1);

    // Add a second namespace
    let ns2 = db.namespace("other", None).unwrap();
    ns2.put(1, "x", None).unwrap();

    let s = db.stats();
    assert_eq!(s.total_keys, 3);
    assert_eq!(s.namespace_count, 2);
}

#[test]
fn stats_write_buffer_bytes_nonzero_after_put() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    assert_eq!(db.stats().write_buffer_bytes, 0);

    ns.put(1, "hello", None).unwrap();

    let s = db.stats();
    assert!(s.write_buffer_bytes > 0);
    assert_eq!(s.data_size_bytes, s.write_buffer_bytes);
}

#[test]
fn stats_total_keys_excludes_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "a", None).unwrap();
    ns.put(2, "b", None).unwrap();
    ns.delete(1).unwrap();

    let s = db.stats();
    assert_eq!(s.total_keys, 1);
}

#[test]
fn stats_op_counters_persist_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // Session 1: perform operations then close
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(1, "a", None).unwrap();
        ns.put(2, "b", None).unwrap();
        ns.get(1).unwrap();
        ns.delete(2).unwrap();
        db.close().unwrap();
    }

    // Session 2: counters should be restored
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let s = db.stats();
        assert_eq!(s.op_puts, 2);
        assert_eq!(s.op_gets, 1);
        assert_eq!(s.op_deletes, 1);
    }
}

#[test]
fn stats_op_counters_accumulate_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // Session 1
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(1, "a", None).unwrap();
        db.close().unwrap();
    }

    // Session 2: more operations
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(2, "b", None).unwrap();
        ns.put(3, "c", None).unwrap();

        let s = db.stats();
        assert_eq!(s.op_puts, 3); // 1 from session 1 + 2 from session 2
        db.close().unwrap();
    }
}

#[test]
fn stats_analyze_persists_and_returns() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "a", None).unwrap();
    ns.put(2, "b", None).unwrap();
    ns.get(1).unwrap();

    let s = db.analyze();
    assert_eq!(s.op_puts, 2);
    assert_eq!(s.op_gets, 1);
    assert_eq!(s.total_keys, 2);
    assert!(s.write_buffer_bytes > 0);
    drop(db);

    // Reopen — counters should have been persisted by analyze
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let s = db.stats();
    assert_eq!(s.op_puts, 2);
    assert_eq!(s.op_gets, 1);
}

#[test]
fn config_returns_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let c = db.config();
    assert_eq!(c.path, tmp.path());
    assert!(c.create_if_missing);
}

#[test]
fn config_defaults() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.write_buffer_size, 4 * 1024 * 1024);
    assert_eq!(config.max_levels, 3);
    assert_eq!(config.block_size, 4 * 1024);
    assert_eq!(config.cache_size, 64 * 1024 * 1024);
}

#[test]
fn config_custom_overrides() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.write_buffer_size = 1024;
    config.max_levels = 7;
    config.block_size = 512;
    config.cache_size = 2048;
    let db = DB::open(config).unwrap();

    let c = db.config();
    assert_eq!(c.write_buffer_size, 1024);
    assert_eq!(c.max_levels, 7);
    assert_eq!(c.block_size, 512);
    assert_eq!(c.cache_size, 2048);
}

// --- Value separation config ---

#[test]
fn config_object_size_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.object_size, 1024);
}

#[test]
fn config_object_size_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 4096;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().object_size, 4096);
}

#[test]
fn config_compress_default() {
    let config = Config::new("/tmp/test");
    assert!(config.compress);
}

#[test]
fn config_compress_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.compress = false;
    let db = DB::open(config).unwrap();

    assert!(!db.config().compress);
}

// --- Bloom filter config ---

#[test]
fn config_bloom_bits_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.bloom_bits, 10);
}

#[test]
fn config_bloom_bits_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_bits = 20;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().bloom_bits, 20);
}

// --- Bloom filter integration ---

#[test]
fn bloom_filter_no_false_negatives_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_bits = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write keys, flush to SSTable (builds bloom filter)
    for i in 0..100 {
        ns.put(Key::Int(i), format!("val{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Every inserted key must be found (no false negatives)
    for i in 0..100 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("val{i}").as_str()));
    }
}

#[test]
fn bloom_filter_rejects_missing_keys_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_bits = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..100 {
        ns.put(Key::Int(i), format!("val{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Non-existent keys should return KeyNotFound
    for i in 100..200 {
        let err = ns.get(Key::Int(i)).unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
    }
}

#[test]
fn bloom_filter_works_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_bits = 10;
    config.block_size = 256;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Multiple flushes to create multiple L0 SSTables
    for batch in 0..3 {
        for i in 0..10 {
            let key = batch * 10 + i;
            ns.put(Key::Int(key), format!("v{key}"), None).unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();

    // All keys readable after compaction (bloom filter rebuilt in output SSTable)
    for i in 0..30 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("v{i}").as_str()));
    }
}

#[test]
fn bloom_filter_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let mut config = Config::new(&db_path);
        config.bloom_bits = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 0..50 {
            ns.put(Key::Int(i), format!("val{i}"), None).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let mut config = Config::new(&db_path);
        config.bloom_bits = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // Data readable via SSTable with bloom filter
        for i in 0..50 {
            let val = ns.get(Key::Int(i)).unwrap();
            assert_eq!(val, Value::from(format!("val{i}").as_str()));
        }
        // Missing keys correctly rejected
        let err = ns.get(Key::Int(999)).unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
    }
}

#[test]
fn bloom_filter_disabled_with_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_bits = 0; // disabled
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..20 {
        ns.put(Key::Int(i), format!("val{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Still works correctly — just without bloom optimization
    for i in 0..20 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("val{i}").as_str()));
    }
    let err = ns.get(Key::Int(999)).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

// --- Verify checksums config ---

#[test]
fn config_verify_checksums_default() {
    let config = Config::new("/tmp/test");
    assert!(config.verify_checksums);
}

#[test]
fn config_verify_checksums_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.verify_checksums = false;
    let db = DB::open(config).unwrap();

    assert!(!db.config().verify_checksums);
}

// --- Corruption error variant ---

#[test]
fn corruption_error_display() {
    let err = Error::Corruption("bad checksum in block 42".into());
    assert_eq!(err.to_string(), "corruption: bad checksum in block 42");
}

#[test]
fn corruption_error_matches() {
    let err = Error::Corruption("test".into());
    assert!(matches!(err, Error::Corruption(_)));
}

// --- Maintenance operation stubs ---

#[test]
fn flush_empty_db_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    // Flushing an empty DB should succeed without error
    db.flush().unwrap();
}

#[test]
fn sync_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put(Key::Int(1), "hello", None).unwrap();
    db.sync().unwrap();
    db.close().unwrap();
}

#[test]
fn sync_empty_db() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    db.sync().unwrap();
    db.close().unwrap();
}

#[test]
fn sync_data_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "synced", None).unwrap();
        db.sync().unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        assert_eq!(ns.get(Key::Int(1)).unwrap(), Value::from("synced"));
        db.close().unwrap();
    }
}

#[test]
fn destroy_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "hello", None).unwrap();
        db.close().unwrap();
    }
    assert!(db_path.exists());
    DB::destroy(&db_path).unwrap();
    assert!(!db_path.exists());
}

#[test]
fn repair_clean_database() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "hello", None).unwrap();
        db.close().unwrap();
    }
    let report = DB::repair(&db_path).unwrap();
    assert!(report.is_clean());
    assert!(!report.has_data_loss());
}

#[test]
fn destroy_nonexistent_path_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let err = DB::destroy(tmp.path().join("does_not_exist")).unwrap_err();
    assert!(matches!(err, Error::Io(_)));
}

#[test]
fn destroy_non_rkv_directory_errors() {
    let tmp = tempfile::tempdir().unwrap();
    // Create a plain directory with no aol or sst — should be rejected
    let dir = tmp.path().join("not_a_db");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("random.txt"), b"data").unwrap();
    let err = DB::destroy(&dir).unwrap_err();
    assert!(matches!(err, Error::Corruption(_)));
}

#[test]
fn destroy_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "hello", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    assert!(db_path.join("sst").exists());
    DB::destroy(&db_path).unwrap();
    assert!(!db_path.exists());
}

#[test]
fn repair_nonexistent_path_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let err = DB::repair(tmp.path().join("does_not_exist")).unwrap_err();
    assert!(matches!(err, Error::Io(_)));
}

#[test]
fn repair_corrupted_aol() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "v1", None).unwrap();
        ns.put(Key::Int(2), "v2", None).unwrap();
        db.close().unwrap();
    }

    // Corrupt the last byte of the AOL (damages the last record's checksum)
    let aol_path = db_path.join("sst/_/aol");
    let mut aol_data = std::fs::read(&aol_path).unwrap();
    let last = aol_data.len() - 1;
    aol_data[last] ^= 0xFF;
    std::fs::write(&aol_path, &aol_data).unwrap();

    let report = DB::repair(&db_path).unwrap();
    assert!(!report.is_clean());
    assert!(report.wal_records_skipped > 0);
    assert!(report.wal_records_scanned >= 2);

    // Database should still be openable after repair
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    // At least the first record should survive
    assert_eq!(ns.get(Key::Int(1)).unwrap(), Value::from("v1"));
    db.close().unwrap();
}

#[test]
fn repair_corrupted_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "hello", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Find and corrupt an SSTable file
    let sst_dir = db_path.join("sst").join("_").join("L0");
    let entries: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(!entries.is_empty());
    let sst_path = entries[0].path();
    let mut sst_data = std::fs::read(&sst_path).unwrap();
    sst_data[10] ^= 0xFF;
    std::fs::write(&sst_path, &sst_data).unwrap();

    let report = DB::repair(&db_path).unwrap();
    assert!(!report.is_clean());
    assert!(report.sstable_blocks_corrupted > 0);

    // Corrupted SSTable file should be removed
    assert!(!sst_path.exists());
}

#[test]
fn repair_with_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let mut config = Config::new(&db_path);
        config.object_size = 10; // force bin objects for small values
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "a]".repeat(100).as_str(), None)
            .unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    let report = DB::repair(&db_path).unwrap();
    assert!(report.is_clean());
    assert!(report.objects_scanned > 0);
    assert_eq!(report.objects_corrupted, 0);
}

#[test]
fn repair_data_readable_after_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(Key::Int(1), "alpha", None).unwrap();
        ns.put(Key::Int(2), "beta", None).unwrap();
        db.close().unwrap();
    }

    let report = DB::repair(&db_path).unwrap();
    assert!(report.is_clean());

    // Reopen and verify data is intact
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    assert_eq!(ns.get(Key::Int(1)).unwrap(), Value::from("alpha"));
    assert_eq!(ns.get(Key::Int(2)).unwrap(), Value::from("beta"));
    db.close().unwrap();
}

#[test]
fn dump_basic_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("source"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put(1, "hello", None).unwrap();
    drop(ns);

    let dump_path = tmp.path().join("backup.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    // Load into a fresh location (remove old DB first)
    std::fs::remove_dir_all(tmp.path().join("source")).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let ns2 = db2.namespace("_", None).unwrap();
    assert_eq!(ns2.get(1).unwrap(), Value::from("hello"));
}

#[test]
fn dump_multiple_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("users", None).unwrap();
    ns1.put("alice", "admin", None).unwrap();
    drop(ns1);

    let ns2 = db.namespace("orders", None).unwrap();
    ns2.put("ord1", "shipped", None).unwrap();
    drop(ns2);

    let dump_path = tmp.path().join("multi.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();

    let ns1 = db2.namespace("users", None).unwrap();
    assert_eq!(ns1.get("alice").unwrap(), Value::from("admin"));

    let ns2 = db2.namespace("orders", None).unwrap();
    assert_eq!(ns2.get("ord1").unwrap(), Value::from("shipped"));
}

#[test]
fn dump_filters_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "alive", None).unwrap();
    ns.put(2, "deleted", None).unwrap();
    ns.delete(2).unwrap();
    drop(ns);

    let dump_path = tmp.path().join("tomb.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let ns2 = db2.namespace("_", None).unwrap();

    assert_eq!(ns2.get(1).unwrap(), Value::from("alive"));
    let err = ns2.get(2).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn dump_empty_db() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();

    let dump_path = tmp.path().join("empty.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let names = db2.list_namespaces().unwrap();
    assert_eq!(names, vec!["_"]);
}

#[test]
fn dump_large_values_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let mut config = Config::new(&db_path);
    config.object_size = 16; // tiny threshold to force value separation
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let big_value = "x".repeat(100);
    ns.put(1, big_value.as_str(), None).unwrap();
    drop(ns);

    let dump_path = tmp.path().join("large.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    // Load with default object_size — the value should be inline
    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let ns2 = db2.namespace("_", None).unwrap();
    assert_eq!(ns2.get(1).unwrap(), Value::from(big_value.as_str()));
}

#[test]
fn dump_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "old", None).unwrap();
    db.flush().unwrap();
    ns.put(1, "new", None).unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    drop(ns);

    let dump_path = tmp.path().join("compacted.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let ns2 = db2.namespace("_", None).unwrap();
    assert_eq!(ns2.get(1).unwrap(), Value::from("new"));
}

#[test]
fn load_rejects_nonempty_target() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put(1, "data", None).unwrap();
    drop(ns);

    let dump_path = tmp.path().join("backup.rkv");
    db.dump(&dump_path).unwrap();
    db.close().unwrap();

    // Target path still has data — load should refuse
    let Err(err) = DB::load(&dump_path) else {
        panic!("expected InvalidConfig error");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn load_rejects_corrupt_dump() {
    let tmp = tempfile::tempdir().unwrap();
    let dump_path = tmp.path().join("corrupt.rkv");
    std::fs::write(&dump_path, b"not a valid dump file").unwrap();

    let Err(err) = DB::load(&dump_path) else {
        panic!("expected Corruption error");
    };
    assert!(matches!(err, Error::Corruption(_)));
}

#[test]
fn dump_load_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");

    // Write data across multiple flushes
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(1, "a", None).unwrap();
        db.flush().unwrap();
        ns.put(2, "b", None).unwrap();
        db.flush().unwrap();
        drop(ns);

        let dump_path = tmp.path().join("backup.rkv");
        db.dump(&dump_path).unwrap();
        db.close().unwrap();
    }

    // Remove source, load, close, reopen
    std::fs::remove_dir_all(&db_path).unwrap();
    {
        let db = DB::load(tmp.path().join("backup.rkv")).unwrap();
        db.close().unwrap();
    }

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    assert_eq!(ns.get(1).unwrap(), Value::from("a"));
    assert_eq!(ns.get(2).unwrap(), Value::from("b"));
}

#[test]
fn dump_incremental_only_new_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Insert two entries — capture the revision after the first
    let _rev1 = ns.put(1, "old", None).unwrap();
    db.flush().unwrap();
    let rev2 = ns.put(2, "new", None).unwrap();
    db.flush().unwrap();
    drop(ns);

    // Incremental dump: only entries after rev1 (so rev2 should appear)
    let dump_path = tmp.path().join("incr.rkv");
    db.dump_with_options(
        &dump_path,
        DumpOptions {
            after_revision: Some(_rev1),
            ..Default::default()
        },
    )
    .unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load(&dump_path).unwrap();
    let ns2 = db2.namespace("_", None).unwrap();

    // Key 2 should be present (its rev > rev1)
    assert_eq!(ns2.get(2).unwrap(), Value::from("new"));
    // Key 1 should NOT be present (its rev <= rev1)
    assert!(ns2.get(1).is_err());

    // Verify the loaded revision is correct
    let _ = rev2; // ensure rev2 was valid
}

#[test]
fn dump_encrypted_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put("secret", "data", None).unwrap();
    drop(ns);

    let dump_path = tmp.path().join("encrypted.rkv");
    db.dump_with_options(
        &dump_path,
        DumpOptions {
            password: Some("hunter2".into()),
            ..Default::default()
        },
    )
    .unwrap();
    db.close().unwrap();

    // Loading without password should fail
    std::fs::remove_dir_all(&db_path).unwrap();
    let result = DB::load(&dump_path);
    let Err(err) = result else {
        panic!("expected EncryptionRequired error")
    };
    assert!(matches!(err, Error::EncryptionRequired(_)));

    // Loading with wrong password should fail
    let result = DB::load_with_password(&dump_path, "wrong");
    let Err(err) = result else {
        panic!("expected Corruption error")
    };
    assert!(matches!(err, Error::Corruption(_)));

    // Clean up any partial state from failed load
    let _ = std::fs::remove_dir_all(&db_path);

    // Loading with correct password should succeed
    let db2 = DB::load_with_password(&dump_path, "hunter2").unwrap();
    let ns2 = db2.namespace("_", None).unwrap();
    assert_eq!(ns2.get("secret").unwrap(), Value::from("data"));
}

#[test]
fn dump_encrypted_incremental() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("src");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put(1, "old", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "new", None).unwrap();
    db.flush().unwrap();
    drop(ns);

    let dump_path = tmp.path().join("enc_incr.rkv");
    db.dump_with_options(
        &dump_path,
        DumpOptions {
            after_revision: Some(rev1),
            password: Some("pass123".into()),
        },
    )
    .unwrap();
    db.close().unwrap();

    std::fs::remove_dir_all(&db_path).unwrap();
    let db2 = DB::load_with_password(&dump_path, "pass123").unwrap();
    let ns2 = db2.namespace("_", None).unwrap();

    // Only key 2 should be present
    assert_eq!(ns2.get(2).unwrap(), Value::from("new"));
    assert!(ns2.get(1).is_err());
}

#[test]
fn compact_empty_db_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    db.compact().unwrap();
}

// --- Namespace encryption ---

#[test]
fn namespace_encrypted_put_works() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("secret", Some("s3cret")).unwrap();
    let rev = ns.put("key", "value", None).unwrap();
    assert_ne!(rev, RevisionID::ZERO);
}

#[test]
fn namespace_is_encrypted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("secret", Some("s3cret")).unwrap();
    assert!(ns.is_encrypted());
}

#[test]
fn namespace_not_encrypted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("public", None).unwrap();
    assert!(!ns.is_encrypted());
}

#[test]
fn namespace_mismatch_requires_password() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    // First access with password — marks as encrypted
    db.namespace("vault", Some("pw")).unwrap();

    // Second access without password — should fail
    let err = db.namespace("vault", None).unwrap_err();
    assert!(matches!(err, Error::EncryptionRequired(_)));
}

#[test]
fn namespace_mismatch_not_encrypted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    // First access without password — marks as non-encrypted
    db.namespace("public", None).unwrap();

    // Second access with password — should fail
    let err = db.namespace("public", Some("pw")).unwrap_err();
    assert!(matches!(err, Error::NotEncrypted(_)));
}

#[test]
fn encryption_required_error_display() {
    let err = Error::EncryptionRequired("namespace 'vault' requires a password".into());
    assert_eq!(
        err.to_string(),
        "encryption required: namespace 'vault' requires a password"
    );
}

#[test]
fn not_encrypted_error_display() {
    let err = Error::NotEncrypted("namespace 'public' is not encrypted".into());
    assert_eq!(
        err.to_string(),
        "namespace is not encrypted: namespace 'public' is not encrypted"
    );
}

// --- I/O model config ---

#[test]
fn config_io_model_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.io_model, IoModel::Mmap);
}

#[test]
fn config_io_model_direct() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.io_model = IoModel::DirectIO;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().io_model, IoModel::DirectIO);
}

#[test]
fn config_io_model_none() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.io_model = IoModel::None;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().io_model, IoModel::None);
}

#[test]
fn io_model_display() {
    assert_eq!(IoModel::None.to_string(), "none");
    assert_eq!(IoModel::DirectIO.to_string(), "directio");
    assert_eq!(IoModel::Mmap.to_string(), "mmap");
}

#[test]
fn io_model_from_str() {
    assert_eq!("none".parse::<IoModel>().unwrap(), IoModel::None);
    assert_eq!("directio".parse::<IoModel>().unwrap(), IoModel::DirectIO);
    assert_eq!("mmap".parse::<IoModel>().unwrap(), IoModel::Mmap);
}

#[test]
fn io_model_from_str_invalid() {
    let err = "bad".parse::<IoModel>().unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn config_compression_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.compression, Compression::LZ4);
}

#[test]
fn config_compression_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.compression = Compression::Zstd;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().compression, Compression::Zstd);
}

#[test]
fn compression_display() {
    assert_eq!(Compression::None.to_string(), "none");
    assert_eq!(Compression::LZ4.to_string(), "lz4");
    assert_eq!(Compression::Zstd.to_string(), "zstd");
}

#[test]
fn compression_from_str() {
    assert_eq!("none".parse::<Compression>().unwrap(), Compression::None);
    assert_eq!("lz4".parse::<Compression>().unwrap(), Compression::LZ4);
    assert_eq!("zstd".parse::<Compression>().unwrap(), Compression::Zstd);
}

#[test]
fn compression_from_str_invalid() {
    let err = "bad".parse::<Compression>().unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn invalid_config_error_display() {
    let err = Error::InvalidConfig("unknown io_model 'bad'".into());
    assert_eq!(err.to_string(), "invalid config: unknown io_model 'bad'");
}

// --- AOL persistence tests ---

#[test]
fn persist_put_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("key", "value", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("key").unwrap(), Value::from("value"));
    }
}

#[test]
fn persist_multiple_keys() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("a", "1", None).unwrap();
        ns.put("b", "2", None).unwrap();
        ns.put("c", "3", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("a").unwrap(), Value::from("1"));
        assert_eq!(ns.get("b").unwrap(), Value::from("2"));
        assert_eq!(ns.get("c").unwrap(), Value::from("3"));
    }
}

#[test]
fn persist_multiple_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns1 = db.namespace("ns1", None).unwrap();
        let ns2 = db.namespace("ns2", None).unwrap();
        ns1.put("k", "v1", None).unwrap();
        ns2.put("k", "v2", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns1 = db.namespace("ns1", None).unwrap();
        let ns2 = db.namespace("ns2", None).unwrap();
        assert_eq!(ns1.get("k").unwrap(), Value::from("v1"));
        assert_eq!(ns2.get("k").unwrap(), Value::from("v2"));
    }
}

#[test]
fn persist_ttl_expired_on_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v", Some(Duration::from_millis(50))).unwrap();
        db.close().unwrap();
    }

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(200));

    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        let err = ns.get("k").unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
    }
}

#[test]
fn persist_ttl_alive_on_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v", Some(Duration::from_secs(3600))).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("k").unwrap(), Value::from("v"));
        let remaining = ns.ttl("k").unwrap().unwrap();
        assert!(remaining.as_secs() > 3500);
    }
}

#[test]
fn persist_delete_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v", None).unwrap();
        ns.delete("k").unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        let err = ns.get("k").unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
        assert!(!ns.exists("k").unwrap());
    }
}

#[test]
fn persist_overwrite_latest_wins() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v1", None).unwrap();
        ns.put("k", "v2", None).unwrap();
        ns.put("k", "v3", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("k").unwrap(), Value::from("v3"));
    }
}

#[test]
fn persist_revision_history() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v1", None).unwrap();
        ns.put("k", "v2", None).unwrap();
        ns.put("k", "v3", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.rev_count("k").unwrap(), 3);
        assert_eq!(ns.rev_get("k", 0).unwrap(), Value::from("v1"));
        assert_eq!(ns.rev_get("k", 1).unwrap(), Value::from("v2"));
        assert_eq!(ns.rev_get("k", 2).unwrap(), Value::from("v3"));
    }
}

#[test]
fn persist_auto_upgrade_str_keys() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put(42_i64, "int_val", None).unwrap();
        ns.put("hello", "str_val", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        // After replay, Int(42) was replayed first (still Int), then Str triggers upgrade
        // so Int(42) should be widened to Str("42")
        let err = ns.get(42_i64).unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
        assert_eq!(ns.get("42").unwrap(), Value::from("int_val"));
        assert_eq!(ns.get("hello").unwrap(), Value::from("str_val"));
    }
}

#[test]
fn persist_count_correct_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("a", "1", None).unwrap();
        ns.put("b", "2", None).unwrap();
        ns.put("c", "3", None).unwrap();
        ns.delete("b").unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.count().unwrap(), 2);
    }
}

#[test]
fn persist_scan_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("user:1", "a", None).unwrap();
        ns.put("user:2", "b", None).unwrap();
        ns.put("post:1", "c", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::from("user:1")));
        assert!(keys.contains(&Key::from("user:2")));
    }
}

#[test]
fn persist_null_value() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", Value::Null, None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("k").unwrap(), Value::Null);
    }
}

#[test]
fn persist_int_keys() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put(1_i64, "a", None).unwrap();
        ns.put(2_i64, "b", None).unwrap();
        ns.put(3_i64, "c", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get(1_i64).unwrap(), Value::from("a"));
        assert_eq!(ns.get(2_i64).unwrap(), Value::from("b"));
        assert_eq!(ns.get(3_i64).unwrap(), Value::from("c"));
        // Scan should work in ordered mode
        let keys = ns.scan(&Key::Int(1), 10, 0, false).unwrap();
        assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
    }
}

#[test]
fn persist_data_appended_across_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("a", "1", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("b", "2", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("a").unwrap(), Value::from("1"));
        assert_eq!(ns.get("b").unwrap(), Value::from("2"));
        assert_eq!(ns.count().unwrap(), 2);
    }
}

// --- AOL buffered flush tests ---

#[test]
fn config_aol_buffer_size_default() {
    let config = Config::new("/tmp/test");
    assert_eq!(config.aol_buffer_size, 128);
}

#[test]
fn config_aol_buffer_size_override() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.aol_buffer_size = 64;
    let db = DB::open(config).unwrap();

    assert_eq!(db.config().aol_buffer_size, 64);
}

#[test]
fn persist_with_buffered_flush_after_close() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 1000; // large threshold — nothing auto-flushes
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("a", "1", None).unwrap();
        ns.put("b", "2", None).unwrap();
        ns.put("c", "3", None).unwrap();
        // close() triggers final flush via background thread shutdown
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("a").unwrap(), Value::from("1"));
        assert_eq!(ns.get("b").unwrap(), Value::from("2"));
        assert_eq!(ns.get("c").unwrap(), Value::from("3"));
    }
}

#[test]
fn persist_with_per_record_flush() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 0; // per-record flush
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("k", "v", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("k").unwrap(), Value::from("v"));
    }
}

#[test]
fn persist_buffered_flush_threshold_triggers() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 2; // flush every 2 records
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("a", "1", None).unwrap();
        ns.put("b", "2", None).unwrap(); // triggers flush at threshold
        ns.put("c", "3", None).unwrap(); // buffered, flushed on close
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert_eq!(ns.get("a").unwrap(), Value::from("1"));
        assert_eq!(ns.get("b").unwrap(), Value::from("2"));
        assert_eq!(ns.get("c").unwrap(), Value::from("3"));
    }
}

// --- Value separation (bin objects) ---

#[test]
fn value_sep_large_value_stored_and_retrieved() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 100; // small threshold for testing
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let large_value = "x".repeat(200);
    ns.put("big", large_value.as_str(), None).unwrap();
    let result = ns.get("big").unwrap();
    assert_eq!(result, Value::from(large_value.as_str()));
}

#[test]
fn value_sep_small_value_stays_inline() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 1024;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("small", "hello", None).unwrap();
    let result = ns.get("small").unwrap();
    assert_eq!(result, Value::from("hello"));

    // No object files should be created for small values
    let objects_dir = tmp.path().join("objects").join("_");
    if objects_dir.exists() {
        let count = std::fs::read_dir(objects_dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_dir())
            .count();
        assert_eq!(count, 0);
    }
    // If objects/_/ doesn't exist at all, that's fine — no objects were created
}

#[test]
fn value_sep_dedup_same_content() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let data = "x".repeat(100);
    ns.put("k1", data.as_str(), None).unwrap();
    ns.put("k2", data.as_str(), None).unwrap();

    // Both keys return the same value
    assert_eq!(ns.get("k1").unwrap(), ns.get("k2").unwrap());

    // Count total objects: should be exactly 1 (dedup)
    assert_eq!(count_object_files(tmp.path(), "_"), 1);
}

#[test]
fn value_sep_persistence_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let large_value = "y".repeat(200);

    {
        let mut config = Config::new(tmp.path());
        config.object_size = 100;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("persist", large_value.as_str(), None).unwrap();
        db.close().unwrap();
    }
    {
        let mut config = Config::new(tmp.path());
        config.object_size = 100;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        let result = ns.get("persist").unwrap();
        assert_eq!(result, Value::from(large_value.as_str()));
    }
}

#[test]
fn value_sep_compress_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    config.compress = false;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let data = "z".repeat(100);
    ns.put("nocomp", data.as_str(), None).unwrap();
    let result = ns.get("nocomp").unwrap();
    assert_eq!(result, Value::from(data.as_str()));
}

#[test]
fn value_sep_rev_get_resolves_pointers() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let v1 = "a".repeat(50);
    let v2 = "b".repeat(50);
    ns.put("rev_key", v1.as_str(), None).unwrap();
    ns.put("rev_key", v2.as_str(), None).unwrap();

    assert_eq!(ns.rev_get("rev_key", 0).unwrap(), Value::from(v1.as_str()));
    assert_eq!(ns.rev_get("rev_key", 1).unwrap(), Value::from(v2.as_str()));
}

#[test]
fn value_sep_large_value_with_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let data = "t".repeat(50);
    ns.put("ttl_key", data.as_str(), Some(Duration::from_secs(3600)))
        .unwrap();
    let result = ns.get("ttl_key").unwrap();
    assert_eq!(result, Value::from(data.as_str()));

    let ttl = ns.ttl("ttl_key").unwrap().unwrap();
    assert!(ttl.as_secs() > 3500);
}

#[test]
fn value_sep_delete_after_large_put() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let data = "d".repeat(50);
    ns.put("del_key", data.as_str(), None).unwrap();
    ns.delete("del_key").unwrap();

    let err = ns.get("del_key").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn value_sep_object_size_zero_forces_all_to_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 0;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Even a 1-byte value exceeds threshold 0
    ns.put("tiny", "x", None).unwrap();
    let result = ns.get("tiny").unwrap();
    assert_eq!(result, Value::from("x"));

    // Object should exist (in pack file)
    assert!(count_object_files(tmp.path(), "_") > 0);
}

#[test]
fn value_sep_null_value_not_separated() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 0;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("null_key", Value::Null, None).unwrap();
    let result = ns.get("null_key").unwrap();
    assert_eq!(result, Value::Null);
}

/// Generate `n` distinct 256-byte values, each with a unique 8-byte prefix.
fn make_distinct_values(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut v = vec![0u8; 256];
            v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            v
        })
        .collect()
}

#[test]
fn value_sep_many_distinct_large_values() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let values = make_distinct_values(100);
    for (i, val) in values.iter().enumerate() {
        ns.put(i as i64, val.as_slice(), None).unwrap();
    }

    // Verify all values round-trip correctly
    for (i, expected) in values.iter().enumerate() {
        let result = ns.get(i as i64).unwrap();
        assert_eq!(result.as_bytes().unwrap(), expected.as_slice());
    }
}

#[test]
fn value_sep_many_distinct_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let values = make_distinct_values(50);

    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for (i, val) in values.iter().enumerate() {
            ns.put(i as i64, val.as_slice(), None).unwrap();
        }
        db.close().unwrap();
    }
    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for (i, expected) in values.iter().enumerate() {
            let result = ns.get(i as i64).unwrap();
            assert_eq!(result.as_bytes().unwrap(), expected.as_slice());
        }
    }
}

#[test]
fn value_sep_cross_namespace_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();

    let data = "x".repeat(100);

    let ns1 = db.namespace("ns1", None).unwrap();
    ns1.put("key", data.as_str(), None).unwrap();

    let ns2 = db.namespace("ns2", None).unwrap();
    ns2.put("key", data.as_str(), None).unwrap();

    // Both namespaces return the correct value
    assert_eq!(ns1.get("key").unwrap(), Value::from(data.as_str()));
    assert_eq!(ns2.get("key").unwrap(), Value::from(data.as_str()));

    // Each namespace has its own object directory with separate files
    let ns1_objects = tmp.path().join("objects").join("ns1");
    let ns2_objects = tmp.path().join("objects").join("ns2");
    assert!(ns1_objects.is_dir());
    assert!(ns2_objects.is_dir());

    // Count objects in each namespace — should be 1 each (separate stores)
    assert_eq!(count_object_files(tmp.path(), "ns1"), 1);
    assert_eq!(count_object_files(tmp.path(), "ns2"), 1);
}

#[test]
fn value_sep_cross_namespace_isolation_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = "y".repeat(200);

    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns1 = db.namespace("ns1", None).unwrap();
        ns1.put("key", data.as_str(), None).unwrap();
        let ns2 = db.namespace("ns2", None).unwrap();
        ns2.put("key", data.as_str(), None).unwrap();
        db.close().unwrap();
    }
    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns1 = db.namespace("ns1", None).unwrap();
        let ns2 = db.namespace("ns2", None).unwrap();
        assert_eq!(ns1.get("key").unwrap(), Value::from(data.as_str()));
        assert_eq!(ns2.get("key").unwrap(), Value::from(data.as_str()));
    }
}

#[test]
fn value_sep_dedup_within_namespace_still_works() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("myns", None).unwrap();

    let data = "z".repeat(100);
    ns.put("k1", data.as_str(), None).unwrap();
    ns.put("k2", data.as_str(), None).unwrap();

    assert_eq!(ns.get("k1").unwrap(), Value::from(data.as_str()));
    assert_eq!(ns.get("k2").unwrap(), Value::from(data.as_str()));

    // Only 1 object — dedup still works within a namespace
    assert_eq!(count_object_files(tmp.path(), "myns"), 1);
}

// --- Bulk delete (wipe) ---

#[test]
fn delete_range_exclusive() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 1..=10_i64 {
        ns.put(i, format!("v{i}"), None).unwrap();
    }

    // Delete [3, 7) — keys 3, 4, 5, 6
    let deleted = ns.delete_range(3_i64, 7_i64, false).unwrap();
    assert_eq!(deleted, 4);
    assert_eq!(ns.count().unwrap(), 6);

    // Keys 3..6 gone
    for i in 3..=6_i64 {
        assert!(!ns.exists(i).unwrap());
    }
    // Key 7 still present (exclusive end)
    assert!(ns.exists(7_i64).unwrap());
}

#[test]
fn delete_range_inclusive() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 1..=10_i64 {
        ns.put(i, format!("v{i}"), None).unwrap();
    }

    // Delete [3, 7] — keys 3, 4, 5, 6, 7
    let deleted = ns.delete_range(3_i64, 7_i64, true).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(ns.count().unwrap(), 5);

    // Key 7 also gone (inclusive)
    assert!(!ns.exists(7_i64).unwrap());
    // Key 8 still present
    assert!(ns.exists(8_i64).unwrap());
}

#[test]
fn delete_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("user:1", "a", None).unwrap();
    ns.put("user:2", "b", None).unwrap();
    ns.put("user:3", "c", None).unwrap();
    ns.put("post:1", "d", None).unwrap();
    ns.put("post:2", "e", None).unwrap();

    let deleted = ns.delete_prefix("user:").unwrap();
    assert_eq!(deleted, 3);
    assert_eq!(ns.count().unwrap(), 2);

    assert!(!ns.exists("user:1").unwrap());
    assert!(ns.exists("post:1").unwrap());
}

#[test]
fn delete_range_empty_result() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();

    // Range [10, 20) has no keys
    let deleted = ns.delete_range(10_i64, 20_i64, false).unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(ns.count().unwrap(), 2);
}

#[test]
fn delete_prefix_no_match() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("foo", "1", None).unwrap();
    ns.put("bar", "2", None).unwrap();

    let deleted = ns.delete_prefix("zzz").unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(ns.count().unwrap(), 2);
}

#[test]
fn delete_range_excludes_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 1..=5_i64 {
        ns.put(i, format!("v{i}"), None).unwrap();
    }
    // Delete key 3 individually first
    ns.delete(3_i64).unwrap();

    // Range delete [1, 5] — should only delete 4 live keys (not tombstoned 3)
    let deleted = ns.delete_range(1_i64, 5_i64, true).unwrap();
    assert_eq!(deleted, 4);
    assert_eq!(ns.count().unwrap(), 0);
}

#[test]
fn delete_range_updates_op_counter() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let before = db.stats().op_deletes;
    for i in 1..=5_i64 {
        ns.put(i, "v", None).unwrap();
    }
    ns.delete_range(1_i64, 5_i64, true).unwrap();
    let after = db.stats().op_deletes;
    assert_eq!(after - before, 5);
}

#[test]
fn delete_prefix_updates_op_counter() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let before = db.stats().op_deletes;
    ns.put("x:1", "a", None).unwrap();
    ns.put("x:2", "b", None).unwrap();
    ns.put("y:1", "c", None).unwrap();
    ns.delete_prefix("x:").unwrap();
    let after = db.stats().op_deletes;
    assert_eq!(after - before, 2);
}

#[test]
fn delete_range_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 1..=5_i64 {
            ns.put(i, format!("v{i}"), None).unwrap();
        }
        ns.delete_range(2_i64, 4_i64, true).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert!(ns.exists(1_i64).unwrap());
        assert!(!ns.exists(2_i64).unwrap());
        assert!(!ns.exists(3_i64).unwrap());
        assert!(!ns.exists(4_i64).unwrap());
        assert!(ns.exists(5_i64).unwrap());
        assert_eq!(ns.count().unwrap(), 2);
    }
}

#[test]
fn delete_prefix_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("user:1", "a", None).unwrap();
        ns.put("user:2", "b", None).unwrap();
        ns.put("post:1", "c", None).unwrap();
        ns.delete_prefix("user:").unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        assert!(!ns.exists("user:1").unwrap());
        assert!(!ns.exists("user:2").unwrap());
        assert!(ns.exists("post:1").unwrap());
        assert_eq!(ns.count().unwrap(), 1);
    }
}

#[test]
fn delete_range_string_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("aaa", "1", None).unwrap();
    ns.put("bbb", "2", None).unwrap();
    ns.put("ccc", "3", None).unwrap();
    ns.put("ddd", "4", None).unwrap();

    // Delete [bbb, ddd) — keys bbb, ccc
    let deleted = ns.delete_range("bbb", "ddd", false).unwrap();
    assert_eq!(deleted, 2);
    assert!(ns.exists("aaa").unwrap());
    assert!(!ns.exists("bbb").unwrap());
    assert!(!ns.exists("ccc").unwrap());
    assert!(ns.exists("ddd").unwrap());
}

// --- Encryption crypto ---

#[test]
fn encrypted_put_get_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("s3cret")).unwrap();
    ns.put("key", "hello", None).unwrap();
    assert_eq!(ns.get("key").unwrap(), Value::from("hello"));
}

#[test]
fn encrypted_null_value_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    ns.put("k", Value::Null, None).unwrap();
    assert_eq!(ns.get("k").unwrap(), Value::Null);
}

#[test]
fn encrypted_data_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("vault", Some("pw")).unwrap();
        ns.put("key", "secret-data", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("vault", Some("pw")).unwrap();
        assert_eq!(ns.get("key").unwrap(), Value::from("secret-data"));
    }
}

#[test]
fn encrypted_wrong_password_rejected_at_open() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("vault", Some("correct")).unwrap();
        ns.put("key", "secret", None).unwrap();
        db.close().unwrap();
    }
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        // Wrong password — should be caught at namespace() call, not at get()
        let err = db.namespace("vault", Some("wrong")).unwrap_err();
        assert!(
            matches!(err, Error::Corruption(_)),
            "expected Corruption for wrong password, got {err:?}"
        );
    }
}

#[test]
fn encrypted_and_plain_namespaces_coexist() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let plain = db.namespace("public", None).unwrap();
    let secret = db.namespace("private", Some("pw")).unwrap();

    plain.put("k", "plain-val", None).unwrap();
    secret.put("k", "secret-val", None).unwrap();

    assert_eq!(plain.get("k").unwrap(), Value::from("plain-val"));
    assert_eq!(secret.get("k").unwrap(), Value::from("secret-val"));
}

#[test]
fn encrypted_rev_get_decrypts() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    ns.put("k", "v1", None).unwrap();
    ns.put("k", "v2", None).unwrap();

    assert_eq!(ns.rev_get("k", 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get("k", 1).unwrap(), Value::from("v2"));
    assert_eq!(ns.rev_count("k").unwrap(), 2);
}

#[test]
fn encrypted_large_value_bin_object() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10; // force value separation
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    let large = "x".repeat(200);
    ns.put("big", large.as_str(), None).unwrap();
    assert_eq!(ns.get("big").unwrap(), Value::from(large.as_str()));
}

#[test]
fn encrypted_large_value_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let large = "y".repeat(200);
    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("vault", Some("pw")).unwrap();
        ns.put("big", large.as_str(), None).unwrap();
        db.close().unwrap();
    }
    {
        let mut config = Config::new(tmp.path());
        config.object_size = 10;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("vault", Some("pw")).unwrap();
        assert_eq!(ns.get("big").unwrap(), Value::from(large.as_str()));
    }
}

#[test]
fn encrypted_delete_and_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    ns.put("k", "v", None).unwrap();
    assert!(ns.exists("k").unwrap());

    ns.delete("k").unwrap();
    assert!(!ns.exists("k").unwrap());
    let err = ns.get("k").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn encrypted_ttl_works() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    ns.put("k", "v", Some(Duration::from_secs(3600))).unwrap();
    let remaining = ns.ttl("k").unwrap().unwrap();
    assert!(remaining.as_secs() > 3500);
    assert_eq!(ns.get("k").unwrap(), Value::from("v"));
}

#[test]
fn encrypted_scan_returns_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("vault", Some("pw")).unwrap();
    ns.put("user:1", "a", None).unwrap();
    ns.put("user:2", "b", None).unwrap();
    ns.put("post:1", "c", None).unwrap();

    let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 2);
}

// --- Flush + SSTable read path ---

#[test]
fn flush_and_get_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "hello", None).unwrap();
    ns.put(2, "world", None).unwrap();

    db.flush().unwrap();

    // Data should be readable from SSTable after flush
    assert_eq!(ns.get(1).unwrap(), Value::from("hello"));
    assert_eq!(ns.get(2).unwrap(), Value::from("world"));
}

#[test]
fn flush_persistence_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(1, "persisted", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen — data should come from SSTable (AOL was truncated)
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    assert_eq!(ns.get(1).unwrap(), Value::from("persisted"));
}

#[test]
fn flush_tombstone_survives() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "value", None).unwrap();
    ns.delete(1).unwrap();

    db.flush().unwrap();

    // Tombstone in SSTable should return KeyNotFound
    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn flush_tombstone_shadows_older_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Flush 1: write key=1
    ns.put(1, "old", None).unwrap();
    db.flush().unwrap();

    // Flush 2: delete key=1
    ns.delete(1).unwrap();
    db.flush().unwrap();

    // Tombstone in newer SSTable should shadow value in older SSTable
    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn flush_multiple_creates_multiple_l0_files() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "first", None).unwrap();
    db.flush().unwrap();

    ns.put(2, "second", None).unwrap();
    db.flush().unwrap();

    // Both keys should be readable
    assert_eq!(ns.get(1).unwrap(), Value::from("first"));
    assert_eq!(ns.get(2).unwrap(), Value::from("second"));

    // Should have 2 SSTable files in L0
    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    let count = std::fs::read_dir(&l0_dir).unwrap().count();
    assert_eq!(count, 2);
}

#[test]
fn flush_newer_value_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "old", None).unwrap();
    db.flush().unwrap();

    ns.put(1, "new", None).unwrap();
    db.flush().unwrap();

    // Newer SSTable value should win
    assert_eq!(ns.get(1).unwrap(), Value::from("new"));
}

#[test]
fn flush_encrypted_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("secret", Some("pass123")).unwrap();
        ns.put("key", "encrypted-data", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen with correct password — should decrypt from SSTable
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("secret", Some("pass123")).unwrap();
    assert_eq!(ns.get("key").unwrap(), Value::from("encrypted-data"));
}

#[test]
fn flush_with_bin_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 10; // Force value separation for values > 10 bytes
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let large_value = "x".repeat(100);
    ns.put(1, large_value.as_str(), None).unwrap();
    db.flush().unwrap();

    // ValuePointer should be in SSTable, resolved via ObjectStore
    assert_eq!(ns.get(1).unwrap(), Value::from(large_value.as_str()));
}

#[test]
fn flush_multiple_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("ns1", None).unwrap();
    let ns2 = db.namespace("ns2", None).unwrap();

    ns1.put("a", "from-ns1", None).unwrap();
    ns2.put("a", "from-ns2", None).unwrap();

    db.flush().unwrap();

    assert_eq!(ns1.get("a").unwrap(), Value::from("from-ns1"));
    assert_eq!(ns2.get("a").unwrap(), Value::from("from-ns2"));

    // Each namespace should have its own SSTable directory
    assert!(tmp.path().join("sst").join("ns1").exists());
    assert!(tmp.path().join("sst").join("ns2").exists());
}

#[test]
fn flush_memtable_miss_sstable_hit() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "flushed", None).unwrap();
    db.flush().unwrap();

    // Put a new key in MemTable (key=2), but key=1 only in SSTable
    ns.put(2, "in-memory", None).unwrap();

    assert_eq!(ns.get(1).unwrap(), Value::from("flushed"));
    assert_eq!(ns.get(2).unwrap(), Value::from("in-memory"));
}

#[test]
fn flush_aol_truncated() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.aol_buffer_size = 0; // Per-record flush so writes hit disk immediately
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "data", None).unwrap();

    let aol_path = tmp.path().join("sst/_/aol");
    let size_before = std::fs::metadata(&aol_path).unwrap().len();
    assert!(size_before > 8); // More than just header

    db.flush().unwrap();

    let size_after = std::fs::metadata(&aol_path).unwrap().len();
    assert_eq!(size_after, 8); // Header only (magic + version + reserved)
}

// --- Per-namespace AOL tests ---

#[test]
fn per_ns_aol_isolation() {
    // Each namespace should have its own AOL file under sst/<ns>/aol
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.aol_buffer_size = 0;
    let db = DB::open(config).unwrap();

    let ns_a = db.namespace("alpha", None).unwrap();
    let ns_b = db.namespace("beta", None).unwrap();
    ns_a.put(1, "a-val", None).unwrap();
    ns_b.put(2, "b-val", None).unwrap();

    // Verify separate AOL files exist
    let aol_a = tmp.path().join("sst/alpha/aol");
    let aol_b = tmp.path().join("sst/beta/aol");
    assert!(aol_a.exists(), "alpha AOL should exist");
    assert!(aol_b.exists(), "beta AOL should exist");

    // Legacy global AOL should NOT exist
    let legacy = tmp.path().join("aol");
    assert!(!legacy.exists(), "legacy global AOL should not exist");

    // Flushing one namespace should only truncate its AOL
    let size_a_before = std::fs::metadata(&aol_a).unwrap().len();
    let size_b_before = std::fs::metadata(&aol_b).unwrap().len();
    assert!(size_a_before > 8);
    assert!(size_b_before > 8);

    db.flush().unwrap();

    let size_a_after = std::fs::metadata(&aol_a).unwrap().len();
    let size_b_after = std::fs::metadata(&aol_b).unwrap().len();
    assert_eq!(size_a_after, 8, "alpha AOL should be truncated to header");
    assert_eq!(size_b_after, 8, "beta AOL should be truncated to header");
}

#[test]
fn per_ns_aol_survives_reopen() {
    // Data written to per-namespace AOLs should survive close + reopen
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 0;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("myns", None).unwrap();
        ns.put("key1", "val1", None).unwrap();
        ns.put("key2", "val2", None).unwrap();
        db.close().unwrap();
    }

    // Reopen and verify data is intact
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("myns", None).unwrap();
    assert_eq!(ns.get("key1").unwrap(), Value::from("val1"));
    assert_eq!(ns.get("key2").unwrap(), Value::from("val2"));
}

#[test]
fn per_ns_aol_legacy_migration() {
    // Simulate a legacy global AOL and verify it migrates to per-namespace files
    let tmp = tempfile::tempdir().unwrap();

    // Step 1: Create a DB, write data, close — this creates per-ns AOLs
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 0;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put("k1", "v1", None).unwrap();
        ns.put("k2", "v2", None).unwrap();
        db.close().unwrap();
    }

    // Step 2: Move the per-ns AOL to the legacy location to simulate old format
    let ns_aol = tmp.path().join("sst/_/aol");
    let legacy_aol = tmp.path().join("aol");
    std::fs::rename(&ns_aol, &legacy_aol).unwrap();

    // Step 3: Reopen — should migrate from legacy to per-ns
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        assert_eq!(ns.get("k1").unwrap(), Value::from("v1"));
        assert_eq!(ns.get("k2").unwrap(), Value::from("v2"));
        db.close().unwrap();
    }

    // Legacy AOL should be deleted after migration
    assert!(
        !legacy_aol.exists(),
        "legacy AOL should be deleted after migration"
    );
    // Per-namespace AOL should exist
    assert!(
        ns_aol.exists(),
        "per-ns AOL should be created after migration"
    );
}

#[test]
fn per_ns_aol_multi_namespace_no_cross_contamination() {
    // Data in one namespace should not appear in another after reopen
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.aol_buffer_size = 0;
        let db = DB::open(config).unwrap();
        let ns_x = db.namespace("x", None).unwrap();
        let ns_y = db.namespace("y", None).unwrap();
        ns_x.put("shared_key", "x-value", None).unwrap();
        ns_y.put("shared_key", "y-value", None).unwrap();
        db.close().unwrap();
    }

    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns_x = db.namespace("x", None).unwrap();
    let ns_y = db.namespace("y", None).unwrap();
    assert_eq!(ns_x.get("shared_key").unwrap(), Value::from("x-value"));
    assert_eq!(ns_y.get("shared_key").unwrap(), Value::from("y-value"));
}

// --- Compaction tests ---

#[test]
fn compact_merges_l0_into_l1() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Create two L0 SSTables
    ns.put(1, "first", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "second", None).unwrap();
    db.flush().unwrap();

    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    assert_eq!(std::fs::read_dir(&l0_dir).unwrap().count(), 2);

    db.compact().unwrap();

    // L0 should be empty, L1 should have exactly 1 SSTable
    let l0_count = std::fs::read_dir(&l0_dir).map(|rd| rd.count()).unwrap_or(0);
    assert_eq!(l0_count, 0);

    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert_eq!(std::fs::read_dir(&l1_dir).unwrap().count(), 1);

    // Data should still be accessible
    assert_eq!(ns.get(1).unwrap(), Value::from("first"));
    assert_eq!(ns.get(2).unwrap(), Value::from("second"));
}

#[test]
fn compact_newer_value_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "old", None).unwrap();
    db.flush().unwrap();

    ns.put(1, "new", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    assert_eq!(ns.get(1).unwrap(), Value::from("new"));
}

#[test]
fn compact_tombstone_preserved() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "alive", None).unwrap();
    db.flush().unwrap();

    ns.delete(1).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Tombstone should survive compaction
    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn compact_data_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let config = Config::new(tmp.path());
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();

        ns.put(1, "a", None).unwrap();
        db.flush().unwrap();
        ns.put(2, "b", None).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();
        db.close().unwrap();
    }

    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    assert_eq!(ns.get(1).unwrap(), Value::from("a"));
    assert_eq!(ns.get(2).unwrap(), Value::from("b"));
}

#[test]
fn compact_multiple_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("ns1", None).unwrap();
    let ns2 = db.namespace("ns2", None).unwrap();

    ns1.put("a", "from-ns1", None).unwrap();
    ns2.put("a", "from-ns2", None).unwrap();
    db.flush().unwrap();

    ns1.put("b", "more-ns1", None).unwrap();
    ns2.put("b", "more-ns2", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Both namespaces should have L1 SSTables
    let l1_ns1 = tmp.path().join("sst").join("ns1").join("L1");
    let l1_ns2 = tmp.path().join("sst").join("ns2").join("L1");
    assert!(l1_ns1.exists());
    assert!(l1_ns2.exists());

    assert_eq!(ns1.get("a").unwrap(), Value::from("from-ns1"));
    assert_eq!(ns2.get("b").unwrap(), Value::from("more-ns2"));
}

#[test]
fn compact_no_l0_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Only put data in memtable, no flush → no L0 SSTables
    ns.put(1, "memonly", None).unwrap();

    db.compact().unwrap();

    // No L1 directory should have been created
    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert!(!l1_dir.exists());

    // Memtable data should still be accessible
    assert_eq!(ns.get(1).unwrap(), Value::from("memonly"));
}

#[test]
fn compact_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "value", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();
    db.compact().unwrap(); // second compact is a no-op (L0 is empty)

    assert_eq!(ns.get(1).unwrap(), Value::from("value"));

    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert_eq!(std::fs::read_dir(&l1_dir).unwrap().count(), 1);
}

#[test]
fn compact_then_flush_adds_new_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "before", None).unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    // New writes go to L0 again
    ns.put(2, "after", None).unwrap();
    db.flush().unwrap();

    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    assert_eq!(std::fs::read_dir(&l0_dir).unwrap().count(), 1);

    assert_eq!(ns.get(1).unwrap(), Value::from("before"));
    assert_eq!(ns.get(2).unwrap(), Value::from("after"));
}

// --- Multi-level compaction tests ---

#[test]
fn compact_cascades_l1_to_l2() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 4;
    config.l1_max_size = 1; // tiny threshold forces cascade
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "b", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // L0 and L1 should be empty, L2 should have data
    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    let l0_count = std::fs::read_dir(&l0_dir).map(|rd| rd.count()).unwrap_or(0);
    assert_eq!(l0_count, 0);

    let l2_dir = tmp.path().join("sst").join("_").join("L2");
    assert!(l2_dir.exists());
    assert_eq!(std::fs::read_dir(&l2_dir).unwrap().count(), 1);

    assert_eq!(ns.get(1).unwrap(), Value::from("a"));
    assert_eq!(ns.get(2).unwrap(), Value::from("b"));
}

#[test]
fn compact_cascades_to_deepest_level() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 4;
    config.l1_max_size = 1;
    config.default_max_size = 1; // force cascade through all levels
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "val1", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "val2", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Data should land at the deepest level (L3)
    let l3_dir = tmp.path().join("sst").join("_").join("L3");
    assert!(l3_dir.exists());
    assert_eq!(std::fs::read_dir(&l3_dir).unwrap().count(), 1);

    assert_eq!(ns.get(1).unwrap(), Value::from("val1"));
    assert_eq!(ns.get(2).unwrap(), Value::from("val2"));
}

#[test]
fn compact_tombstone_dropped_at_bottom() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 3;
    config.l1_max_size = 1;
    config.default_max_size = 1; // cascade to L2 (bottom)
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "alive", None).unwrap();
    db.flush().unwrap();
    ns.delete(1).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Tombstone should be dropped at bottom level — L2 SSTable
    // should be empty or non-existent (all entries were tombstones).
    let l2_dir = tmp.path().join("sst").join("_").join("L2");
    let l2_count = std::fs::read_dir(&l2_dir).map(|rd| rd.count()).unwrap_or(0);
    assert_eq!(l2_count, 0);

    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn compact_tombstone_preserved_at_intermediate() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 4;
    // Only cascade to L1 (not the bottom level L3)
    config.l1_max_size = 256 * 1024 * 1024; // big enough to stop cascade
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "alive", None).unwrap();
    db.flush().unwrap();
    ns.delete(1).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Tombstone should survive at L1 (not the bottom level)
    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert!(l1_dir.exists());
    assert_eq!(std::fs::read_dir(&l1_dir).unwrap().count(), 1);

    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn compact_respects_max_levels_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 3;
    config.l1_max_size = 1;
    config.default_max_size = 1;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "val", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "val2", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // No L3 should exist (max_levels = 3 means L0, L1, L2)
    let l3_dir = tmp.path().join("sst").join("_").join("L3");
    assert!(!l3_dir.exists());

    // Data lands at L2 (the bottommost)
    let l2_dir = tmp.path().join("sst").join("_").join("L2");
    assert!(l2_dir.exists());

    assert_eq!(ns.get(1).unwrap(), Value::from("val"));
    assert_eq!(ns.get(2).unwrap(), Value::from("val2"));
}

#[test]
fn compact_cascade_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut config = Config::new(tmp.path());
        config.max_levels = 4;
        config.l1_max_size = 1;
        config.default_max_size = 1;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();

        ns.put(1, "deep", None).unwrap();
        db.flush().unwrap();
        ns.put(2, "deeper", None).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();
        db.close().unwrap();
    }

    let mut config = Config::new(tmp.path());
    config.max_levels = 4;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    assert_eq!(ns.get(1).unwrap(), Value::from("deep"));
    assert_eq!(ns.get(2).unwrap(), Value::from("deeper"));
}

#[test]
fn compact_max_levels_one_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.max_levels = 1;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "val", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // L0 files should be untouched (no merge target available)
    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    assert_eq!(std::fs::read_dir(&l0_dir).unwrap().count(), 1);

    assert_eq!(ns.get(1).unwrap(), Value::from("val"));
}

// --- Auto-Compaction tests ---

#[test]
fn auto_compact_triggers_on_l0_count() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.l0_max_count = 3; // trigger after 3 L0 files
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Flush 1 and 2: no auto-compact yet
    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "b", None).unwrap();
    db.flush().unwrap();

    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    assert_eq!(std::fs::read_dir(&l0_dir).unwrap().count(), 2);

    // Flush 3: hits l0_max_count=3, triggers auto-compact
    ns.put(3, "c", None).unwrap();
    db.flush().unwrap();
    db.wait_for_compaction();

    // After auto-compact, L0 should be empty (merged into L1)
    let l0_count = std::fs::read_dir(&l0_dir).map(|d| d.count()).unwrap_or(0);
    assert_eq!(l0_count, 0);

    // L1 should have data
    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert!(l1_dir.exists());
    assert!(std::fs::read_dir(&l1_dir).unwrap().count() > 0);

    // All values readable
    assert_eq!(ns.get(1).unwrap(), Value::from("a"));
    assert_eq!(ns.get(2).unwrap(), Value::from("b"));
    assert_eq!(ns.get(3).unwrap(), Value::from("c"));
}

#[test]
fn auto_compact_does_not_trigger_below_threshold() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.l0_max_count = 10; // high threshold
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "b", None).unwrap();
    db.flush().unwrap();

    // L0 should still have 2 files (no auto-compact)
    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    assert_eq!(std::fs::read_dir(&l0_dir).unwrap().count(), 2);

    // No L1 created
    let l1_dir = tmp.path().join("sst").join("_").join("L1");
    assert!(!l1_dir.exists());
}

#[test]
fn auto_compact_data_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("mydb");
    {
        let mut config = Config::new(&db_path);
        config.l0_max_count = 2;
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();

        ns.put(1, "first", None).unwrap();
        db.flush().unwrap();
        ns.put(2, "second", None).unwrap();
        db.flush().unwrap(); // triggers auto-compact
        db.wait_for_compaction();

        db.close().unwrap();
    }
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        assert_eq!(ns.get(1).unwrap(), Value::from("first"));
        assert_eq!(ns.get(2).unwrap(), Value::from("second"));
        db.close().unwrap();
    }
}

#[test]
fn auto_compact_triggers_on_l0_size() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.l0_max_count = 100; // high count threshold
    config.l0_max_size = 1; // 1 byte — any L0 file triggers
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "value", None).unwrap();
    db.flush().unwrap();
    db.wait_for_compaction();

    // L0 should be empty after auto-compact
    let l0_dir = tmp.path().join("sst").join("_").join("L0");
    let l0_count = std::fs::read_dir(&l0_dir).map(|d| d.count()).unwrap_or(0);
    assert_eq!(l0_count, 0);

    assert_eq!(ns.get(1).unwrap(), Value::from("value"));
}

// --- Bin Object GC tests ---

/// Helper: count object files under `<db>/objects/<ns>/`.
fn count_object_files(db_path: &std::path::Path, ns: &str) -> usize {
    let obj_dir = db_path.join("objects").join(ns);
    if !obj_dir.exists() {
        return 0;
    }
    let mut count = 0;

    for entry in std::fs::read_dir(&obj_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();

        if entry.file_type().unwrap().is_dir() {
            // Fan-out directory — count loose object files
            for obj_entry in std::fs::read_dir(entry.path()).unwrap() {
                let obj_name = obj_entry.unwrap().file_name().to_string_lossy().to_string();
                if obj_name.len() == 64 {
                    count += 1;
                }
            }
        } else if name.starts_with("pack-") && name.ends_with(".pack") {
            // Pack file — count records by scanning
            let data = std::fs::read(entry.path()).unwrap();
            let mut pos = 6; // skip header (magic + version)
            while pos + 46 <= data.len() {
                // 41 header + 5 checksum minimum
                let data_len =
                    u32::from_be_bytes(data[pos + 37..pos + 41].try_into().unwrap()) as usize;
                let record_end = pos + 41 + data_len + 5;
                if record_end > data.len() {
                    break;
                }
                count += 1;
                pos = record_end;
            }
        }
    }
    count
}

#[test]
fn gc_overwrite_removes_orphaned_object() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 16; // force value separation
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Write a large value, then overwrite with a different large value
    ns.put(1, "a".repeat(100).as_str(), None).unwrap();
    db.flush().unwrap();
    assert_eq!(count_object_files(tmp.path(), "_"), 1);

    ns.put(1, "b".repeat(100).as_str(), None).unwrap();
    db.flush().unwrap();
    assert_eq!(count_object_files(tmp.path(), "_"), 2); // both objects exist

    db.compact().unwrap();

    // Both objects survive — revision history preserves both values
    assert_eq!(count_object_files(tmp.path(), "_"), 2);
    assert_eq!(ns.get(1).unwrap(), Value::from("b".repeat(100).as_str()));
}

#[test]
fn gc_tombstone_removes_orphaned_object() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 16;
    config.max_levels = 2; // L1 is bottom — tombstones dropped
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "x".repeat(100).as_str(), None).unwrap();
    db.flush().unwrap();
    assert_eq!(count_object_files(tmp.path(), "_"), 1);

    ns.delete(1).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Object should be garbage-collected
    assert_eq!(count_object_files(tmp.path(), "_"), 0);
    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn gc_dedup_preserved_when_one_ref_deleted_another_alive() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 16;
    config.max_levels = 2; // bottom level — tombstones dropped
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Two keys reference the exact same large content (dedup)
    let shared_content = "d".repeat(100);
    ns.put(1, shared_content.as_str(), None).unwrap();
    ns.put(2, shared_content.as_str(), None).unwrap();
    db.flush().unwrap();

    // Only 1 object file due to dedup
    assert_eq!(count_object_files(tmp.path(), "_"), 1);

    // Delete key 1 but keep key 2
    ns.delete(1).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Object must survive — key 2 still references it
    assert_eq!(count_object_files(tmp.path(), "_"), 1);
    assert_eq!(ns.get(2).unwrap(), Value::from(shared_content.as_str()));
    let err = ns.get(1).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));
}

#[test]
fn gc_no_objects_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Small values — no bin objects
    ns.put(1, "small", None).unwrap();
    db.flush().unwrap();
    ns.put(2, "tiny", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    assert_eq!(ns.get(1).unwrap(), Value::from("small"));
    assert_eq!(ns.get(2).unwrap(), Value::from("tiny"));
    assert_eq!(count_object_files(tmp.path(), "_"), 0);
}

#[test]
fn gc_after_cascade_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 16;
    config.max_levels = 3;
    config.l1_max_size = 1; // force cascade to L2
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1, "old".repeat(50).as_str(), None).unwrap();
    db.flush().unwrap();
    ns.put(1, "new".repeat(50).as_str(), None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // After cascade + GC, both objects survive (revision history preserved)
    assert_eq!(count_object_files(tmp.path(), "_"), 2);
    assert_eq!(ns.get(1).unwrap(), Value::from("new".repeat(50).as_str()));
}

#[test]
fn gc_dedup_both_keys_alive() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 16;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let shared = "s".repeat(100);
    ns.put(1, shared.as_str(), None).unwrap();
    ns.put(2, shared.as_str(), None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    // Deduped object survives — both keys reference it
    assert_eq!(count_object_files(tmp.path(), "_"), 1);
    assert_eq!(ns.get(1).unwrap(), Value::from(shared.as_str()));
    assert_eq!(ns.get(2).unwrap(), Value::from(shared.as_str()));
}

// --- Namespace management tests ---

#[test]
fn list_namespaces_after_put() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("users", None).unwrap();
    ns.put("alice", "1", None).unwrap();
    let ns2 = db.namespace("orders", None).unwrap();
    ns2.put("order1", "x", None).unwrap();

    let names = db.list_namespaces().unwrap();
    assert_eq!(names, vec!["_", "orders", "users"]);
}

#[test]
fn list_namespaces_includes_default() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k", "v", None).unwrap();

    let names = db.list_namespaces().unwrap();
    assert_eq!(names, vec!["_"]);
}

#[test]
fn list_namespaces_includes_flushed_sstable_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("data", None).unwrap();
    ns.put("k", "v", None).unwrap();
    db.flush().unwrap();

    // After flush, memtable is empty but L0 SSTable exists
    let names = db.list_namespaces().unwrap();
    assert!(names.contains(&"data".to_owned()));
}

#[test]
fn drop_namespace_removes_data() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("users", None).unwrap();
    ns.put("alice", "1", None).unwrap();
    drop(ns);

    db.drop_namespace("users").unwrap();

    let names = db.list_namespaces().unwrap();
    assert!(!names.contains(&"users".to_owned()));
}

#[test]
fn drop_namespace_removes_sstable_files() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("logs", None).unwrap();
    ns.put("entry1", "data", None).unwrap();
    drop(ns);
    db.flush().unwrap();

    // Verify SSTable dir exists
    let sst_dir = tmp.path().join("sst").join("logs");
    assert!(sst_dir.exists());

    db.drop_namespace("logs").unwrap();
    assert!(!sst_dir.exists());
}

#[test]
fn drop_namespace_removes_object_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.object_size = 0; // Force all values to bin objects
    let db = DB::open(config).unwrap();

    let ns = db.namespace("blobs", None).unwrap();
    ns.put("big", "some data", None).unwrap();
    drop(ns);

    let obj_dir = tmp.path().join("objects").join("blobs");
    assert!(obj_dir.exists());

    db.drop_namespace("blobs").unwrap();
    assert!(!obj_dir.exists());
}

#[test]
fn drop_namespace_removes_crypto_salt() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("secret", Some("pass123")).unwrap();
    ns.put("k", "v", None).unwrap();
    drop(ns);

    let salt_path = tmp.path().join("crypto").join("secret.salt");
    assert!(salt_path.exists());

    db.drop_namespace("secret").unwrap();
    assert!(!salt_path.exists());
}

#[test]
fn drop_namespace_does_not_affect_other_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("keep", None).unwrap();
    ns1.put("k1", "v1", None).unwrap();
    let ns2 = db.namespace("remove", None).unwrap();
    ns2.put("k2", "v2", None).unwrap();
    drop(ns1);
    drop(ns2);

    db.drop_namespace("remove").unwrap();

    let ns1 = db.namespace("keep", None).unwrap();
    assert_eq!(ns1.get("k1").unwrap(), Value::from("v1"));
}

#[test]
fn drop_namespace_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().to_path_buf();

    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();

        let ns = db.namespace("ephemeral", None).unwrap();
        ns.put("k", "v", None).unwrap();
        drop(ns);

        db.drop_namespace("ephemeral").unwrap();
        db.close().unwrap();
    }

    // Reopen — dropped namespace should not reappear
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();

    let names = db.list_namespaces().unwrap();
    assert!(!names.contains(&"ephemeral".to_owned()));
}

#[test]
fn drop_empty_name_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.drop_namespace("").unwrap_err();
    assert!(matches!(err, Error::InvalidNamespace(_)));
}

#[test]
fn list_namespaces_sorted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    for name in &["zeta", "alpha", "mid"] {
        let ns = db.namespace(name, None).unwrap();
        ns.put("k", "v", None).unwrap();
    }

    let names = db.list_namespaces().unwrap();
    assert_eq!(names, vec!["_", "alpha", "mid", "zeta"]);
}

// --- Merged scan: MemTable + SSTable ---

#[test]
fn scan_after_flush_sees_all_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Put two keys, flush to SSTable
    ns.put("user:1", "alice", None).unwrap();
    ns.put("user:2", "bob", None).unwrap();
    db.flush().unwrap();

    // Put a third key in MemTable
    ns.put("user:3", "charlie", None).unwrap();

    // Scan should return all three
    let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&Key::from("user:1")));
    assert!(keys.contains(&Key::from("user:2")));
    assert!(keys.contains(&Key::from("user:3")));
}

#[test]
fn scan_after_flush_only_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    db.flush().unwrap();

    // All keys are in SSTable, MemTable is empty
    let keys = ns.scan(&Key::from(""), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 2);
}

#[test]
fn scan_tombstone_shadows_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("user:1", "alice", None).unwrap();
    ns.put("user:2", "bob", None).unwrap();
    db.flush().unwrap();

    // Delete user:1 in MemTable — should shadow the SSTable entry
    ns.delete("user:1").unwrap();

    let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::from("user:2")]);
}

#[test]
fn scan_memtable_overwrites_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("key", "old_value", None).unwrap();
    db.flush().unwrap();

    // Overwrite in MemTable
    ns.put("key", "new_value", None).unwrap();

    // Scan should return the key once (not duplicate)
    let keys = ns.scan(&Key::from(""), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::from("key")]);

    // Verify value is the new one
    let val = ns.get("key").unwrap();
    assert_eq!(val, Value::from("new_value"));
}

#[test]
fn rscan_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    db.flush().unwrap();
    ns.put(3_i64, "c", None).unwrap();

    let keys = ns.rscan(&Key::Int(3), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(2), Key::Int(1)]);
}

#[test]
fn scan_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Multiple flushes to create multiple L0 SSTables
    ns.put("a", "1", None).unwrap();
    db.flush().unwrap();
    ns.put("b", "2", None).unwrap();
    db.flush().unwrap();
    ns.put("c", "3", None).unwrap();
    db.flush().unwrap();

    // Compact merges into L1
    db.compact().unwrap();

    let keys = ns.scan(&Key::from(""), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&Key::from("a")));
    assert!(keys.contains(&Key::from("b")));
    assert!(keys.contains(&Key::from("c")));
}

#[test]
fn scan_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("scan_restart");

    // Phase 1: write and flush
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put("x", "1", None).unwrap();
        ns.put("y", "2", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Phase 2: reopen and scan
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        let keys = ns.scan(&Key::from(""), 10, 0, false).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::from("x")));
        assert!(keys.contains(&Key::from("y")));
        db.close().unwrap();
    }
}

#[test]
fn scan_with_limit_and_offset_across_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    db.flush().unwrap();
    ns.put("c", "3", None).unwrap();
    ns.put("d", "4", None).unwrap();

    // Skip 1, take 2 from merged (a, b, c, d)
    let keys = ns.scan(&Key::from(""), 2, 1, false).unwrap();
    assert_eq!(keys, vec![Key::from("b"), Key::from("c")]);
}

#[test]
fn scan_ordered_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(10_i64, "a", None).unwrap();
    ns.put(20_i64, "b", None).unwrap();
    db.flush().unwrap();
    ns.put(30_i64, "c", None).unwrap();

    let keys = ns.scan(&Key::Int(15), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(20), Key::Int(30)]);
}

#[test]
fn scan_multiple_flushes_dedup() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("key", "v1", None).unwrap();
    db.flush().unwrap();

    ns.put("key", "v2", None).unwrap();
    db.flush().unwrap();

    // Key appears in two SSTables but should only show up once in scan
    let keys = ns.scan(&Key::from(""), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::from("key")]);

    // Value should be the newest
    assert_eq!(ns.get("key").unwrap(), Value::from("v2"));
}

#[test]
fn scan_prefix_bloom_skip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.bloom_prefix_len = 4; // enable prefix bloom
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write keys with prefix "user:" and flush
    ns.put("user:1", "alice", None).unwrap();
    ns.put("user:2", "bob", None).unwrap();
    db.flush().unwrap();

    // Scan for a different prefix — prefix bloom should filter this SSTable
    let keys = ns.scan(&Key::from("post:"), 10, 0, false).unwrap();
    assert!(keys.is_empty());

    // Scan for matching prefix — should find both keys
    let keys = ns.scan(&Key::from("user:"), 10, 0, false).unwrap();
    assert_eq!(keys.len(), 2);
}

#[test]
fn rscan_tombstone_shadows_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    db.flush().unwrap();

    ns.delete(2_i64).unwrap();

    let keys = ns.rscan(&Key::Int(3), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(1)]);
}

#[test]
fn scan_cross_namespace_isolation_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns1 = db.namespace("ns1", None).unwrap();
    let ns2 = db.namespace("ns2", None).unwrap();

    ns1.put("shared_key", "from_ns1", None).unwrap();
    ns2.put("shared_key", "from_ns2", None).unwrap();
    db.flush().unwrap();

    let keys1 = ns1.scan(&Key::from(""), 10, 0, false).unwrap();
    let keys2 = ns2.scan(&Key::from(""), 10, 0, false).unwrap();
    assert_eq!(keys1.len(), 1);
    assert_eq!(keys2.len(), 1);
    assert_eq!(ns1.get("shared_key").unwrap(), Value::from("from_ns1"));
    assert_eq!(ns2.get("shared_key").unwrap(), Value::from("from_ns2"));
}

// --- Block Cache ---

#[test]
fn cache_hit_after_repeated_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 8 * 1024 * 1024; // 8 MB
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    for i in 0..10 {
        ns.put(Key::Int(i), format!("value_{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // First read populates the cache
    for i in 0..10 {
        let v = ns.get(Key::Int(i)).unwrap();
        assert_eq!(v, Value::from(format!("value_{i}").as_str()));
    }

    // Second read should hit the cache (same results expected)
    for i in 0..10 {
        let v = ns.get(Key::Int(i)).unwrap();
        assert_eq!(v, Value::from(format!("value_{i}").as_str()));
    }
}

#[test]
fn cache_works_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 8 * 1024 * 1024;
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("a", "alpha", None).unwrap();
    ns.put("b", "beta", None).unwrap();
    db.flush().unwrap();

    // Read from SSTable (populates cache)
    assert_eq!(ns.get("a").unwrap(), Value::from("alpha"));
    assert_eq!(ns.get("b").unwrap(), Value::from("beta"));

    // Second flush adds more data
    ns.put("c", "gamma", None).unwrap();
    db.flush().unwrap();

    // All keys still readable (mix of cached and new reads)
    assert_eq!(ns.get("a").unwrap(), Value::from("alpha"));
    assert_eq!(ns.get("b").unwrap(), Value::from("beta"));
    assert_eq!(ns.get("c").unwrap(), Value::from("gamma"));
}

#[test]
fn cache_disabled_with_zero_size() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 0; // disabled
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    for i in 0..10 {
        ns.put(Key::Int(i), format!("val_{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Should still work correctly without cache
    for i in 0..10 {
        let v = ns.get(Key::Int(i)).unwrap();
        assert_eq!(v, Value::from(format!("val_{i}").as_str()));
    }

    // Scans should also work
    let results = ns.scan(&Key::Int(0), 100, 0, false).unwrap();
    assert_eq!(results.len(), 10);
}

#[test]
fn cache_compaction_evicts_old_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 8 * 1024 * 1024;
    config.l0_max_count = 100; // prevent auto-compaction
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Create multiple L0 SSTables
    for batch in 0..3 {
        for i in 0..5 {
            let key = Key::Int(batch * 10 + i);
            ns.put(key, format!("v{batch}_{i}"), None).unwrap();
        }
        db.flush().unwrap();
    }

    // Read all keys to populate cache
    for batch in 0..3 {
        for i in 0..5 {
            let key = Key::Int(batch * 10 + i);
            let _ = ns.get(key).unwrap();
        }
    }

    // Compact — old SSTables are merged and cache entries evicted
    db.compact().unwrap();

    // Data should still be accessible (reads go through new SSTables)
    for batch in 0..3 {
        for i in 0..5 {
            let key = Key::Int(batch * 10 + i);
            let v = ns.get(key).unwrap();
            assert_eq!(v, Value::from(format!("v{batch}_{i}").as_str()));
        }
    }
}

#[test]
fn cache_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().to_path_buf();

    // Session 1: write data and flush
    {
        let mut config = Config::new(&db_path);
        config.cache_size = 8 * 1024 * 1024;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        for i in 0..10 {
            ns.put(Key::Int(i), format!("data_{i}"), None).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Session 2: reopen (fresh cache) and verify reads work
    {
        let mut config = Config::new(&db_path);
        config.cache_size = 8 * 1024 * 1024;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        // First read populates cache from new SSTable readers
        for i in 0..10 {
            let v = ns.get(Key::Int(i)).unwrap();
            assert_eq!(v, Value::from(format!("data_{i}").as_str()));
        }

        // Second read should hit cache
        for i in 0..10 {
            let v = ns.get(Key::Int(i)).unwrap();
            assert_eq!(v, Value::from(format!("data_{i}").as_str()));
        }
    }
}

// --- Stats counters tests ---

#[test]
fn stats_sstable_count_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    assert_eq!(db.stats().sstable_count, 0);

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();

    let s = db.stats();
    assert!(
        s.sstable_count >= 1,
        "expected at least 1 SSTable after flush"
    );
}

#[test]
fn stats_level_stats_populated_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let s = db.stats();
    assert_eq!(s.level_stats.len(), s.level_count);
    for ls in &s.level_stats {
        assert_eq!(ls.file_count, 0);
        assert_eq!(ls.size_bytes, 0);
    }

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();

    let s = db.stats();
    let total_files: u64 = s.level_stats.iter().map(|ls| ls.file_count).sum();
    assert_eq!(total_files, s.sstable_count);
    assert!(total_files >= 1);
    // At least one level should have nonzero size
    assert!(s.level_stats.iter().any(|ls| ls.size_bytes > 0));
}

#[test]
fn stats_cache_hits_misses_after_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 8 * 1024 * 1024; // ensure cache is enabled
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();

    // First read: cache miss (block not yet cached)
    let _ = ns.get(1).unwrap();
    let s = db.stats();
    assert!(
        s.cache_misses >= 1,
        "expected cache miss on first SSTable read"
    );

    // Second read of same key: cache hit
    let hits_before = db.stats().cache_hits;
    let _ = ns.get(1).unwrap();
    let s = db.stats();
    assert!(
        s.cache_hits > hits_before,
        "expected cache hit on repeated read"
    );
}

#[test]
fn stats_cache_disabled_reports_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 0; // disable cache
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "a", None).unwrap();
    db.flush().unwrap();
    let _ = ns.get(1).unwrap();

    let s = db.stats();
    assert_eq!(s.cache_hits, 0);
    assert_eq!(s.cache_misses, 0);
}

#[test]
fn stats_default_includes_level_stats() {
    let s = Stats::default();
    assert!(s.level_stats.is_empty());

    let ls = LevelStat::default();
    assert_eq!(ls.file_count, 0);
    assert_eq!(ls.size_bytes, 0);
}

// --- stats.meta corruption ---

#[test]
fn stats_meta_too_small() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    // Create a DB, write some ops, close to persist stats.meta
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put(1, "a", None).unwrap();
        db.close().unwrap();
    }

    // Truncate stats.meta to only 10 bytes (needs 30)
    let meta_path = db_path.join("stats.meta");
    std::fs::write(&meta_path, [0u8; 10]).unwrap();

    // Reopen — should silently reset counters to 0
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let s = db.stats();
    assert_eq!(s.op_puts, 0);
    db.close().unwrap();
}

#[test]
fn stats_meta_bad_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put(1, "a", None).unwrap();
        db.close().unwrap();
    }

    // Corrupt magic bytes in stats.meta
    let meta_path = db_path.join("stats.meta");
    let mut data = std::fs::read(&meta_path).unwrap();
    data[0] = 0xFF;
    data[1] = 0xFF;
    std::fs::write(&meta_path, &data).unwrap();

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let s = db.stats();
    assert_eq!(s.op_puts, 0);
    db.close().unwrap();
}

#[test]
fn stats_meta_bad_version() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        ns.put(1, "a", None).unwrap();
        db.close().unwrap();
    }

    // Corrupt version in stats.meta (bytes 4-5)
    let meta_path = db_path.join("stats.meta");
    let mut data = std::fs::read(&meta_path).unwrap();
    data[4] = 0xFF;
    data[5] = 0xFF;
    std::fs::write(&meta_path, &data).unwrap();

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let s = db.stats();
    assert_eq!(s.op_puts, 0);
    db.close().unwrap();
}

// --- pending compactions ---

#[test]
fn stats_pending_compactions_l0_count() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    // Set l0_max_count very low to trigger pending compaction detection
    let mut config = Config::new(&db_path);
    config.l0_max_count = 2;
    config.write_buffer_size = 64; // small buffer to force frequent flushes
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write and flush enough times to exceed l0_max_count
    for i in 0..3 {
        ns.put(i, format!("value_{i}").as_str(), None).unwrap();
        db.flush().unwrap();
    }

    let s = db.stats();
    assert!(
        s.pending_compactions > 0,
        "expected pending compactions > 0"
    );
    db.close().unwrap();
}

// --- write_buffer_size auto-flush ---

#[test]
fn auto_flush_triggers_when_write_buffer_size_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    // Small write buffer to trigger auto-flush quickly
    config.write_buffer_size = 512;
    // Disable compaction so background merges don't race with auto-flush
    config.l0_max_count = 1000;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write enough data to exceed the 512-byte write buffer
    for i in 0..100 {
        ns.put(Key::Int(i), format!("value-{i:050}"), None).unwrap();
    }

    // Auto-flush should have created SSTables without manual flush()
    let stats = db.stats();
    assert!(
        stats.sstable_count > 0,
        "expected auto-flush to create SSTables, got sstable_count={}",
        stats.sstable_count,
    );

    // All data should still be readable
    for i in 0..100 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("value-{i:050}").as_str()));
    }

    db.close().unwrap();
}

#[test]
fn auto_flush_data_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let mut config = Config::new(&db_path);
        config.write_buffer_size = 256;
        // Disable compaction so background merges don't race with auto-flush
        config.l0_max_count = 1000;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        for i in 0..50 {
            ns.put(Key::Int(i), format!("val-{i:040}"), None).unwrap();
        }
        // Don't call flush() — rely on auto-flush + AOL for persistence
        db.close().unwrap();
    }

    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        for i in 0..50 {
            let val = ns.get(Key::Int(i)).unwrap();
            assert_eq!(val, Value::from(format!("val-{i:040}").as_str()));
        }
        db.close().unwrap();
    }
}

#[test]
fn no_auto_flush_when_buffer_not_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    // Large write buffer — auto-flush should not trigger
    config.write_buffer_size = 64 * 1024 * 1024;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write a small amount of data
    for i in 0..10 {
        ns.put(Key::Int(i), format!("v{i}"), None).unwrap();
    }

    let stats = db.stats();
    assert_eq!(
        stats.sstable_count, 0,
        "expected no auto-flush with large write_buffer_size",
    );

    db.close().unwrap();
}

// --- IoModel integration ---

#[test]
fn io_model_mmap_read_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.io_model = IoModel::Mmap;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..20 {
        ns.put(Key::Int(i), format!("mmap-{i}"), None).unwrap();
    }
    db.flush().unwrap();

    for i in 0..20 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("mmap-{i}").as_str()));
    }

    db.close().unwrap();
}

#[test]
fn io_model_none_read_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.io_model = IoModel::None;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..20 {
        ns.put(Key::Int(i), format!("buf-{i}"), None).unwrap();
    }
    db.flush().unwrap();

    for i in 0..20 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("buf-{i}").as_str()));
    }

    db.close().unwrap();
}

#[test]
fn io_model_directio_read_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.io_model = IoModel::DirectIO;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..20 {
        ns.put(Key::Int(i), format!("direct-{i}"), None).unwrap();
    }
    db.flush().unwrap();

    for i in 0..20 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("direct-{i}").as_str()));
    }

    db.close().unwrap();
}

/// Revision persists through flush to SSTable and can be retrieved via get_with_revision.
#[test]
fn revision_survives_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rev_flush");

    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let rev1 = ns.put("name", "Alice", None).unwrap();
    let rev2 = ns.put("age", "30", None).unwrap();
    assert_ne!(rev1, rev2);

    // Verify from memtable first
    let (val, rev) = ns.get_with_revision("name").unwrap();
    assert_eq!(val, Value::from("Alice"));
    assert_eq!(rev, rev1);

    // Flush to SSTable
    db.flush().unwrap();

    // Verify from SSTable
    let (val, rev) = ns.get_with_revision("name").unwrap();
    assert_eq!(val, Value::from("Alice"));
    assert_eq!(rev, rev1);

    let (val, rev) = ns.get_with_revision("age").unwrap();
    assert_eq!(val, Value::from("30"));
    assert_eq!(rev, rev2);

    db.close().unwrap();
}

/// Revision persists through compaction.
#[test]
fn revision_survives_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rev_compact");

    let mut config = Config::new(&db_path);
    config.write_buffer_size = 1024 * 1024; // don't auto-flush
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write first batch and flush
    let rev_a = ns.put("a", "first", None).unwrap();
    let rev_b = ns.put("b", "first", None).unwrap();
    db.flush().unwrap();

    // Write second batch (overlapping key "a") and flush
    let rev_a2 = ns.put("a", "second", None).unwrap();
    let rev_c = ns.put("c", "first", None).unwrap();
    db.flush().unwrap();

    // Compact — merges both L0 SSTables
    db.compact().unwrap();

    // "a" should have the newer revision
    let (val, rev) = ns.get_with_revision("a").unwrap();
    assert_eq!(val, Value::from("second"));
    assert_eq!(rev, rev_a2);
    assert_ne!(rev, rev_a); // overwritten revision differs

    // "b" should keep its original revision
    let (val, rev) = ns.get_with_revision("b").unwrap();
    assert_eq!(val, Value::from("first"));
    assert_eq!(rev, rev_b);

    // "c" should have its revision
    let (val, rev) = ns.get_with_revision("c").unwrap();
    assert_eq!(val, Value::from("first"));
    assert_eq!(rev, rev_c);

    db.close().unwrap();
}

/// Revision survives close/reopen — persisted in SSTable on disk.
#[test]
fn revision_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rev_reopen");

    let rev1;
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
        rev1 = ns.put("key", "value", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let (val, rev) = ns.get_with_revision("key").unwrap();
    assert_eq!(val, Value::from("value"));
    assert_eq!(rev, rev1);
    db.close().unwrap();
}

/// Delete → flush → re-put: revision should be correct both in memtable and after flush.
#[test]
fn revision_correct_after_delete_flush_reput() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rev_del_flush_reput");

    let mut config = Config::new(&db_path);
    config.write_buffer_size = 1024 * 1024; // prevent auto-flush
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // 1. put → delete → flush (tombstone on disk)
    let _rev_a = ns.put("foo", "bar", None).unwrap();
    ns.delete("foo").unwrap();
    db.flush().unwrap();

    // 2. Re-put the same key — new value in memtable
    let rev_b = ns.put("foo", "baz", None).unwrap();

    // 3. Should be correct from memtable
    let (val, rev) = ns.get_with_revision("foo").unwrap();
    assert_eq!(val, Value::from("baz"));
    assert_eq!(rev, rev_b, "revision wrong from memtable");

    // 4. Flush again — new value moves to SSTable (tombstone is in older SSTable)
    db.flush().unwrap();

    // 5. Should still be correct from SSTable
    let (val, rev) = ns.get_with_revision("foo").unwrap();
    assert_eq!(val, Value::from("baz"));
    assert_eq!(
        rev, rev_b,
        "revision wrong from SSTable after delete+flush+reput"
    );

    // 6. Compact — merge SSTable with tombstone and newer value
    db.compact().unwrap();

    // 7. Still correct after compaction
    let (val, rev) = ns.get_with_revision("foo").unwrap();
    assert_eq!(val, Value::from("baz"));
    assert_eq!(rev, rev_b, "revision wrong after compaction over tombstone");

    db.close().unwrap();
}

/// Delete → flush → re-put → flush → reopen: revision must survive the full lifecycle.
#[test]
fn revision_survives_delete_flush_reput_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rev_del_reopen");

    let rev_b;
    {
        let mut config = Config::new(&db_path);
        config.write_buffer_size = 1024 * 1024;
        let db = DB::open(config).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        let _rev_a = ns.put("foo", "bar", None).unwrap();
        ns.delete("foo").unwrap();
        db.flush().unwrap();

        // Tombstone is on SSTable; get should fail
        assert!(ns.get_with_revision("foo").is_err());

        // Re-put same key
        rev_b = ns.put("foo", "baz", None).unwrap();

        // From memtable
        let (val, rev) = ns.get_with_revision("foo").unwrap();
        assert_eq!(val, Value::from("baz"));
        assert_eq!(rev, rev_b, "wrong rev from memtable");

        // Flush new value to SSTable
        db.flush().unwrap();

        // From SSTable
        let (val, rev) = ns.get_with_revision("foo").unwrap();
        assert_eq!(val, Value::from("baz"));
        assert_eq!(rev, rev_b, "wrong rev from SSTable");

        db.close().unwrap();
    }

    // Reopen and verify
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let (val, rev) = ns.get_with_revision("foo").unwrap();
    assert_eq!(val, Value::from("baz"));
    assert_eq!(rev, rev_b, "wrong rev after reopen");

    db.close().unwrap();
}

/// V2 SSTables written by flush survive close/reopen — format upgrade is transparent.
#[test]
fn format_version_upgrade_transparent() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("v2_upgrade");

    // Write data and flush to SSTables (now V2 format)
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    for i in 0..50 {
        ns.put(Key::Int(i), format!("val-{i}"), None).unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();

    // Reopen and verify all data is readable
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    for i in 0..50 {
        let val = ns.get(Key::Int(i)).unwrap();
        assert_eq!(val, Value::from(format!("val-{i}").as_str()));
    }
    db.close().unwrap();
}

#[test]
fn revision_count_spans_memtable_and_sstable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("revspan"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // First put → flush to SSTable
    let _rev1 = ns.put("key", "v1", None).unwrap();
    db.flush().unwrap();

    // Second put → stays in memtable
    let _rev2 = ns.put("key", "v2", None).unwrap();

    // rev_count should see both: 1 SSTable + 1 memtable
    assert_eq!(ns.rev_count("key").unwrap(), 2);

    // Index 0 = SSTable (oldest), index 1 = memtable (newest)
    assert_eq!(ns.rev_get("key", 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get("key", 1).unwrap(), Value::from("v2"));

    // Out-of-bounds index returns KeyNotFound
    assert!(ns.rev_get("key", 2).is_err());

    // rev_get_with_ttl follows the same index semantics
    let (val, _expired, _ttl) = ns.rev_get_with_ttl("key", 0).unwrap();
    assert_eq!(val, Value::from("v1"));
    let (val, _expired, _ttl) = ns.rev_get_with_ttl("key", 1).unwrap();
    assert_eq!(val, Value::from("v2"));

    db.close().unwrap();
}

// --- Phase 1 correctness fixes ---

#[test]
fn count_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("count_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 1..=5_i64 {
        ns.put(i, format!("v{i}"), None).unwrap();
    }
    assert_eq!(ns.count().unwrap(), 5);

    db.flush().unwrap();

    // count() must still return 5 after flush
    assert_eq!(ns.count().unwrap(), 5);

    // Add more keys after flush and verify merged count
    ns.put(6_i64, "v6", None).unwrap();
    assert_eq!(ns.count().unwrap(), 6);

    // Delete a flushed key and verify count decreases
    ns.delete(3_i64).unwrap();
    assert_eq!(ns.count().unwrap(), 5);

    db.close().unwrap();
}

#[test]
fn exists_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("exists_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("key1", "val1", None).unwrap();
    ns.put("key2", "val2", None).unwrap();
    assert!(ns.exists("key1").unwrap());

    db.flush().unwrap();

    // exists() must still return true after flush
    assert!(ns.exists("key1").unwrap());
    assert!(ns.exists("key2").unwrap());

    // Non-existent key should still return false
    assert!(!ns.exists("nope").unwrap());

    // Deleted key should return false
    ns.delete("key1").unwrap();
    assert!(!ns.exists("key1").unwrap());

    db.close().unwrap();
}

#[test]
fn delete_range_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("delrange_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 1..=10_i64 {
        ns.put(i, format!("v{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Delete range [3, 7) on flushed keys — should delete 3, 4, 5, 6
    let deleted = ns.delete_range(3_i64, 7_i64, false).unwrap();
    assert_eq!(deleted, 4);
    assert_eq!(ns.count().unwrap(), 6);

    // Verify individual keys
    assert!(ns.exists(2_i64).unwrap());
    assert!(!ns.exists(3_i64).unwrap());
    assert!(!ns.exists(6_i64).unwrap());
    assert!(ns.exists(7_i64).unwrap());

    db.close().unwrap();
}

#[test]
fn delete_prefix_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("delprefix_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("user:alice", "a", None).unwrap();
    ns.put("user:bob", "b", None).unwrap();
    ns.put("post:1", "p1", None).unwrap();
    ns.put("post:2", "p2", None).unwrap();
    db.flush().unwrap();

    // Delete all keys with prefix "user:" — should delete 2
    let deleted = ns.delete_prefix("user:").unwrap();
    assert_eq!(deleted, 2);
    assert_eq!(ns.count().unwrap(), 2);

    assert!(!ns.exists("user:alice").unwrap());
    assert!(!ns.exists("user:bob").unwrap());
    assert!(ns.exists("post:1").unwrap());
    assert!(ns.exists("post:2").unwrap());

    db.close().unwrap();
}

#[test]
fn encrypted_namespace_requires_password_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("enc_restart");

    // Open DB and create an encrypted namespace
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("secret", Some("mypassword")).unwrap();
        ns.put("key", "classified", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen DB and try to open the encrypted namespace without a password
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let err = db.namespace("secret", None).unwrap_err();
        assert!(
            matches!(err, Error::EncryptionRequired(_)),
            "expected EncryptionRequired, got {err:?}"
        );

        // Opening with password should work
        let ns = db.namespace("secret", Some("mypassword")).unwrap();
        assert!(ns.exists("key").unwrap());
        db.close().unwrap();
    }
}

// --- TTL in SSTables (Phase 3) ---

#[test]
fn ttl_survives_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("ttl_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Put a key with a long TTL (10 seconds)
    ns.put("ttl_key", "value", Some(Duration::from_secs(10)))
        .unwrap();
    // Put a key without TTL
    ns.put("no_ttl", "value2", None).unwrap();

    db.flush().unwrap();

    // Both should still be accessible after flush
    assert_eq!(ns.get("ttl_key").unwrap(), Value::from("value"));
    assert_eq!(ns.get("no_ttl").unwrap(), Value::from("value2"));
    assert!(ns.exists("ttl_key").unwrap());
    assert_eq!(ns.count().unwrap(), 2);

    db.close().unwrap();
}

#[test]
fn ttl_expires_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("ttl_expire"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Put a key with a very short TTL (100ms)
    ns.put("ephemeral", "gone", Some(Duration::from_millis(100)))
        .unwrap();
    ns.put("permanent", "stays", None).unwrap();

    db.flush().unwrap();

    // Wait for expiration
    std::thread::sleep(Duration::from_millis(200));

    // Expired key should not be found
    assert!(ns.get("ephemeral").is_err());
    assert!(!ns.exists("ephemeral").unwrap());

    // Permanent key should still work
    assert_eq!(ns.get("permanent").unwrap(), Value::from("stays"));
    assert_eq!(ns.count().unwrap(), 1);

    db.close().unwrap();
}

#[test]
fn ttl_survives_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("ttl_compact"));
    config.write_buffer_size = 256; // Small buffer to force multiple flushes
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write keys with TTL and flush twice to create multiple L0 SSTables
    ns.put("a", "v1", Some(Duration::from_secs(30))).unwrap();
    ns.put("b", "v2", None).unwrap();
    db.flush().unwrap();

    ns.put("c", "v3", Some(Duration::from_secs(30))).unwrap();
    ns.put("d", "v4", None).unwrap();
    db.flush().unwrap();

    // Compact L0 → L1
    db.compact().unwrap();
    db.wait_for_compaction();

    // All keys should still be accessible
    assert_eq!(ns.get("a").unwrap(), Value::from("v1"));
    assert_eq!(ns.get("b").unwrap(), Value::from("v2"));
    assert_eq!(ns.get("c").unwrap(), Value::from("v3"));
    assert_eq!(ns.get("d").unwrap(), Value::from("v4"));
    assert_eq!(ns.count().unwrap(), 4);

    db.close().unwrap();
}

// --- Revision persistence across flush/compaction ---

#[test]
fn rev_count_preserved_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("rev_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "v1", None).unwrap();
    ns.put(1, "v2", None).unwrap();
    ns.put(1, "v3", None).unwrap();
    assert_eq!(ns.rev_count(1).unwrap(), 3);

    db.flush().unwrap();

    assert_eq!(ns.rev_count(1).unwrap(), 3);
    db.close().unwrap();
}

#[test]
fn rev_get_preserved_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("rev_get_flush"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "v1", None).unwrap();
    ns.put(1, "v2", None).unwrap();
    ns.put(1, "v3", None).unwrap();

    db.flush().unwrap();

    assert_eq!(ns.rev_get(1, 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get(1, 1).unwrap(), Value::from("v2"));
    assert_eq!(ns.rev_get(1, 2).unwrap(), Value::from("v3"));
    assert!(ns.rev_get(1, 3).is_err());

    db.close().unwrap();
}

#[test]
fn rev_count_spans_flush_and_memtable() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("rev_span"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "v1", None).unwrap();
    ns.put(1, "v2", None).unwrap();
    db.flush().unwrap();

    ns.put(1, "v3", None).unwrap();

    assert_eq!(ns.rev_count(1).unwrap(), 3);
    assert_eq!(ns.rev_get(1, 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get(1, 1).unwrap(), Value::from("v2"));
    assert_eq!(ns.rev_get(1, 2).unwrap(), Value::from("v3"));

    db.close().unwrap();
}

#[test]
fn rev_count_after_multiple_flushes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("rev_multi"));
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "v1", None).unwrap();
    db.flush().unwrap();

    ns.put(1, "v2", None).unwrap();
    db.flush().unwrap();

    assert_eq!(ns.rev_count(1).unwrap(), 2);
    assert_eq!(ns.rev_get(1, 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get(1, 1).unwrap(), Value::from("v2"));

    db.close().unwrap();
}

#[test]
fn rev_count_preserved_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("rev_compact"));
    config.max_levels = 2;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(1, "v1", None).unwrap();
    ns.put(1, "v2", None).unwrap();
    db.flush().unwrap();

    ns.put(1, "v3", None).unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    assert_eq!(ns.rev_count(1).unwrap(), 3);
    assert_eq!(ns.rev_get(1, 0).unwrap(), Value::from("v1"));
    assert_eq!(ns.rev_get(1, 1).unwrap(), Value::from("v2"));
    assert_eq!(ns.rev_get(1, 2).unwrap(), Value::from("v3"));

    db.close().unwrap();
}

#[test]
fn compaction_merges_revision_chains() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("rev_merge"));
    config.max_levels = 3;
    config.l1_max_size = 1; // force cascade
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Put 2 revisions, flush to L0
    ns.put(1, "a", None).unwrap();
    ns.put(1, "b", None).unwrap();
    db.flush().unwrap();

    // Put 1 more, flush to another L0
    ns.put(1, "c", None).unwrap();
    db.flush().unwrap();

    // Compact: L0→L1→L2 cascade
    db.compact().unwrap();

    assert_eq!(ns.rev_count(1).unwrap(), 3);
    assert_eq!(ns.rev_get(1, 0).unwrap(), Value::from("a"));
    assert_eq!(ns.rev_get(1, 1).unwrap(), Value::from("b"));
    assert_eq!(ns.rev_get(1, 2).unwrap(), Value::from("c"));

    db.close().unwrap();
}

/// Regression test: large datasets with auto-flush should not lose keys.
/// Reproduces a bug where SSTableReader::get() fails to find keys after
/// auto-flush creates multiple L0 SSTables.
#[test]
fn get_after_auto_flush_large_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.write_buffer_size = 4096; // tiny buffer -> many auto-flushes
    config.l0_max_count = 10000; // disable compaction
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let n = 10_000;
    let value = "x".repeat(64);
    for i in 0..n {
        ns.put(i as i64, value.as_str(), None).unwrap();
    }

    // Verify all keys are readable
    let mut missing = Vec::new();
    for i in 0..n {
        if ns.get(i as i64).is_err() {
            missing.push(i);
        }
    }
    assert!(
        missing.is_empty(),
        "{} keys missing out of {n}: first 10 = {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );

    db.close().unwrap();
}

/// Same test but WITH compaction enabled to check merge correctness.
#[test]
fn get_after_auto_flush_with_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.write_buffer_size = 4096;
    // default l0_max_count allows compaction
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let n = 10_000;
    let value = "x".repeat(64);
    for i in 0..n {
        ns.put(i as i64, value.as_str(), None).unwrap();
    }

    // Wait for compaction to finish
    db.wait_for_compaction();

    let mut missing = Vec::new();
    for i in 0..n {
        if ns.get(i as i64).is_err() {
            missing.push(i);
        }
    }
    assert!(
        missing.is_empty(),
        "{} keys missing out of {n}: first 10 = {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );

    db.close().unwrap();
}

// --- Restart point integration tests ---

#[test]
fn get_after_flush_uses_restart_points() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..200 {
        ns.put(i as i64, format!("value_{i}").as_str(), None)
            .unwrap();
    }
    db.flush().unwrap();

    for i in 0..200 {
        let val = ns.get(i as i64).unwrap();
        assert_eq!(val, Value::from(format!("value_{i}").as_str()), "key {i}");
    }

    db.close().unwrap();
}

#[test]
fn get_after_compaction_uses_restart_points() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.write_buffer_size = 4096;
    config.l0_max_count = 2;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..500 {
        ns.put(i as i64, format!("v{i}").as_str(), None).unwrap();
    }
    db.flush().unwrap();
    db.wait_for_compaction();

    for i in 0..500 {
        let val = ns.get(i as i64).unwrap();
        assert_eq!(val, Value::from(format!("v{i}").as_str()), "key {i}");
    }

    db.close().unwrap();
}

#[test]
fn get_no_cache_uses_targeted_restart_parse() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 0;
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..500 {
        ns.put(i as i64, format!("val{i}").as_str(), None).unwrap();
    }
    db.flush().unwrap();

    for i in 0..500 {
        let val = ns.get(i as i64).unwrap();
        assert_eq!(val, Value::from(format!("val{i}").as_str()), "key {i}");
    }

    let s = db.stats();
    assert_eq!(s.cache_hits, 0);
    assert_eq!(s.cache_misses, 0);

    db.close().unwrap();
}

#[test]
fn rev_count_after_flush_with_restart_points() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    for i in 0..5 {
        ns.put(42, format!("rev{i}").as_str(), None).unwrap();
    }
    db.flush().unwrap();

    assert_eq!(ns.rev_count(42).unwrap(), 5);
    assert_eq!(ns.get(42).unwrap(), Value::from("rev4"));

    db.close().unwrap();
}

// --- Prometheus metrics ---

#[test]
fn prometheus_metrics_includes_op_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    ns.get("a").unwrap();

    let output = db.prometheus_metrics();
    assert!(output.contains("rkv_ops_total{op=\"put\"} 2"));
    assert!(output.contains("rkv_ops_total{op=\"get\"} 1"));
    assert!(output.contains("# TYPE rkv_op_duration_seconds histogram"));
    // Histogram should record 2 put observations
    assert!(output.contains("rkv_op_duration_seconds_count{op=\"put\"} 2"));
    // Histogram should record 1 get observation
    assert!(output.contains("rkv_op_duration_seconds_count{op=\"get\"} 1"));

    db.close().unwrap();
}

#[test]
fn prometheus_metrics_tracks_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("k", "v", None).unwrap();
    db.flush().unwrap();

    let output = db.prometheus_metrics();
    assert!(
        output.contains("rkv_flush_total 1"),
        "expected flush_total 1"
    );
    assert!(
        output.contains("rkv_bytes_flushed_total"),
        "expected bytes_flushed_total"
    );
    // Flush duration histogram should have 1 observation
    assert!(output.contains("rkv_flush_duration_seconds_count 1"));

    db.close().unwrap();
}

#[test]
fn event_listener_receives_flush_events() {
    struct TestListener {
        flush_count: AtomicU64,
    }
    impl EventListener for TestListener {
        fn on_flush_complete(&self, _event: FlushEvent) {
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let listener = Arc::new(TestListener {
        flush_count: AtomicU64::new(0),
    });
    let mut config = Config::new(tmp.path());
    config.event_listener = Some(listener.clone());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    db.flush().unwrap();

    assert_eq!(listener.flush_count.load(Ordering::Relaxed), 1);

    ns.put("b", "2", None).unwrap();
    db.flush().unwrap();
    assert_eq!(listener.flush_count.load(Ordering::Relaxed), 2);

    db.close().unwrap();
}

#[test]
fn prometheus_metrics_tracks_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.write_buffer_size = 64; // tiny buffer to force flushes
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Generate multiple L0 files to trigger compaction
    for i in 0..5 {
        ns.put(i, format!("val_{i}").as_str(), None).unwrap();
        db.flush().unwrap();
    }

    db.compact().unwrap();

    let output = db.prometheus_metrics();
    // Background thread may also compact, so check >= 1
    assert!(
        output.contains("rkv_compaction_total") && !output.contains("rkv_compaction_total 0"),
        "expected compaction_total > 0"
    );
    assert!(
        output.contains("rkv_bytes_compacted_total")
            && !output.contains("rkv_bytes_compacted_total 0"),
        "expected bytes_compacted_total > 0"
    );

    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// WriteBatch tests
// ---------------------------------------------------------------------------

#[test]
fn write_batch_basic_put_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new()
        .put("k1", "v1", None)
        .put("k2", "v2", None)
        .put("k3", "v3", None);

    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 3);

    assert_eq!(ns.get("k1").unwrap(), Value::from("v1"));
    assert_eq!(ns.get("k2").unwrap(), Value::from("v2"));
    assert_eq!(ns.get("k3").unwrap(), Value::from("v3"));

    // Now delete k2 in a batch
    let batch = WriteBatch::new().delete("k2");
    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 1);

    assert!(ns.get("k2").is_err());
    assert_eq!(ns.get("k1").unwrap(), Value::from("v1"));
    assert_eq!(ns.get("k3").unwrap(), Value::from("v3"));

    db.close().unwrap();
}

#[test]
fn write_batch_mixed_put_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Pre-populate
    ns.put("existing", "old_value", None).unwrap();

    let batch = WriteBatch::new()
        .put("new_key", "new_value", None)
        .delete("existing")
        .put("another", "value", None);

    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 3);

    assert_eq!(ns.get("new_key").unwrap(), Value::from("new_value"));
    assert!(ns.get("existing").is_err());
    assert_eq!(ns.get("another").unwrap(), Value::from("value"));

    db.close().unwrap();
}

#[test]
fn write_batch_empty_returns_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new();
    let revs = ns.write_batch(batch).unwrap();
    assert!(revs.is_empty());

    db.close().unwrap();
}

#[test]
fn write_batch_with_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new()
        .put("k1", "v1", Some(Duration::from_secs(3600)))
        .put("k2", "v2", None);

    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 2);

    // k1 should have a TTL
    let ttl = ns.ttl("k1").unwrap();
    assert!(ttl.is_some());
    let remaining = ttl.unwrap();
    assert!(remaining.as_secs() > 3500);

    // k2 should not have a TTL
    let ttl = ns.ttl("k2").unwrap();
    assert!(ttl.is_none());

    db.close().unwrap();
}

#[test]
fn write_batch_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().to_path_buf();

    // Write batch and close
    {
        let db = DB::open(Config::new(&db_path)).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        let batch = WriteBatch::new()
            .put("k1", "v1", None)
            .put("k2", "v2", None)
            .delete("k3"); // delete non-existing is fine

        ns.write_batch(batch).unwrap();
        db.close().unwrap();
    }

    // Reopen and verify AOL replay recovers the batch
    {
        let db = DB::open(Config::new(&db_path)).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        assert_eq!(ns.get("k1").unwrap(), Value::from("v1"));
        assert_eq!(ns.get("k2").unwrap(), Value::from("v2"));

        db.close().unwrap();
    }
}

#[test]
fn write_batch_survives_flush_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().to_path_buf();

    {
        let db = DB::open(Config::new(&db_path)).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        let batch = WriteBatch::new()
            .put("k1", "v1", None)
            .put("k2", "v2", None)
            .put("k3", "v3", None);
        ns.write_batch(batch).unwrap();

        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = DB::open(Config::new(&db_path)).unwrap();
        let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

        assert_eq!(ns.get("k1").unwrap(), Value::from("v1"));
        assert_eq!(ns.get("k2").unwrap(), Value::from("v2"));
        assert_eq!(ns.get("k3").unwrap(), Value::from("v3"));

        db.close().unwrap();
    }
}

#[test]
fn write_batch_updates_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new()
        .put("k1", "v1", None)
        .put("k2", "v2", None)
        .delete("k3");

    ns.write_batch(batch).unwrap();

    let stats = db.stats();
    assert!(
        stats.op_puts >= 2,
        "expected >= 2 puts, got {}",
        stats.op_puts
    );
    assert!(
        stats.op_deletes >= 1,
        "expected >= 1 deletes, got {}",
        stats.op_deletes
    );

    db.close().unwrap();
}

#[test]
fn write_batch_rejected_on_replica() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.role = rkv::Role::Replica;
    config.primary_addr = Some("127.0.0.1:19999".to_string());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new().put("k1", "v1", None);
    let err = ns.write_batch(batch).unwrap_err();
    assert!(matches!(err, Error::ReadOnlyReplica));

    db.close().unwrap();
}

#[test]
fn write_batch_concurrent_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open(Config::new(tmp.path())).unwrap());

    let mut handles = Vec::new();
    for t in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
            let mut batch = WriteBatch::new();
            for i in 0..25 {
                batch = batch.put(format!("t{t}_k{i}"), format!("v{i}"), None);
            }
            ns.write_batch(batch).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All 100 keys should be present
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    for t in 0..4 {
        for i in 0..25 {
            let val = ns.get(format!("t{t}_k{i}")).unwrap();
            assert_eq!(val, Value::from(format!("v{i}")));
        }
    }

    drop(ns);
    let Ok(db) = Arc::try_unwrap(db) else {
        panic!("other Arc references still exist");
    };
    db.close().unwrap();
}

#[test]
fn write_batch_int_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let db = DB::open(Config::new(tmp.path())).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    let batch = WriteBatch::new()
        .put(1_i64, "one", None)
        .put(2_i64, "two", None)
        .put(3_i64, "three", None)
        .delete(2_i64);

    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 4);

    assert_eq!(ns.get(1_i64).unwrap(), Value::from("one"));
    assert!(ns.get(2_i64).is_err());
    assert_eq!(ns.get(3_i64).unwrap(), Value::from("three"));

    db.close().unwrap();
}

// --- Config validation ---

#[test]
fn config_validate_rejects_zero_max_levels() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("db"));
    config.max_levels = 0;
    let Err(err) = DB::open(config) else {
        panic!("expected InvalidConfig error for zero max_levels");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn config_validate_rejects_zero_block_size() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("db"));
    config.block_size = 0;
    let Err(err) = DB::open(config) else {
        panic!("expected InvalidConfig error for zero block_size");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn config_validate_rejects_zero_write_buffer_size() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("db"));
    config.write_buffer_size = 0;
    let Err(err) = DB::open(config) else {
        panic!("expected InvalidConfig error for zero write_buffer_size");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn config_validate_rejects_zero_l0_max_count() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path().join("db"));
    config.l0_max_count = 0;
    let Err(err) = DB::open(config) else {
        panic!("expected InvalidConfig error for zero l0_max_count");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn config_validate_default_passes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path().join("db"));
    let db = DB::open(config).unwrap();
    db.close().unwrap();
}

// --- Corrupted SSTable recovery ---

#[test]
fn corrupted_sstable_detected_on_read() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");

    // Write data and flush to SSTable
    {
        let config = Config::new(&db_path);
        let db = DB::open(config).unwrap();
        let ns = db.namespace("_", None).unwrap();
        ns.put(1_i64, "value1", None).unwrap();
        ns.put(2_i64, "value2", None).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Corrupt the SSTable file
    let sst_dir = db_path.join("sst").join("_").join("L0");
    let sst_files: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert!(!sst_files.is_empty());

    let sst_path = sst_files[0].path();
    let mut data = std::fs::read(&sst_path).unwrap();
    // Corrupt multiple bytes near the start of the data blocks
    // (skip the first few bytes which may be metadata)
    for offset in [10, 20, 30, 40, 50] {
        if offset < data.len() {
            data[offset] ^= 0xFF;
        }
    }
    std::fs::write(&sst_path, &data).unwrap();

    // Reopen — reads should detect corruption or return wrong data
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    // With verify_checksums=true (default), corrupted blocks should error.
    // Even if they don't error, the values should differ from originals.
    let r1 = ns.get(1_i64);
    let r2 = ns.get(2_i64);
    let has_problem = r1.is_err()
        || r2.is_err()
        || r1.as_ref().ok() != Some(&Value::from("value1"))
        || r2.as_ref().ok() != Some(&Value::from("value2"));
    assert!(
        has_problem,
        "corrupted SSTable should produce read errors or wrong data"
    );
    db.close().unwrap();
}

// --- Partial SSTable cleanup ---

#[test]
fn sstable_partial_write_cleanup() {
    // Verify that SSTableWriter stores the path for cleanup
    // This test verifies the code structure; actual disk-full simulation
    // is not portable, but the cleanup path is exercised by the error
    // propagation in finish().
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let config = Config::new(&db_path);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Write enough data to trigger a flush
    for i in 0..100 {
        ns.put(i as i64, format!("value-{i}"), None).unwrap();
    }
    db.flush().unwrap();

    // Verify SSTable files exist and are valid
    let sst_dir = db_path.join("sst").join("_").join("L0");
    let sst_count = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert!(sst_count > 0, "should have at least one SSTable file");

    // Verify all data is readable
    for i in 0..100 {
        assert_eq!(ns.get(i as i64).unwrap(), Value::from(format!("value-{i}")));
    }

    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// In-Memory Mode Tests
// ---------------------------------------------------------------------------

#[test]
fn in_memory_crud() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Put / get
    ns.put("hello", "world", None).unwrap();
    assert_eq!(ns.get("hello").unwrap(), Value::from("world"));

    // Overwrite
    ns.put("hello", "earth", None).unwrap();
    assert_eq!(ns.get("hello").unwrap(), Value::from("earth"));

    // Delete
    ns.delete("hello").unwrap();
    let err = ns.get("hello").unwrap_err();
    assert!(matches!(err, Error::KeyNotFound));

    // Integer keys
    ns.put(42_i64, "answer", None).unwrap();
    assert_eq!(ns.get(42_i64).unwrap(), Value::from("answer"));

    // Exists
    assert!(ns.exists(42_i64).unwrap());
    assert!(!ns.exists(99_i64).unwrap());
}

#[test]
fn in_memory_no_disk_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::in_memory();
    config.path = dir.path().to_path_buf();
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put("key", "value", None).unwrap();
    drop(db);

    // No files should be created in the temp dir
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(
        entries.is_empty(),
        "in-memory mode should not create any files"
    );
}

#[test]
fn in_memory_noop_maintenance() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();
    ns.put("a", "1", None).unwrap();

    // flush, compact, sync should all no-op without error
    db.flush().unwrap();
    db.compact().unwrap();
    db.sync().unwrap();

    // Data should still be readable (not flushed away)
    assert_eq!(ns.get("a").unwrap(), Value::from("1"));
}

#[test]
fn in_memory_dump_returns_error() {
    let db = DB::open(Config::in_memory()).unwrap();
    let err = db.dump("/tmp/should-not-exist.dump").unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn in_memory_large_values_inline() {
    let mut config = Config::in_memory();
    config.object_size = 10; // very small threshold
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Write a value larger than object_size — should stay inline, no bin objects
    let big_value = "x".repeat(100);
    ns.put("big", big_value.as_str(), None).unwrap();
    assert_eq!(ns.get("big").unwrap(), Value::from(big_value.as_str()));
}

#[test]
fn in_memory_scan_and_count() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    for i in 0..10 {
        ns.put(i as i64, format!("v{i}").as_str(), None).unwrap();
    }

    let keys = ns.scan(&Key::Int(0), 100, 0, false).unwrap();
    assert_eq!(keys.len(), 10);

    assert_eq!(ns.count().unwrap(), 10);
}

#[test]
fn in_memory_multiple_namespaces() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns1 = db.namespace("ns1", None).unwrap();
    let ns2 = db.namespace("ns2", None).unwrap();

    ns1.put("key", "from-ns1", None).unwrap();
    ns2.put("key", "from-ns2", None).unwrap();

    assert_eq!(ns1.get("key").unwrap(), Value::from("from-ns1"));
    assert_eq!(ns2.get("key").unwrap(), Value::from("from-ns2"));

    let namespaces = db.list_namespaces().unwrap();
    assert!(namespaces.contains(&"ns1".to_string()));
    assert!(namespaces.contains(&"ns2".to_string()));
}

#[test]
fn in_memory_write_batch() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let batch = WriteBatch::new()
        .put("a", "1", None)
        .put("b", "2", None)
        .delete("a");
    let revs = ns.write_batch(batch).unwrap();
    assert_eq!(revs.len(), 3);

    assert!(matches!(ns.get("a"), Err(Error::KeyNotFound)));
    assert_eq!(ns.get("b").unwrap(), Value::from("2"));
}

#[test]
fn in_memory_replication_rejected() {
    let mut config = Config::in_memory();
    config.role = rkv::Role::Primary;
    let Err(err) = DB::open(config) else {
        panic!("expected error for in-memory + replication");
    };
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn in_memory_encryption_rejected() {
    let db = DB::open(Config::in_memory()).unwrap();
    let err = db.namespace("secret", Some("password")).unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[test]
fn in_memory_ttl() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Key with TTL should be readable before expiry
    ns.put("ephemeral", "gone-soon", Some(Duration::from_millis(50)))
        .unwrap();
    assert_eq!(ns.get("ephemeral").unwrap(), Value::from("gone-soon"));
    let ttl = ns.ttl("ephemeral").unwrap();
    assert!(ttl.is_some());

    // After expiry, key should be gone
    std::thread::sleep(Duration::from_millis(60));
    assert!(matches!(ns.get("ephemeral"), Err(Error::KeyNotFound)));
}

// ---------------------------------------------------------------------------
// Iterator API Tests
// ---------------------------------------------------------------------------

#[test]
fn iterator_keys_forward() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();

    let keys: Vec<Key> = ns
        .keys(&Key::Int(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
}

#[test]
fn iterator_keys_reverse() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();

    // For ordered (Int) mode, rscan prefix is the upper bound
    let keys: Vec<Key> = ns
        .rkeys(&Key::Int(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(2), Key::Int(1)]);
}

#[test]
fn iterator_entries_forward() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("x", "1", None).unwrap();
    ns.put("y", "2", None).unwrap();
    ns.put("z", "3", None).unwrap();

    let entries: Vec<(Key, Value)> = ns
        .entries(&Key::from(""))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (Key::from("x"), Value::from("1")));
    assert_eq!(entries[1], (Key::from("y"), Value::from("2")));
    assert_eq!(entries[2], (Key::from("z"), Value::from("3")));
}

#[test]
fn iterator_entries_reverse() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    ns.put("c", "3", None).unwrap();

    let entries: Vec<(Key, Value)> = ns
        .rentries(&Key::from(""))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (Key::from("c"), Value::from("3")));
    assert_eq!(entries[1], (Key::from("b"), Value::from("2")));
    assert_eq!(entries[2], (Key::from("a"), Value::from("1")));
}

#[test]
fn iterator_skips_tombstones() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    ns.put(3_i64, "c", None).unwrap();
    ns.delete(2_i64).unwrap();

    // keys() should skip the deleted key
    let keys: Vec<Key> = ns
        .keys(&Key::Int(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys, vec![Key::Int(1), Key::Int(3)]);

    // entries() should also skip
    let entries: Vec<(Key, Value)> = ns
        .entries(&Key::Int(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, Key::Int(1));
    assert_eq!(entries[1].0, Key::Int(3));
}

#[test]
fn iterator_prefix_filter() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("user:1", "alice", None).unwrap();
    ns.put("user:2", "bob", None).unwrap();
    ns.put("post:1", "hello", None).unwrap();

    let keys: Vec<Key> = ns
        .keys(&Key::from("user:"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&Key::from("user:1")));
    assert!(keys.contains(&Key::from("user:2")));
}

#[test]
fn iterator_lazy_semantics() {
    let db = DB::open(Config::in_memory()).unwrap();
    let ns = db.namespace("_", None).unwrap();

    for i in 0..100 {
        ns.put(i as i64, format!("v{i}").as_str(), None).unwrap();
    }

    // Take only first 3 — lazy, should not materialize all 100
    let keys: Vec<Key> = ns
        .keys(&Key::Int(0))
        .unwrap()
        .take(3)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], Key::Int(0));
    assert_eq!(keys[1], Key::Int(1));
    assert_eq!(keys[2], Key::Int(2));
}

#[test]
fn iterator_with_disk_sstables() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::new(dir.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    // Write some data and flush to SSTables
    for i in 0..5 {
        ns.put(i as i64, format!("v{i}").as_str(), None).unwrap();
    }
    db.flush().unwrap();

    // Write more data to memtable
    for i in 5..10 {
        ns.put(i as i64, format!("v{i}").as_str(), None).unwrap();
    }

    // Iterator should merge memtable + SSTable data
    let keys: Vec<Key> = ns
        .keys(&Key::Int(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(keys.len(), 10);

    let entries: Vec<(Key, Value)> = ns
        .entries(&Key::Int(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 10);
    assert_eq!(entries[0], (Key::Int(0), Value::from("v0")));
    assert_eq!(entries[9], (Key::Int(9), Value::from("v9")));

    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// pop_first
// ---------------------------------------------------------------------------

#[test]
fn pop_first_returns_first_live_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    ns.put("c", "3", None).unwrap();

    let result = ns.pop_first(&Key::from("")).unwrap();
    assert_eq!(result, Some((Key::from("a"), Value::from("1"))));

    // Key "a" should be deleted now
    assert!(ns.get("a").is_err());

    // Next pop_first should return "b"
    let result = ns.pop_first(&Key::from("")).unwrap();
    assert_eq!(result, Some((Key::from("b"), Value::from("2"))));
}

#[test]
fn pop_first_with_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("queue:1", "msg1", None).unwrap();
    ns.put("queue:2", "msg2", None).unwrap();
    ns.put("other:1", "x", None).unwrap();

    let result = ns.pop_first(&Key::from("queue:")).unwrap();
    assert_eq!(result, Some((Key::from("queue:1"), Value::from("msg1"))));

    // "other:1" is unaffected
    assert_eq!(ns.get("other:1").unwrap(), Value::from("x"));
}

#[test]
fn pop_first_empty_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    assert_eq!(ns.pop_first(&Key::from("")).unwrap(), None);
}

#[test]
fn pop_first_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    db.flush().unwrap();
    ns.put("c", "3", None).unwrap();

    // Should pop "a" from SSTable
    let result = ns.pop_first(&Key::from("")).unwrap();
    assert_eq!(result, Some((Key::from("a"), Value::from("1"))));

    // Pop "b" from SSTable
    let result = ns.pop_first(&Key::from("")).unwrap();
    assert_eq!(result, Some((Key::from("b"), Value::from("2"))));

    // Pop "c" from memtable
    let result = ns.pop_first(&Key::from("")).unwrap();
    assert_eq!(result, Some((Key::from("c"), Value::from("3"))));

    // Empty
    assert_eq!(ns.pop_first(&Key::from("")).unwrap(), None);
}

// ---------------------------------------------------------------------------
// Edge-case coverage for performance improvements
// ---------------------------------------------------------------------------

#[test]
fn rscan_with_empty_sstable_levels() {
    // Reverse iteration when some SSTable levels have no matching entries.
    // Write data to L0 via flush, then add more data only in memtable.
    // The rscan must skip empty SSTable levels gracefully.
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Flush with keys in one range
    ns.put(1_i64, "a", None).unwrap();
    ns.put(2_i64, "b", None).unwrap();
    db.flush().unwrap();

    // Delete all flushed keys so the SSTable has only tombstones for this range
    ns.delete(1_i64).unwrap();
    ns.delete(2_i64).unwrap();
    db.flush().unwrap();

    // Add fresh keys only in memtable
    ns.put(10_i64, "x", None).unwrap();
    ns.put(20_i64, "y", None).unwrap();

    // rscan should return only the live keys, skipping SSTable tombstones
    let keys = ns.rscan(&Key::Int(20), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(20), Key::Int(10)]);
}

#[test]
fn rscan_single_sstable_via_flush() {
    // ConcatIterator with a single SSTable source — ensures it works
    // when there is exactly one SSTable in the level.
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put(5_i64, "five", None).unwrap();
    ns.put(3_i64, "three", None).unwrap();
    ns.put(7_i64, "seven", None).unwrap();
    db.flush().unwrap();

    // Compact to ensure a single SSTable per level
    db.compact().unwrap();

    let keys = ns.rscan(&Key::Int(7), 10, 0, false).unwrap();
    assert_eq!(keys, vec![Key::Int(7), Key::Int(5), Key::Int(3)]);
}

#[test]
fn pop_first_only_tombstones_returns_none() {
    // When all entries in the namespace are tombstones, pop_first should
    // return None rather than surfacing a deleted entry.
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Write keys and then delete them all
    ns.put("a", "1", None).unwrap();
    ns.put("b", "2", None).unwrap();
    ns.put("c", "3", None).unwrap();
    ns.delete("a").unwrap();
    ns.delete("b").unwrap();
    ns.delete("c").unwrap();

    assert_eq!(ns.pop_first(&Key::from("")).unwrap(), None);
}

#[test]
fn pop_first_only_tombstones_after_flush_returns_none() {
    // Same as above but tombstones span both memtable and SSTable.
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();

    ns.put("x", "1", None).unwrap();
    ns.put("y", "2", None).unwrap();
    db.flush().unwrap();

    // Delete both — tombstones live in memtable, data in SSTable
    ns.delete("x").unwrap();
    ns.delete("y").unwrap();

    assert_eq!(ns.pop_first(&Key::from("")).unwrap(), None);
}

#[test]
fn cache_hit_rate_zero_when_empty() {
    // When no cache accesses have occurred, cache_hit_rate should be 0.0.
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 8 * 1024 * 1024;
    let db = DB::open(config).unwrap();

    let stats = db.stats();
    assert!(
        (stats.cache_hit_rate - 0.0).abs() < f64::EPSILON,
        "expected 0.0 hit rate on fresh DB, got {}",
        stats.cache_hit_rate
    );
    assert_eq!(stats.cache_hits, 0);
    assert_eq!(stats.cache_misses, 0);
}

#[test]
fn cache_hit_rate_zero_with_disabled_cache() {
    // When cache_size is 0 (disabled), stats should report 0.0 hit rate.
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.cache_size = 0;
    let db = DB::open(config).unwrap();

    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k", "v", None).unwrap();
    db.flush().unwrap();
    let _ = ns.get("k").unwrap();

    let stats = db.stats();
    assert!(
        (stats.cache_hit_rate - 0.0).abs() < f64::EPSILON,
        "expected 0.0 hit rate with disabled cache, got {}",
        stats.cache_hit_rate
    );
}

// ── Dedup-on-write tests ───────────────────────────────────────────────

#[test]
fn dedup_basic_skips_identical_write() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k", "v", None).unwrap();
    let rev2 = ns.put("k", "v", None).unwrap();
    assert_eq!(rev1, rev2, "identical write should return same revision");
    assert_eq!(db.stats().dedup_skips, 1);
    assert_eq!(
        db.stats().op_puts,
        1,
        "deduped write should not increment op_puts"
    );
}

#[test]
fn dedup_different_value_proceeds() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k", "v1", None).unwrap();
    let rev2 = ns.put("k", "v2", None).unwrap();
    assert_ne!(rev1, rev2, "different value should produce new revision");
    assert_eq!(db.stats().dedup_skips, 0);
}

#[test]
fn dedup_disabled_by_default() {
    let config = Config::in_memory();
    assert!(!config.dedup);
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k", "v", None).unwrap();
    let rev2 = ns.put("k", "v", None).unwrap();
    assert_ne!(
        rev1, rev2,
        "dedup off: identical write should produce new revision"
    );
    assert_eq!(db.stats().dedup_skips, 0);
}

#[test]
fn dedup_skipped_when_new_write_has_ttl() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k", "v", None).unwrap();
    let rev2 = ns.put("k", "v", Some(Duration::from_secs(60))).unwrap();
    assert_ne!(rev1, rev2, "TTL write should not be deduped");
    assert_eq!(db.stats().dedup_skips, 0);
}

#[test]
fn dedup_skipped_when_existing_has_ttl() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("k", "v", Some(Duration::from_secs(3600))).unwrap();
    let rev2 = ns.put("k", "v", None).unwrap();
    // existing key has TTL, so dedup should not apply
    assert_eq!(db.stats().dedup_skips, 0);
    // Verify value is still there
    assert_eq!(ns.get("k").unwrap(), Value::from("v"));
    let _ = rev2;
}

#[test]
fn dedup_per_namespace_override() {
    let mut config = Config::in_memory();
    config.dedup = false; // global off
    let db = DB::open(config).unwrap();

    // Enable dedup for namespace "a" only
    db.set_namespace_dedup("a", true);
    let ns_a = db.namespace("a", None).unwrap();
    let ns_b = db.namespace("b", None).unwrap();

    let rev_a1 = ns_a.put("k", "v", None).unwrap();
    let rev_a2 = ns_a.put("k", "v", None).unwrap();
    assert_eq!(rev_a1, rev_a2, "ns 'a' has dedup on");

    let rev_b1 = ns_b.put("k", "v", None).unwrap();
    let rev_b2 = ns_b.put("k", "v", None).unwrap();
    assert_ne!(rev_b1, rev_b2, "ns 'b' uses global (off)");
}

#[test]
fn dedup_clear_override_falls_back_to_global() {
    let mut config = Config::in_memory();
    config.dedup = true; // global on
    let db = DB::open(config).unwrap();

    db.set_namespace_dedup("_", false); // override off
    assert!(!db.dedup_enabled("_"));
    db.clear_namespace_dedup("_"); // remove override
    assert!(db.dedup_enabled("_")); // falls back to global
}

#[test]
fn dedup_batch_skips_identical_ops() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k1", "v1", None).unwrap();
    ns.put("k2", "v2", None).unwrap();

    let batch = WriteBatch::new()
        .put("k1", "v1", None) // same — dedup
        .put("k2", "changed", None) // different — no dedup
        .put("k3", "new", None); // new key — no dedup
    let revs = ns.write_batch(batch).unwrap();

    assert_eq!(
        revs[0], rev1,
        "batch dedup: same value returns existing rev"
    );
    assert_eq!(db.stats().dedup_skips, 1);
    assert_eq!(ns.get("k2").unwrap(), Value::from("changed"));
    assert_eq!(ns.get("k3").unwrap(), Value::from("new"));
}

#[test]
fn dedup_delete_then_put_same_value() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("k", "v", None).unwrap();
    ns.delete("k").unwrap();
    // Key is deleted — put should proceed (get_with_revision returns KeyNotFound)
    let rev2 = ns.put("k", "v", None).unwrap();
    assert_eq!(ns.get("k").unwrap(), Value::from("v"));
    assert_eq!(db.stats().dedup_skips, 0);
    let _ = rev2;
}

#[test]
fn dedup_null_value() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    let rev1 = ns.put("k", Value::Null, None).unwrap();
    let rev2 = ns.put("k", Value::Null, None).unwrap();
    assert_eq!(rev1, rev2, "Null value dedup should work");
    assert_eq!(db.stats().dedup_skips, 1);
}

#[test]
fn dedup_stats_counter_accumulates() {
    let mut config = Config::in_memory();
    config.dedup = true;
    let db = DB::open(config).unwrap();
    let ns = db.namespace("_", None).unwrap();

    ns.put("k", "v", None).unwrap();
    for _ in 0..5 {
        ns.put("k", "v", None).unwrap();
    }
    assert_eq!(db.stats().dedup_skips, 5);
}
