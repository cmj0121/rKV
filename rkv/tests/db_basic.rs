use std::path::PathBuf;
use std::time::Duration;

use rkv::{
    Compression, Config, Error, IoModel, Key, RevisionID, Stats, Value, DB, DEFAULT_NAMESPACE,
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
    assert!(names.is_empty());
}

#[test]
fn drop_default_namespace_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.drop_namespace(DEFAULT_NAMESPACE).unwrap_err();
    assert!(matches!(err, Error::InvalidNamespace(_)));
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

    let keys = ns.scan(&Key::Int(1), 10, 0).unwrap();
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

    let keys = ns.rscan(&Key::Int(3), 10, 0).unwrap();
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

    let keys = ns.scan(&Key::from("user:"), 10, 0).unwrap();
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
    let keys = ns.scan(&Key::Int(1), 2, 2).unwrap();
    assert_eq!(keys, vec![Key::Int(3), Key::Int(4)]);

    // Skip all
    let keys = ns.scan(&Key::Int(1), 10, 10).unwrap();
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
    let keys = ns.rscan(&Key::Int(5), 2, 1).unwrap();
    assert_eq!(keys, vec![Key::Int(4), Key::Int(3)]);

    // Skip all
    let keys = ns.rscan(&Key::Int(5), 10, 10).unwrap();
    assert!(keys.is_empty());
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

    ns.put("k", "v", Some(Duration::from_millis(1))).unwrap();
    std::thread::sleep(Duration::from_millis(10));

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
    assert_eq!(s.namespace_count, 0);
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

    std::thread::sleep(Duration::from_millis(10));
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
    assert_eq!(config.cache_size, 8 * 1024 * 1024);
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
fn sync_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.sync().unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn destroy_returns_not_implemented() {
    let err = DB::destroy(PathBuf::from("/tmp/rkv_test_destroy")).unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn repair_returns_not_implemented() {
    let result = DB::repair(PathBuf::from("/tmp/rkv_test_repair"));
    let Err(err) = result else {
        panic!("expected NotImplemented error");
    };
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn dump_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.dump("/tmp/rkv_test_dump.bak").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn load_returns_not_implemented() {
    let result = DB::load(PathBuf::from("/tmp/rkv_test_load.bak"));
    let Err(err) = result else {
        panic!("expected NotImplemented error");
    };
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn compact_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.compact().unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
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
        ns.put("k", "v", Some(Duration::from_millis(1))).unwrap();
        db.close().unwrap();
    }

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(10));

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
        let keys = ns.scan(&Key::from("user:"), 10, 0).unwrap();
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
        let keys = ns.scan(&Key::Int(1), 10, 0).unwrap();
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

    // Count total object files: should be exactly 1 (dedup)
    let ns_objects_dir = tmp.path().join("objects").join("_");
    let mut file_count = 0;
    for fan_out in std::fs::read_dir(ns_objects_dir).unwrap() {
        let fan_out = fan_out.unwrap();
        if fan_out.file_type().unwrap().is_dir() {
            for f in std::fs::read_dir(fan_out.path()).unwrap() {
                if f.unwrap().file_type().unwrap().is_file() {
                    file_count += 1;
                }
            }
        }
    }
    assert_eq!(file_count, 1);
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

    // Object file should exist
    let ns_objects_dir = tmp.path().join("objects").join("_");
    let mut file_count = 0;
    for fan_out in std::fs::read_dir(ns_objects_dir).unwrap() {
        let fan_out = fan_out.unwrap();
        if fan_out.file_type().unwrap().is_dir() {
            for f in std::fs::read_dir(fan_out.path()).unwrap() {
                if f.unwrap().file_type().unwrap().is_file() {
                    file_count += 1;
                }
            }
        }
    }
    assert!(file_count > 0);
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

    // Count object files in each namespace — should be 1 each (separate stores)
    let count_files = |dir: &std::path::Path| -> usize {
        let mut count = 0;
        for fan_out in std::fs::read_dir(dir).unwrap() {
            let fan_out = fan_out.unwrap();
            if fan_out.file_type().unwrap().is_dir() {
                for f in std::fs::read_dir(fan_out.path()).unwrap() {
                    if f.unwrap().file_type().unwrap().is_file() {
                        count += 1;
                    }
                }
            }
        }
        count
    };
    assert_eq!(count_files(&ns1_objects), 1);
    assert_eq!(count_files(&ns2_objects), 1);
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

    // Only 1 object file — dedup still works within a namespace
    let ns_objects = tmp.path().join("objects").join("myns");
    let mut file_count = 0;
    for fan_out in std::fs::read_dir(ns_objects).unwrap() {
        let fan_out = fan_out.unwrap();
        if fan_out.file_type().unwrap().is_dir() {
            for f in std::fs::read_dir(fan_out.path()).unwrap() {
                if f.unwrap().file_type().unwrap().is_file() {
                    file_count += 1;
                }
            }
        }
    }
    assert_eq!(file_count, 1);
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
fn encrypted_wrong_password_returns_corruption() {
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
        // Same namespace, wrong password — data decryption should fail
        let ns = db.namespace("vault", Some("wrong")).unwrap();
        let err = ns.get("key").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
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

    let keys = ns.scan(&Key::from("user:"), 10, 0).unwrap();
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

    // Should have 2 SSTable files
    let sst_dir = tmp.path().join("sst").join("_");
    let count = std::fs::read_dir(&sst_dir).unwrap().count();
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

    let aol_path = tmp.path().join("aol");
    let size_before = std::fs::metadata(&aol_path).unwrap().len();
    assert!(size_before > 8); // More than just header

    db.flush().unwrap();

    let size_after = std::fs::metadata(&aol_path).unwrap().len();
    assert_eq!(size_after, 8); // Header only (magic + version + reserved)
}
