use std::path::PathBuf;
use std::time::Duration;

use rkv::{Config, Error, IoModel, Key, RevisionID, Stats, Value, DB, DEFAULT_NAMESPACE};

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
fn list_namespaces_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.list_namespaces().unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
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
fn drop_namespace_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.drop_namespace("users").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
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
fn flush_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.flush().unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
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
fn invalid_config_error_display() {
    let err = Error::InvalidConfig("unknown io_model 'bad'".into());
    assert_eq!(err.to_string(), "invalid config: unknown io_model 'bad'");
}
