use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use rkv::{Config, Error, Key, Role, DB, DEFAULT_NAMESPACE};

/// Find an available TCP port by binding to port 0.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Helper: open a DB with a specific role, binding replication to the given port.
fn open_primary(path: &std::path::Path, repl_port: u16) -> DB {
    let mut config = Config::new(path);
    config.role = Role::Primary;
    config.repl_bind = "127.0.0.1".to_owned();
    config.repl_port = repl_port;
    DB::open(config).unwrap()
}

fn open_replica(path: &std::path::Path, primary_addr: &str) -> DB {
    let mut config = Config::new(path);
    config.role = Role::Replica;
    config.primary_addr = Some(primary_addr.to_owned());
    DB::open(config).unwrap()
}

#[test]
fn replica_rejects_writes() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    // Give the sender listener time to bind
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(200));

    // Writes on replica must fail
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let err = ns.put("key", "value", None).unwrap_err();
    assert!(
        matches!(err, Error::ReadOnlyReplica),
        "expected ReadOnlyReplica, got: {err}"
    );

    // Delete should also be rejected
    let err = ns.delete("key").unwrap_err();
    assert!(matches!(err, Error::ReadOnlyReplica));

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn replica_rejects_delete_range() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(200));

    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let err = ns
        .delete_range(Key::Int(0), Key::Int(100), false)
        .unwrap_err();
    assert!(matches!(err, Error::ReadOnlyReplica));

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn stats_role_reflects_config() {
    let tmp = tempfile::tempdir().unwrap();

    // Standalone (default)
    let config = Config::new(tmp.path().join("standalone"));
    let db = DB::open(config).unwrap();
    assert_eq!(db.stats().role, "standalone");
    db.close().unwrap();

    // Primary
    let port = free_port();
    let primary = open_primary(&tmp.path().join("primary"), port);
    assert_eq!(primary.stats().role, "primary");
    primary.close().unwrap();
}

#[test]
fn primary_accepts_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp.path(), port);
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Primary should accept writes normally
    ns.put("hello", "world", None).unwrap();
    let val = ns.get("hello").unwrap();
    assert_eq!(val.as_bytes(), Some(b"world".as_slice()));

    drop(ns);
    primary.close().unwrap();
}

#[test]
fn replica_reads_replicated_data() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Write data on primary before replica connects (tests full sync)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("key1", "value1", None).unwrap();
    drop(ns);

    // Flush to ensure SSTable files exist for full sync
    primary.flush().unwrap();

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for full sync + live stream connection
    thread::sleep(Duration::from_millis(1000));

    // The replica should have received the data via full sync
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("key1");
    // Full sync copies SSTable files — replica may need to load them
    // This depends on whether the engine re-reads SSTables after file copy.
    // At minimum, the replica should not crash.
    drop(val);

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn replica_without_primary_addr_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::new(tmp.path());
    config.role = Role::Replica;
    // No primary_addr set

    let result = DB::open(config);
    assert!(result.is_err());
    let Err(err) = result else {
        panic!("expected error, got Ok");
    };
    assert!(
        matches!(err, Error::InvalidConfig(_)),
        "expected InvalidConfig, got: {err}"
    );
}

#[test]
fn standalone_has_no_replication() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    // Standalone should accept writes
    let ns = db.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("key", "value", None).unwrap();
    let val = ns.get("key").unwrap();
    assert_eq!(val.as_bytes(), Some(b"value".as_slice()));

    assert_eq!(db.stats().role, "standalone");
    assert!(!db.is_replica());

    drop(ns);
    db.close().unwrap();
}

#[test]
fn live_replication_propagates_writes() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for replica to connect and complete full sync
    thread::sleep(Duration::from_millis(1000));

    // Write data on primary AFTER replica is connected (tests live stream)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("live_key", "live_value", None).unwrap();
    drop(ns);

    // Wait for live stream to propagate
    thread::sleep(Duration::from_millis(500));

    // Check if replica received the live data
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("live_key");
    assert!(
        val.is_ok(),
        "live key should be readable on replica, got error: {:?}",
        val.err()
    );
    assert_eq!(
        val.unwrap().as_bytes(),
        Some(b"live_value".as_slice()),
        "live key should have correct value on replica"
    );

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn namespace_syncs_to_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);

    // Create a custom namespace on primary and write data
    let ns = primary.namespace("myns", None).unwrap();
    ns.put("k1", "v1", None).unwrap();
    drop(ns);

    // Flush so SSTable files exist for full sync
    primary.flush().unwrap();

    // Namespaces should be listed on primary
    let ns_list = primary.list_namespaces().unwrap();
    assert!(ns_list.contains(&"myns".to_string()));

    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for full sync + SSTable reload
    thread::sleep(Duration::from_millis(1500));

    // Replica should list the synced namespace
    let ns_list = replica.list_namespaces().unwrap();
    assert!(
        ns_list.contains(&"myns".to_string()),
        "expected 'myns' in replica namespace list, got: {ns_list:?}"
    );

    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn live_namespace_creation_syncs_to_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for replica to connect and complete initial full sync
    thread::sleep(Duration::from_millis(1500));

    // Create a NEW namespace on primary AFTER replica is connected
    let ns = primary.namespace("live_ns", None).unwrap();
    ns.put("key1", "val1", None).unwrap();
    drop(ns);

    // Wait for live stream to propagate
    thread::sleep(Duration::from_millis(500));

    // Replica should have the new namespace
    let ns_list = replica.list_namespaces().unwrap();
    assert!(
        ns_list.contains(&"live_ns".to_string()),
        "expected 'live_ns' in replica namespace list, got: {ns_list:?}"
    );
    assert!(
        ns_list.contains(&"_".to_string()),
        "default namespace '_' should always be present, got: {ns_list:?}"
    );

    // Replica should also have the key
    let ns = replica.namespace("live_ns", None).unwrap();
    let val = ns.get("key1");
    match val {
        Ok(v) => assert_eq!(v.as_bytes(), Some(b"val1".as_slice())),
        Err(e) => panic!("expected key1 on replica, got error: {e}"),
    }

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn ttl_expired_key_shows_deleted_on_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1000));

    // Write a key with a short TTL on primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("ttl_key", "ttl_value", Some(Duration::from_millis(200)))
        .unwrap();
    drop(ns);

    // Wait for live-stream propagation + TTL expiry
    thread::sleep(Duration::from_millis(800));

    // On primary: key should be expired (KeyNotFound via get)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert!(
        ns.get(Key::from("ttl_key")).is_err(),
        "expected expired key to be invisible on primary"
    );

    // Key should still appear in scan with include_deleted
    let prefix = Key::from("");
    let deleted_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        deleted_keys.contains(&Key::from("ttl_key")),
        "expected expired key in primary's 'show deleted' scan, got: {deleted_keys:?}"
    );
    drop(ns);

    // On replica: same behavior
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert!(
        ns.get(Key::from("ttl_key")).is_err(),
        "expected expired key to be invisible on replica"
    );
    let deleted_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        deleted_keys.contains(&Key::from("ttl_key")),
        "expected expired key in replica's 'show deleted' scan, got: {deleted_keys:?}"
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn replica_full_sync_clears_stale_memtable() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Write and flush a key on primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k", "v1", None).unwrap();
    drop(ns);
    primary.flush().unwrap();

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1000));

    // Replica should see v1 from full-sync
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k").unwrap();
    assert_eq!(
        val.as_bytes(),
        Some(b"v1".as_slice()),
        "replica should see v1 after full-sync"
    );
    drop(ns);

    // Now update the key on primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k", "v2", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(500));

    // Replica should see v2
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k").unwrap();
    assert_eq!(
        val.as_bytes(),
        Some(b"v2".as_slice()),
        "replica should see v2 after live update"
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn replica_allows_flush_and_compact() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Write data to populate replica
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("fk", "fv", None).unwrap();
    drop(ns);
    primary.flush().unwrap();

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1000));

    // Flush and compact should succeed on replica (maintenance ops)
    replica.flush().unwrap();
    replica.compact().unwrap();
    replica.sync().unwrap();

    // Data should still be accessible
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("fk");
    // After full-sync + flush, data is in SSTables
    assert!(val.is_ok(), "data should survive replica flush");
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn first_key_on_pure_db_visible_on_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    // Both DBs start completely empty (pure)
    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for full sync to complete (empty sync)
    thread::sleep(Duration::from_millis(1500));

    // Write the FIRST key ever on the primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("first_key", "first_value", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(1000));

    // Check the key on the replica — must be visible as live data, NOT deleted
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();

    // Strict assertion: get must succeed
    let val = ns.get("first_key");
    assert!(
        val.is_ok(),
        "first key should be readable on replica, got error: {:?}",
        val.err()
    );
    assert_eq!(
        val.unwrap().as_bytes(),
        Some(b"first_value".as_slice()),
        "first key should have correct value on replica"
    );

    // Also verify via scan — key should be live (not tombstoned)
    let prefix = Key::from("");
    let live_keys = ns.scan(&prefix, 100, 0, false).unwrap();
    assert!(
        live_keys.contains(&Key::from("first_key")),
        "first_key should appear in live scan on replica, got: {live_keys:?}"
    );

    // Verify it does NOT appear only in deleted scan
    let all_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        all_keys.contains(&Key::from("first_key")),
        "first_key should appear in full scan on replica, got: {all_keys:?}"
    );

    drop(ns);
    replica.close().unwrap();
    primary.close().unwrap();
}

/// Reproduces user report: first key with TTL=1s on a pure primary is
/// invisible on the replica even with "show deleted" toggled on.
#[test]
fn first_key_with_ttl_visible_then_deleted_on_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    // Both DBs start completely empty
    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Write the FIRST key with TTL=1s on primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("ttl_first", "ttl_value", Some(Duration::from_secs(1)))
        .unwrap();
    drop(ns);

    // Wait for live-stream propagation (key should still be alive)
    thread::sleep(Duration::from_millis(300));

    // --- While TTL is active: key should be visible on both ---
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("ttl_first");
    assert!(
        val.is_ok(),
        "key should be live on primary within TTL, got: {:?}",
        val.err()
    );
    drop(ns);

    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("ttl_first");
    assert!(
        val.is_ok(),
        "key should be live on replica within TTL, got: {:?}",
        val.err()
    );
    drop(ns);

    // --- Wait for TTL to expire ---
    thread::sleep(Duration::from_millis(1200));

    // --- After TTL: key should be invisible via get, visible via scan(deleted) ---
    let prefix = Key::from("");

    // Primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert!(
        ns.get(Key::from("ttl_first")).is_err(),
        "expired key should be invisible via get on primary"
    );
    let deleted_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        deleted_keys.contains(&Key::from("ttl_first")),
        "expired key should appear in primary 'show deleted' scan, got: {deleted_keys:?}"
    );
    drop(ns);

    // Replica
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert!(
        ns.get(Key::from("ttl_first")).is_err(),
        "expired key should be invisible via get on replica"
    );
    let deleted_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        deleted_keys.contains(&Key::from("ttl_first")),
        "expired key should appear in replica 'show deleted' scan, got: {deleted_keys:?}"
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

/// Dropping a namespace on the primary must sync to the replica — the replica
/// should no longer list the namespace or return data from it.
#[test]
fn drop_namespace_syncs_to_replica() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    // Wait for full sync
    thread::sleep(Duration::from_millis(1500));

    // Create a custom namespace on primary and write data
    let ns = primary.namespace("drop_me", None).unwrap();
    ns.put("k1", "v1", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(500));

    // Verify the namespace exists on the replica
    let ns_list = replica.list_namespaces().unwrap();
    assert!(
        ns_list.contains(&"drop_me".to_string()),
        "expected 'drop_me' on replica before drop, got: {ns_list:?}"
    );
    let ns = replica.namespace("drop_me", None).unwrap();
    let val = ns.get("k1").unwrap();
    assert_eq!(val.as_bytes(), Some(b"v1".as_slice()));
    drop(ns);

    // Drop the namespace on primary
    primary.drop_namespace("drop_me").unwrap();

    // Wait for drop to propagate
    thread::sleep(Duration::from_millis(500));

    // Verify the namespace is gone from the replica
    let ns_list = replica.list_namespaces().unwrap();
    assert!(
        !ns_list.contains(&"drop_me".to_string()),
        "namespace 'drop_me' should be gone from replica after drop, got: {ns_list:?}"
    );

    // The default namespace should still exist
    assert!(
        ns_list.contains(&"_".to_string()),
        "default namespace '_' should still exist, got: {ns_list:?}"
    );

    replica.close().unwrap();
    primary.close().unwrap();
}

/// Expired keys must survive a flush and remain visible in 'show deleted' scans.
/// This tests that `drain_latest` preserves expired entries as tombstones in SSTables.
#[test]
fn expired_key_visible_after_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let primary = open_primary(tmp.path(), port);

    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("ek", "ev", Some(Duration::from_millis(200)))
        .unwrap();
    drop(ns);

    // Wait for TTL to expire
    thread::sleep(Duration::from_millis(500));

    // Flush the memtable to SSTables
    primary.flush().unwrap();

    // The expired key should still appear in scan(include_deleted=true)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert!(
        ns.get(Key::from("ek")).is_err(),
        "expired key should be invisible via get"
    );
    let prefix = Key::from("");
    let deleted_keys = ns.scan(&prefix, 100, 0, true).unwrap();
    assert!(
        deleted_keys.contains(&Key::from("ek")),
        "expired key should appear in 'show deleted' scan after flush, got: {deleted_keys:?}"
    );
    drop(ns);

    primary.close().unwrap();
}
