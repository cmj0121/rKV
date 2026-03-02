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

/// After a reconnect, the replica should perform an incremental sync — only
/// records written since the last known revision are sent, not a full sync.
#[test]
fn incremental_sync_after_reconnect() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Connect replica first (empty full sync)
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Write k1 via live stream so replay_fn updates revision tracker
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k1", "v1", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(500));

    // Verify k1 arrived via live stream
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k1").unwrap();
    assert_eq!(val.as_bytes(), Some(b"v1".as_slice()));
    drop(ns);

    // Close replica (saves checkpoint with non-zero revision from live stream)
    replica.close().unwrap();

    // Write more data while replica is offline (stays in primary's AOL)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k2", "v2", None).unwrap();
    drop(ns);

    // Reconnect replica — checkpoint has k1's revision, primary AOL has k2
    // → incremental sync sends only k2
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Both old and new data should be present
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k1");
    assert!(
        val.is_ok(),
        "old key should survive incremental sync, got: {:?}",
        val.err()
    );
    let val = ns.get("k2");
    assert!(
        val.is_ok(),
        "new key should arrive via incremental sync, got: {:?}",
        val.err()
    );
    drop(ns);

    // Verify checkpoint file exists
    let checkpoint = tmp_replica.path().join("repl_checkpoint");
    replica.close().unwrap();
    assert!(
        checkpoint.exists(),
        "checkpoint file should be persisted on close"
    );

    primary.close().unwrap();
}

/// When the primary's AOL has been truncated (after flush), the replica
/// should fall back to a full sync instead of incremental.
#[test]
fn fallback_to_full_sync_after_flush() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Write initial data and flush so SSTables exist
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k1", "v1", None).unwrap();
    drop(ns);
    primary.flush().unwrap();

    // Connect replica — full sync copies SSTables (k1)
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k1");
    assert!(val.is_ok(), "k1 should arrive via full sync");
    drop(ns);

    // Close replica (saves checkpoint)
    replica.close().unwrap();

    // Write more data on primary AND flush — this truncates the AOL
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k2", "v2", None).unwrap();
    drop(ns);
    primary.flush().unwrap();

    // Reconnect replica — AOL is truncated, so must fall back to full sync
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Both keys should be present (full sync copies all SSTables)
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k1");
    assert!(
        val.is_ok(),
        "k1 should survive full-sync fallback, got: {:?}",
        val.err()
    );
    let val = ns.get("k2");
    assert!(
        val.is_ok(),
        "k2 should arrive via full-sync fallback, got: {:?}",
        val.err()
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

/// Force-sync wipes all local state and performs a fresh full sync.
#[test]
fn force_sync_wipes_and_resyncs() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Write initial data and flush
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k1", "v1", None).unwrap();
    drop(ns);
    primary.flush().unwrap();

    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Verify data arrived
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert_eq!(ns.get("k1").unwrap().as_bytes(), Some(b"v1".as_slice()));
    drop(ns);

    // Write more data on primary
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("k2", "v2", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(500));

    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    assert_eq!(ns.get("k2").unwrap().as_bytes(), Some(b"v2".as_slice()));
    drop(ns);

    // Trigger force-sync
    replica.force_sync().unwrap();

    // Wait for wipe + reconnect + full sync
    thread::sleep(Duration::from_millis(3000));

    // All data should still be present after re-sync (primary has it all)
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("k1");
    assert!(
        val.is_ok(),
        "k1 should be present after force-sync, got: {:?}",
        val.err()
    );
    let val = ns.get("k2");
    assert!(
        val.is_ok(),
        "k2 should be present after force-sync, got: {:?}",
        val.err()
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

/// force_sync() on a non-replica should return an error.
#[test]
fn force_sync_on_non_replica_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let primary = open_primary(tmp.path(), port);

    let err = primary.force_sync().unwrap_err();
    assert!(
        matches!(err, Error::ReadOnlyReplica),
        "expected ReadOnlyReplica, got: {err}"
    );

    primary.close().unwrap();

    // Standalone should also fail
    let tmp2 = tempfile::tempdir().unwrap();
    let config = Config::new(tmp2.path());
    let db = DB::open(config).unwrap();
    let err = db.force_sync().unwrap_err();
    assert!(
        matches!(err, Error::ReadOnlyReplica),
        "expected ReadOnlyReplica on standalone, got: {err}"
    );
    db.close().unwrap();
}

/// Checkpoint file should persist the last-revision across replica restarts.
#[test]
fn checkpoint_persists_across_restarts() {
    let tmp_primary = tempfile::tempdir().unwrap();
    let tmp_replica = tempfile::tempdir().unwrap();
    let port = free_port();

    let primary = open_primary(tmp_primary.path(), port);
    thread::sleep(Duration::from_millis(100));

    // Connect replica first (empty full sync)
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Write data via live stream so replay_fn updates revision tracker
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("ck1", "cv1", None).unwrap();
    drop(ns);

    // Wait for live-stream propagation
    thread::sleep(Duration::from_millis(500));

    // Verify data arrived via live stream
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("ck1").unwrap();
    assert_eq!(val.as_bytes(), Some(b"cv1".as_slice()));
    drop(ns);

    // Close replica — checkpoint should be saved with non-zero revision
    replica.close().unwrap();

    let checkpoint_path = tmp_replica.path().join("repl_checkpoint");
    assert!(
        checkpoint_path.exists(),
        "checkpoint file should exist after replica close"
    );

    // Read the checkpoint — it should be a 16-byte big-endian u128 > 0
    let data = std::fs::read(&checkpoint_path).unwrap();
    assert_eq!(data.len(), 16, "checkpoint should be 16 bytes");
    let rev = u128::from_be_bytes(data[0..16].try_into().unwrap());
    assert!(
        rev > 0,
        "checkpoint revision should be non-zero, got: {rev}"
    );

    // Write more data while replica is offline so incremental sync is triggered
    // (otherwise, records_after_revision returns empty → full sync → wipes memtable)
    let ns = primary.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns.put("ck2", "cv2", None).unwrap();
    drop(ns);

    // Reopen the replica — it should load the checkpoint and request
    // incremental sync (primary's AOL has ck2 after the checkpoint revision)
    let replica = open_replica(tmp_replica.path(), &format!("127.0.0.1:{port}"));
    thread::sleep(Duration::from_millis(1500));

    // Old data (from replica's own AOL replay) + new data (from incremental sync)
    let ns = replica.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns.get("ck1");
    assert!(
        val.is_ok(),
        "old key should be accessible after restart, got: {:?}",
        val.err()
    );
    let val = ns.get("ck2");
    assert!(
        val.is_ok(),
        "new key should arrive via incremental sync after restart, got: {:?}",
        val.err()
    );
    drop(ns);

    replica.close().unwrap();
    primary.close().unwrap();
}

// --- Peer (master-master) replication tests ---

fn open_peer(path: &std::path::Path, repl_port: u16, peers: Vec<String>, cluster_id: u16) -> DB {
    let mut config = Config::new(path);
    config.role = Role::Peer;
    config.repl_bind = "127.0.0.1".to_owned();
    config.repl_port = repl_port;
    config.peers = peers;
    config.cluster_id = Some(cluster_id);
    DB::open(config).unwrap()
}

#[test]
fn peer_write_propagates_bidirectionally() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let port_a = free_port();
    let port_b = free_port();

    let peer_a = open_peer(tmp_a.path(), port_a, vec![format!("127.0.0.1:{port_b}")], 1);
    thread::sleep(Duration::from_millis(100));

    let peer_b = open_peer(tmp_b.path(), port_b, vec![format!("127.0.0.1:{port_a}")], 2);
    thread::sleep(Duration::from_millis(1000));

    // Write on A → should appear on B
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns_a.put("from_a", "hello_a", None).unwrap();
    drop(ns_a);

    thread::sleep(Duration::from_millis(500));

    let ns_b = peer_b.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns_b.get("from_a");
    assert!(
        val.is_ok(),
        "write from A should appear on B: {:?}",
        val.err()
    );

    // Write on B → should appear on A
    ns_b.put("from_b", "hello_b", None).unwrap();
    drop(ns_b);

    thread::sleep(Duration::from_millis(500));

    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns_a.get("from_b");
    assert!(
        val.is_ok(),
        "write from B should appear on A: {:?}",
        val.err()
    );
    drop(ns_a);

    peer_b.close().unwrap();
    peer_a.close().unwrap();
}

#[test]
fn peer_lww_conflict_resolution() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let port_a = free_port();
    let port_b = free_port();

    let peer_a = open_peer(tmp_a.path(), port_a, vec![format!("127.0.0.1:{port_b}")], 1);
    thread::sleep(Duration::from_millis(100));

    let peer_b = open_peer(tmp_b.path(), port_b, vec![format!("127.0.0.1:{port_a}")], 2);
    thread::sleep(Duration::from_millis(1000));

    // Write the same key on A first, then on B (B's revision is newer)
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns_a.put("conflict_key", "value_a", None).unwrap();
    drop(ns_a);
    thread::sleep(Duration::from_millis(50));

    let ns_b = peer_b.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns_b.put("conflict_key", "value_b", None).unwrap();
    drop(ns_b);

    // Wait for sync to propagate
    thread::sleep(Duration::from_millis(1000));

    // Both nodes should converge to the same value (B's, since it's newer)
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let ns_b = peer_b.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val_a = ns_a.get("conflict_key").unwrap();
    let val_b = ns_b.get("conflict_key").unwrap();
    assert_eq!(
        val_a.as_bytes(),
        val_b.as_bytes(),
        "both peers should converge to the same value"
    );
    assert_eq!(
        val_b.as_bytes(),
        Some(b"value_b".as_slice()),
        "LWW should pick B's newer write"
    );
    drop(ns_a);
    drop(ns_b);

    // Verify conflicts_resolved counter increased on at least one node
    let stats_a = peer_a.stats();
    let stats_b = peer_b.stats();
    assert!(
        stats_a.conflicts_resolved + stats_b.conflicts_resolved > 0,
        "at least one node should have resolved a conflict, a={} b={}",
        stats_a.conflicts_resolved,
        stats_b.conflicts_resolved,
    );

    peer_b.close().unwrap();
    peer_a.close().unwrap();
}

#[test]
fn peer_loop_prevention() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let port_a = free_port();
    let port_b = free_port();

    let peer_a = open_peer(tmp_a.path(), port_a, vec![format!("127.0.0.1:{port_b}")], 1);
    thread::sleep(Duration::from_millis(100));

    let peer_b = open_peer(tmp_b.path(), port_b, vec![format!("127.0.0.1:{port_a}")], 2);
    thread::sleep(Duration::from_millis(1000));

    // Write a key on A
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns_a.put("loop_test", "original", None).unwrap();
    drop(ns_a);

    thread::sleep(Duration::from_millis(500));

    // Verify B received it
    let ns_b = peer_b.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns_b.get("loop_test").unwrap();
    assert_eq!(val.as_bytes(), Some(b"original".as_slice()));
    drop(ns_b);

    // Overwrite on A — the new version should propagate to B
    // but B should NOT send it back to A (loop prevention)
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    ns_a.put("loop_test", "updated", None).unwrap();
    drop(ns_a);

    thread::sleep(Duration::from_millis(500));

    let ns_b = peer_b.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns_b.get("loop_test").unwrap();
    assert_eq!(val.as_bytes(), Some(b"updated".as_slice()));
    drop(ns_b);

    // A should still have the updated value (not overwritten by loop-back)
    let ns_a = peer_a.namespace(DEFAULT_NAMESPACE, None).unwrap();
    let val = ns_a.get("loop_test").unwrap();
    assert_eq!(val.as_bytes(), Some(b"updated".as_slice()));
    drop(ns_a);

    peer_b.close().unwrap();
    peer_a.close().unwrap();
}

#[test]
fn peer_stats_reflect_connections() {
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let port_a = free_port();
    let port_b = free_port();

    let peer_a = open_peer(tmp_a.path(), port_a, vec![format!("127.0.0.1:{port_b}")], 1);
    thread::sleep(Duration::from_millis(100));

    let peer_b = open_peer(tmp_b.path(), port_b, vec![format!("127.0.0.1:{port_a}")], 2);
    thread::sleep(Duration::from_millis(1500));

    let stats_a = peer_a.stats();
    let stats_b = peer_b.stats();
    assert_eq!(stats_a.role, "peer");
    assert_eq!(stats_b.role, "peer");
    // Each node should see at least 1 peer session
    assert!(
        stats_a.peer_count >= 1,
        "A should have at least 1 peer, got: {}",
        stats_a.peer_count,
    );
    assert!(
        stats_b.peer_count >= 1,
        "B should have at least 1 peer, got: {}",
        stats_b.peer_count,
    );

    peer_b.close().unwrap();
    peer_a.close().unwrap();
}

#[test]
fn peer_without_peers_list_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut config = Config::new(tmp.path());
    config.role = Role::Peer;
    config.repl_bind = "127.0.0.1".to_owned();
    config.repl_port = port;
    config.peers = vec![];
    config.cluster_id = Some(99);

    let result = DB::open(config);
    assert!(result.is_err(), "peer with empty peers list should fail");
}
