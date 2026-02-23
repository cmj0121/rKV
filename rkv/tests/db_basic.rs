use std::time::Duration;

use rkv::{Config, Error, DB, DEFAULT_NAMESPACE};

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

    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();
    assert_eq!(ns.name(), "_");
}

#[test]
fn namespace_custom() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let ns = db.namespace("users").unwrap();
    assert_eq!(ns.name(), "users");
}

#[test]
fn namespace_empty_name_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.namespace("").unwrap_err();
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

#[test]
fn put_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns.put("key", "value").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn get_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns.get("key").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn rev_count_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns.rev_count("key").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn rev_get_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns.rev_get("key", 0).unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn put_with_ttl_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns
        .put_with_ttl("key", "value", Duration::from_secs(60))
        .unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn ttl_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();
    let ns = db.namespace(DEFAULT_NAMESPACE).unwrap();

    let err = ns.ttl("key").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}
