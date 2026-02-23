use rkv::{Config, Error, DB};

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
fn put_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.put("key", "value").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}

#[test]
fn get_returns_not_implemented() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config::new(tmp.path());
    let db = DB::open(config).unwrap();

    let err = db.get("key").unwrap_err();
    assert!(matches!(err, Error::NotImplemented(_)));
}
