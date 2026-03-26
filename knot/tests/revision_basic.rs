use knot::{Knot, Properties, PropertyValue};

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

#[test]
fn rev_count_after_writes() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    // Insert creates first revision
    let mut props = Properties::new();
    props.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props).unwrap();
    assert_eq!(table.rev_count("alice").unwrap(), 1);

    // Replace creates second revision
    props.insert("age".into(), PropertyValue::Integer(31));
    table.replace("alice", &props).unwrap();
    assert_eq!(table.rev_count("alice").unwrap(), 2);

    // Update creates third revision
    let mut changes = Properties::new();
    changes.insert("age".into(), PropertyValue::Integer(32));
    table.update("alice", &changes).unwrap();
    assert_eq!(table.rev_count("alice").unwrap(), 3);
}

#[test]
fn history_returns_all_revisions() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props = Properties::new();
    props.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props).unwrap();

    props.insert("age".into(), PropertyValue::Integer(31));
    table.replace("alice", &props).unwrap();

    let history = table.history("alice").unwrap();
    assert_eq!(history.len(), 2);
}

#[test]
fn at_revision_returns_specific() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props = Properties::new();
    props.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props).unwrap();

    props.insert("age".into(), PropertyValue::Integer(31));
    table.replace("alice", &props).unwrap();

    // First revision (index 0) should have age=30
    let rev0 = table.at_revision("alice", 0).unwrap().unwrap();
    let p = rev0.properties.unwrap();
    assert_eq!(p.get("age"), Some(&PropertyValue::Integer(30)));

    // Second revision (index 1) should have age=31
    let rev1 = table.at_revision("alice", 1).unwrap().unwrap();
    let p = rev1.properties.unwrap();
    assert_eq!(p.get("age"), Some(&PropertyValue::Integer(31)));
}

#[test]
fn at_revision_out_of_range_returns_none() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    table.insert_set("alice").unwrap();

    assert!(table.at_revision("alice", 99).unwrap().is_none());
}

#[test]
fn revision_has_timestamp() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    table.insert_set("alice").unwrap();

    let history = table.history("alice").unwrap();
    assert_eq!(history.len(), 1);
    // Timestamp should be non-zero (a real timestamp)
    assert!(history[0].timestamp_ms > 0);
}

#[test]
fn set_mode_revision() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("tags").unwrap();

    let table = knot.table("tags").unwrap();
    table.insert_set("important").unwrap();

    let history = table.history("important").unwrap();
    assert_eq!(history.len(), 1);
    assert!(history[0].properties.is_none());
}

#[test]
fn rev_count_nonexistent_key() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    assert_eq!(table.rev_count("nobody").unwrap(), 0);
}
