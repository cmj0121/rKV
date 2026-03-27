use knot::{Knot, Properties, PropertyValue};

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

#[test]
fn create_and_list_tables() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    knot.create_table("person").unwrap();
    knot.create_table("school").unwrap();

    let mut tables = knot.tables();
    tables.sort();
    assert_eq!(tables, vec!["person", "school"]);
}

#[test]
fn create_table_duplicate_error() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    knot.create_table("person").unwrap();
    let err = knot.create_table("person").unwrap_err();
    assert!(matches!(err, knot::Error::TableExists(_)));
}

#[test]
fn create_table_if_not_exists() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    knot.create_table_if_not_exists("person").unwrap();
    knot.create_table_if_not_exists("person").unwrap(); // no error
    assert_eq!(knot.tables().len(), 1);
}

#[test]
fn drop_table() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    knot.create_table("person").unwrap();
    knot.drop_table("person").unwrap();
    assert!(knot.tables().is_empty());
}

#[test]
fn drop_nonexistent_table_error() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    let err = knot.drop_table("nope").unwrap_err();
    assert!(matches!(err, knot::Error::TableNotFound(_)));
}

#[test]
fn insert_and_get_node() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props = Properties::new();
    props.insert("name".into(), PropertyValue::String("Alice".into()));
    props.insert("age".into(), PropertyValue::Integer(30));

    table.insert("alice", &props).unwrap();

    let node = table.get("alice").unwrap().unwrap();
    assert_eq!(node.key, "alice");
    let p = node.properties.unwrap();
    assert_eq!(p.get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(p.get("age"), Some(&PropertyValue::Integer(30)));
}

#[test]
fn insert_set_mode() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("tags").unwrap();

    let table = knot.table("tags").unwrap();
    table.insert_set("important").unwrap();

    let node = table.get("important").unwrap().unwrap();
    assert_eq!(node.key, "important");
    assert!(node.properties.is_none());
}

#[test]
fn get_nonexistent_returns_none() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    assert!(table.get("nobody").unwrap().is_none());
}

#[test]
fn exists_check() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    table.insert_set("alice").unwrap();

    assert!(table.exists("alice").unwrap());
    assert!(!table.exists("bob").unwrap());
}

#[test]
fn replace_overwrites_all() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props1 = Properties::new();
    props1.insert("name".into(), PropertyValue::String("Alice".into()));
    props1.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props1).unwrap();

    let mut props2 = Properties::new();
    props2.insert("role".into(), PropertyValue::String("admin".into()));
    table.replace("alice", &props2).unwrap();

    let node = table.get("alice").unwrap().unwrap();
    let p = node.properties.unwrap();
    assert_eq!(p.get("role"), Some(&PropertyValue::String("admin".into())));
    assert!(p.get("name").is_none()); // replaced — old props gone
    assert!(p.get("age").is_none());
}

#[test]
fn update_merges_properties() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props = Properties::new();
    props.insert("name".into(), PropertyValue::String("Alice".into()));
    props.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props).unwrap();

    let mut changes = Properties::new();
    changes.insert("age".into(), PropertyValue::Integer(31));
    table.update("alice", &changes).unwrap();

    let node = table.get("alice").unwrap().unwrap();
    let p = node.properties.unwrap();
    assert_eq!(p.get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(p.get("age"), Some(&PropertyValue::Integer(31)));
}

#[test]
fn update_with_null_removes_property() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut props = Properties::new();
    props.insert("name".into(), PropertyValue::String("Alice".into()));
    props.insert("age".into(), PropertyValue::Integer(30));
    table.insert("alice", &props).unwrap();

    let mut changes = std::collections::HashMap::new();
    changes.insert("age".into(), None); // remove age
    table.update_with_nulls("alice", &changes).unwrap();

    let node = table.get("alice").unwrap().unwrap();
    let p = node.properties.unwrap();
    assert_eq!(p.get("name"), Some(&PropertyValue::String("Alice".into())));
    assert!(p.get("age").is_none());
}

#[test]
fn delete_node() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    table.insert_set("alice").unwrap();
    assert!(table.exists("alice").unwrap());

    table.delete("alice").unwrap();
    assert!(!table.exists("alice").unwrap());
}

#[test]
fn delete_nonexistent_is_noop() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    table.delete("nobody").unwrap(); // no error
}

#[test]
fn invalid_key_rejected() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();
    let err = table.insert_set("").unwrap_err();
    assert!(matches!(err, knot::Error::InvalidKey(_)));
}

#[test]
fn invalid_table_name_rejected() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();

    let err = knot.create_table("has.dot").unwrap_err();
    assert!(matches!(err, knot::Error::InvalidName(_)));
}
