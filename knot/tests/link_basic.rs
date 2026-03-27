use knot::{Knot, Properties, PropertyValue};

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

fn setup_campus(db: &rkv::DB) -> Knot {
    let mut knot = Knot::new(db, "test").unwrap();
    knot.create_table("person").unwrap();
    knot.create_table("school").unwrap();

    // Insert nodes
    let person = knot.table("person").unwrap();
    person.insert_set("alice").unwrap();
    person.insert_set("bob").unwrap();

    let school = knot.table("school").unwrap();
    school.insert_set("mit").unwrap();
    school.insert_set("stanford").unwrap();

    knot
}

#[test]
fn create_and_list_links() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);

    knot.create_link("attends", "person", "school", false, false)
        .unwrap();
    knot.create_link("friends", "person", "person", true, false)
        .unwrap();

    let mut links = knot.links();
    links.sort();
    assert_eq!(links, vec!["attends", "friends"]);
}

#[test]
fn create_link_duplicate_error() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);

    knot.create_link("attends", "person", "school", false, false)
        .unwrap();
    let err = knot
        .create_link("attends", "person", "school", false, false)
        .unwrap_err();
    assert!(matches!(err, knot::Error::LinkTableExists(_)));
}

#[test]
fn create_link_if_not_exists() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);

    knot.create_link_if_not_exists("attends", "person", "school", false, false)
        .unwrap();
    knot.create_link_if_not_exists("attends", "person", "school", false, false)
        .unwrap(); // no error
    assert_eq!(knot.links().len(), 1);
}

#[test]
fn create_link_missing_table_error() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);

    let err = knot
        .create_link("attends", "person", "nonexistent", false, false)
        .unwrap_err();
    assert!(matches!(err, knot::Error::TableNotFound(_)));
}

#[test]
fn drop_link() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);

    knot.create_link("attends", "person", "school", false, false)
        .unwrap();
    knot.drop_link("attends").unwrap();
    assert!(knot.links().is_empty());
}

#[test]
fn insert_and_get_link() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();

    let mut props = Properties::new();
    props.insert("year".into(), PropertyValue::Integer(2020));
    link.insert("alice", "mit", &props).unwrap();

    let entry = link.get("alice", "mit").unwrap().unwrap();
    assert_eq!(entry.from, "alice");
    assert_eq!(entry.to, "mit");
    let p = entry.properties.unwrap();
    assert_eq!(p.get("year"), Some(&PropertyValue::Integer(2020)));
}

#[test]
fn insert_bare_link() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    link.insert_bare("alice", "mit").unwrap();

    let entry = link.get("alice", "mit").unwrap().unwrap();
    assert!(entry.properties.is_none());
}

#[test]
fn link_endpoint_validation() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    let err = link.insert_bare("nonexistent", "mit").unwrap_err();
    assert!(matches!(err, knot::Error::EndpointNotFound(_)));
}

#[test]
fn delete_link() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    link.insert_bare("alice", "mit").unwrap();
    link.delete("alice", "mit").unwrap();

    assert!(link.get("alice", "mit").unwrap().is_none());
}

#[test]
fn delete_nonexistent_link_is_noop() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    link.delete("alice", "mit").unwrap(); // no error
}

#[test]
fn scan_outgoing_links() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    link.insert_bare("alice", "mit").unwrap();
    link.insert_bare("alice", "stanford").unwrap();
    link.insert_bare("bob", "mit").unwrap();

    let from_alice = link.from("alice").unwrap();
    assert_eq!(from_alice.len(), 2);
    let targets: Vec<&str> = from_alice.iter().map(|e| e.to.as_str()).collect();
    assert!(targets.contains(&"mit"));
    assert!(targets.contains(&"stanford"));
}

#[test]
fn reverse_lookup() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();
    link.insert_bare("alice", "mit").unwrap();
    link.insert_bare("bob", "mit").unwrap();

    let to_mit = link.to("mit").unwrap();
    assert_eq!(to_mit.len(), 2);
    let sources: Vec<&str> = to_mit.iter().map(|e| e.from.as_str()).collect();
    assert!(sources.contains(&"alice"));
    assert!(sources.contains(&"bob"));
}

#[test]
fn upsert_overwrites_link_properties() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    let link = knot.link("attends").unwrap();

    let mut props1 = Properties::new();
    props1.insert("year".into(), PropertyValue::Integer(2020));
    link.insert("alice", "mit", &props1).unwrap();

    let mut props2 = Properties::new();
    props2.insert("year".into(), PropertyValue::Integer(2023));
    link.insert("alice", "mit", &props2).unwrap();

    let entry = link.get("alice", "mit").unwrap().unwrap();
    let p = entry.properties.unwrap();
    assert_eq!(p.get("year"), Some(&PropertyValue::Integer(2023)));
}

#[test]
fn self_referential_link() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("friends", "person", "person", true, false)
        .unwrap();

    let link = knot.link("friends").unwrap();
    link.insert_bare("alice", "bob").unwrap();

    let from_alice = link.from("alice").unwrap();
    assert_eq!(from_alice.len(), 1);
    assert_eq!(from_alice[0].to, "bob");

    // Reverse lookup also works
    let to_alice = link.to("alice").unwrap();
    assert!(to_alice.is_empty()); // alice is source, not target

    let to_bob = link.to("bob").unwrap();
    assert_eq!(to_bob.len(), 1);
    assert_eq!(to_bob[0].from, "alice");
}

#[test]
fn alter_link_flags() {
    let (_dir, db) = temp_db();
    let mut knot = setup_campus(&db);
    knot.create_link("attends", "person", "school", false, false)
        .unwrap();

    knot.alter_link("attends", Some(true), Some(true)).unwrap();

    // Verify by reopening
    let knot2 = Knot::new(&db, "test").unwrap();
    let link = knot2.link("attends").unwrap();
    // If it was altered, the link handle should exist
    // (deeper flag verification would need metadata access)
    drop(link);
}
