use knot::Knot;

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

fn setup_graph(db: &rkv::DB) -> Knot {
    let mut knot = Knot::new(db, "test").unwrap();
    knot.create_table("person").unwrap();
    knot.create_table("school").unwrap();

    let person = knot.table("person").unwrap();
    person.insert_set("alice").unwrap();
    person.insert_set("bob").unwrap();
    person.insert_set("charlie").unwrap();

    let school = knot.table("school").unwrap();
    school.insert_set("mit").unwrap();

    knot.create_link("attends", "person", "school", false, false)
        .unwrap();
    knot.create_link("friends", "person", "person", true, false)
        .unwrap();

    let attends = knot.link("attends").unwrap();
    attends.insert_bare("alice", "mit").unwrap();
    attends.insert_bare("bob", "mit").unwrap();

    let friends = knot.link("friends").unwrap();
    friends.insert_bare("alice", "bob").unwrap();

    knot
}

#[test]
fn delete_cascade_off_cleans_links_only() {
    let (_dir, db) = temp_db();
    let knot = setup_graph(&db);

    let person = knot.table("person").unwrap();
    person.delete_cascade("alice", false).unwrap();

    // Alice is gone
    assert!(!person.exists("alice").unwrap());

    // Bob and mit still exist
    assert!(person.exists("bob").unwrap());
    let school = knot.table("school").unwrap();
    assert!(school.exists("mit").unwrap());

    // Alice's links are gone
    let attends = knot.link("attends").unwrap();
    assert!(attends.get("alice", "mit").unwrap().is_none());

    let friends = knot.link("friends").unwrap();
    assert!(friends.get("alice", "bob").unwrap().is_none());

    // Bob's link to mit still exists
    assert!(attends.get("bob", "mit").unwrap().is_some());
}

#[test]
fn delete_cascade_on_propagates() {
    let (_dir, db) = temp_db();
    let knot = setup_graph(&db);

    let person = knot.table("person").unwrap();
    person.delete_cascade("alice", true).unwrap();

    // Alice is gone
    assert!(!person.exists("alice").unwrap());

    // Cascade through attends → mit should be deleted
    let school = knot.table("school").unwrap();
    assert!(!school.exists("mit").unwrap());

    // Cascade through friends → bob should be deleted
    assert!(!person.exists("bob").unwrap());

    // Charlie should survive (not connected to alice)
    assert!(person.exists("charlie").unwrap());
}

#[test]
fn schema_level_cascade() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();
    knot.create_table("school").unwrap();

    let person = knot.table("person").unwrap();
    person.insert_set("alice").unwrap();
    let school = knot.table("school").unwrap();
    school.insert_set("mit").unwrap();

    // Create link with cascade=true at schema level
    knot.create_link("attends", "person", "school", false, true)
        .unwrap();
    let attends = knot.link("attends").unwrap();
    attends.insert_bare("alice", "mit").unwrap();

    // Delete with cascade=false — but schema-level cascade overrides
    let person = knot.table("person").unwrap();
    person.delete_cascade("alice", false).unwrap();

    assert!(!person.exists("alice").unwrap());
    // mit should be deleted because link has schema-level cascade
    let school = knot.table("school").unwrap();
    assert!(!school.exists("mit").unwrap());
}

#[test]
fn cascade_cycle_detection() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();

    let person = knot.table("person").unwrap();
    person.insert_set("alice").unwrap();
    person.insert_set("bob").unwrap();

    // Bidirectional friends — potential cycle
    knot.create_link("friends", "person", "person", true, false)
        .unwrap();
    let friends = knot.link("friends").unwrap();
    friends.insert_bare("alice", "bob").unwrap();
    friends.insert_bare("bob", "alice").unwrap();

    // Cascade delete alice — should not infinite loop
    let person = knot.table("person").unwrap();
    person.delete_cascade("alice", true).unwrap();

    assert!(!person.exists("alice").unwrap());
    assert!(!person.exists("bob").unwrap());
}

#[test]
fn delete_nonexistent_node_cascade_is_noop() {
    let (_dir, db) = temp_db();
    let knot = setup_graph(&db);

    let person = knot.table("person").unwrap();
    person.delete_cascade("nobody", false).unwrap(); // no error
}

#[test]
fn delete_cascade_cleans_reverse_index() {
    let (_dir, db) = temp_db();
    let knot = setup_graph(&db);

    let person = knot.table("person").unwrap();
    person.delete_cascade("alice", false).unwrap();

    // Reverse lookup on mit should not show alice
    let attends = knot.link("attends").unwrap();
    let to_mit = attends.to("mit").unwrap();
    let sources: Vec<&str> = to_mit.iter().map(|e| e.from.as_str()).collect();
    assert!(!sources.contains(&"alice"));
    assert!(sources.contains(&"bob"));
}
