use knot::{Condition, Knot, Properties, PropertyValue};

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

fn setup_campus(db: &rkv::DB) -> Knot<'_> {
    let mut knot = Knot::new(db, "test").unwrap();
    knot.create_table("person").unwrap();
    knot.create_table("school").unwrap();
    knot.create_table("city").unwrap();

    let person = knot.table("person").unwrap();
    let mut p = Properties::new();
    p.insert("role".into(), PropertyValue::String("teacher".into()));
    person.insert("alice", &p).unwrap();
    person.insert_set("bob").unwrap();

    let school = knot.table("school").unwrap();
    let mut p = Properties::new();
    p.insert("ranking".into(), PropertyValue::Integer(1));
    school.insert("mit", &p).unwrap();
    school.insert_set("stanford").unwrap();

    let city = knot.table("city").unwrap();
    city.insert_set("cambridge").unwrap();

    knot.create_link("attends", "person", "school", false, false)
        .unwrap();
    knot.create_link("located-in", "school", "city", false, false)
        .unwrap();
    knot.create_link("friends", "person", "person", true, false)
        .unwrap();

    let attends = knot.link("attends").unwrap();
    let mut p = Properties::new();
    p.insert("year".into(), PropertyValue::Integer(2020));
    attends.insert("alice", "mit", &p).unwrap();
    attends.insert_bare("bob", "stanford").unwrap();

    let located = knot.link("located-in").unwrap();
    located.insert_bare("mit", "cambridge").unwrap();

    let friends = knot.link("friends").unwrap();
    friends.insert_bare("alice", "bob").unwrap();

    knot
}

#[test]
fn directed_one_hop() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    let result = knot
        .traverse("person", "alice", &["attends"], None, None, false)
        .unwrap();

    assert_eq!(result.leaves.len(), 1);
    assert_eq!(result.leaves[0], ("school".to_owned(), "mit".to_owned()));
    assert!(result.paths.is_none());
}

#[test]
fn directed_multi_hop() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    let result = knot
        .traverse(
            "person",
            "alice",
            &["attends", "located-in"],
            None,
            None,
            false,
        )
        .unwrap();

    assert_eq!(result.leaves.len(), 1);
    assert_eq!(
        result.leaves[0],
        ("city".to_owned(), "cambridge".to_owned())
    );
}

#[test]
fn directed_with_paths() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    let result = knot
        .traverse(
            "person",
            "alice",
            &["attends", "located-in"],
            None,
            None,
            true,
        )
        .unwrap();

    let paths = result.paths.unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0],
        vec![
            ("person".to_owned(), "alice".to_owned()),
            ("school".to_owned(), "mit".to_owned()),
            ("city".to_owned(), "cambridge".to_owned()),
        ]
    );
}

#[test]
fn directed_with_link_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    // Filter: only follow attends where year > 2019
    let filter = Condition::gt("year", PropertyValue::Integer(2019));
    let result = knot
        .traverse("person", "alice", &["attends"], Some(&filter), None, false)
        .unwrap();

    assert_eq!(result.leaves.len(), 1); // year=2020 > 2019

    // Filter: year > 2021 — should find nothing
    let filter = Condition::gt("year", PropertyValue::Integer(2021));
    let result = knot
        .traverse("person", "alice", &["attends"], Some(&filter), None, false)
        .unwrap();

    assert!(result.leaves.is_empty());
}

#[test]
fn directed_with_node_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    // Filter: only reach schools with ranking < 5
    let filter = Condition::lt("ranking", PropertyValue::Integer(5));
    let result = knot
        .traverse("person", "alice", &["attends"], None, Some(&filter), false)
        .unwrap();

    assert_eq!(result.leaves.len(), 1); // mit has ranking=1

    // Filter: ranking > 100 — should find nothing
    let filter = Condition::gt("ranking", PropertyValue::Integer(100));
    let result = knot
        .traverse("person", "alice", &["attends"], None, Some(&filter), false)
        .unwrap();

    assert!(result.leaves.is_empty());
}

#[test]
fn directed_cycle_detection() {
    let (_dir, db) = temp_db();
    let mut knot = Knot::new(&db, "test").unwrap();
    knot.create_table("person").unwrap();
    let person = knot.table("person").unwrap();
    person.insert_set("a").unwrap();
    person.insert_set("b").unwrap();

    knot.create_link("knows", "person", "person", false, false)
        .unwrap();
    let knows = knot.link("knows").unwrap();
    knows.insert_bare("a", "b").unwrap();
    knows.insert_bare("b", "a").unwrap();

    let result = knot
        .traverse("person", "a", &["knows", "knows"], None, None, false)
        .unwrap();

    // b→a should be filtered by cycle detection (a already visited)
    assert!(result.leaves.is_empty());
}

#[test]
fn discovery_basic() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    let result = knot.discover("person", "alice", 2, false).unwrap();

    // Hop 1: alice → mit (attends), alice → bob (friends, forward only since bidi=false)
    // Hop 2: mit → cambridge (located-in), bob → stanford (attends)
    let keys: Vec<&str> = result.leaves.iter().map(|(_, k)| k.as_str()).collect();
    assert!(keys.contains(&"mit"));
    assert!(keys.contains(&"bob"));
    assert!(keys.contains(&"cambridge") || keys.contains(&"stanford"));
}

#[test]
fn discovery_max_hops() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    let result = knot.discover("person", "alice", 1, false).unwrap();

    // Only 1 hop: should reach mit and bob, not cambridge
    let keys: Vec<&str> = result.leaves.iter().map(|(_, k)| k.as_str()).collect();
    assert!(keys.contains(&"mit"));
    assert!(keys.contains(&"bob"));
    assert!(!keys.contains(&"cambridge"));
}

#[test]
fn discovery_bidi() {
    let (_dir, db) = temp_db();
    let knot = setup_campus(&db);

    // With bidi=true, should also follow reverse of bidirectional links
    let result = knot.discover("person", "bob", 1, true).unwrap();

    let keys: Vec<&str> = result.leaves.iter().map(|(_, k)| k.as_str()).collect();
    // bob → stanford (attends forward)
    assert!(keys.contains(&"stanford"));
    // bob ← alice (friends, bidi reverse) — alice should appear
    assert!(keys.contains(&"alice"));
}
