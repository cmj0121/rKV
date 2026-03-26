use knot::{Condition, Knot, Properties, PropertyValue, Sort, SortOrder};

fn temp_db() -> (tempfile::TempDir, rkv::DB) {
    let dir = tempfile::tempdir().unwrap();
    let config = rkv::Config::new(dir.path());
    let db = rkv::DB::open(config).unwrap();
    (dir, db)
}

fn setup_people(db: &rkv::DB) -> Knot {
    let mut knot = Knot::new(db, "test").unwrap();
    knot.create_table("person").unwrap();

    let table = knot.table("person").unwrap();

    let mut p = Properties::new();
    p.insert("name".into(), PropertyValue::String("Alice".into()));
    p.insert("age".into(), PropertyValue::Integer(45));
    p.insert("role".into(), PropertyValue::String("teacher".into()));
    table.insert("alice", &p).unwrap();

    let mut p = Properties::new();
    p.insert("name".into(), PropertyValue::String("Bob".into()));
    p.insert("age".into(), PropertyValue::Integer(22));
    p.insert("role".into(), PropertyValue::String("student".into()));
    table.insert("bob", &p).unwrap();

    let mut p = Properties::new();
    p.insert("name".into(), PropertyValue::String("Charlie".into()));
    p.insert("age".into(), PropertyValue::Integer(35));
    p.insert("role".into(), PropertyValue::String("teacher".into()));
    table.insert("charlie", &p).unwrap();

    knot
}

#[test]
fn query_all() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let page = table.query(None, None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 3);
    assert!(!page.has_more);
}

#[test]
fn query_with_eq_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::eq("role", PropertyValue::String("teacher".into()));
    let page = table.query(Some(&cond), None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 2);
    let names: Vec<&str> = page.items.iter().map(|n| n.key.as_str()).collect();
    assert!(names.contains(&"alice"));
    assert!(names.contains(&"charlie"));
}

#[test]
fn query_with_gt_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::gt("age", PropertyValue::Integer(30));
    let page = table.query(Some(&cond), None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 2);
}

#[test]
fn query_with_and_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::and(vec![
        Condition::eq("role", PropertyValue::String("teacher".into())),
        Condition::gt("age", PropertyValue::Integer(40)),
    ]);
    let page = table.query(Some(&cond), None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].key, "alice");
}

#[test]
fn query_sorted_asc() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let sort = Sort {
        field: "age".into(),
        order: SortOrder::Asc,
    };
    let page = table.query(None, Some(&sort), None, 100, None).unwrap();
    let ages: Vec<i64> = page
        .items
        .iter()
        .map(
            |n| match n.properties.as_ref().unwrap().get("age").unwrap() {
                PropertyValue::Integer(i) => *i,
                _ => panic!("expected integer"),
            },
        )
        .collect();
    assert_eq!(ages, vec![22, 35, 45]);
}

#[test]
fn query_sorted_desc() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let sort = Sort {
        field: "age".into(),
        order: SortOrder::Desc,
    };
    let page = table.query(None, Some(&sort), None, 100, None).unwrap();
    let ages: Vec<i64> = page
        .items
        .iter()
        .map(
            |n| match n.properties.as_ref().unwrap().get("age").unwrap() {
                PropertyValue::Integer(i) => *i,
                _ => panic!("expected integer"),
            },
        )
        .collect();
    assert_eq!(ages, vec![45, 35, 22]);
}

#[test]
fn query_with_projection() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let fields = vec!["name".to_string()];
    let page = table.query(None, None, Some(&fields), 100, None).unwrap();
    for node in &page.items {
        let props = node.properties.as_ref().unwrap();
        assert!(props.contains_key("name"));
        assert!(!props.contains_key("age"));
        assert!(!props.contains_key("role"));
    }
}

#[test]
fn query_with_limit() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let page = table.query(None, None, None, 2, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(page.has_more);
    assert!(page.cursor.is_some());
}

#[test]
fn query_with_cursor() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let page1 = table.query(None, None, None, 2, None).unwrap();
    assert_eq!(page1.items.len(), 2);
    assert!(page1.has_more);

    let cursor = page1.cursor.as_deref();
    let page2 = table.query(None, None, None, 2, cursor).unwrap();
    assert_eq!(page2.items.len(), 1);
    assert!(!page2.has_more);
}

#[test]
fn count_all() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    assert_eq!(table.count(None).unwrap(), 3);
}

#[test]
fn count_with_filter() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::eq("role", PropertyValue::String("teacher".into()));
    assert_eq!(table.count(Some(&cond)).unwrap(), 2);
}

#[test]
fn query_like() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::like("name", "Al%");
    let page = table.query(Some(&cond), None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].key, "alice");
}

#[test]
fn query_in() {
    let (_dir, db) = temp_db();
    let knot = setup_people(&db);
    let table = knot.table("person").unwrap();

    let cond = Condition::r#in(
        "role",
        vec![
            PropertyValue::String("teacher".into()),
            PropertyValue::String("admin".into()),
        ],
    );
    let page = table.query(Some(&cond), None, None, 100, None).unwrap();
    assert_eq!(page.items.len(), 2);
}
