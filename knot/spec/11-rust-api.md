# Rust Library API

The primary interface for embedded use. Knot borrows an rKV `DB` reference —
it does not own or manage the database. One `Knot` instance serves one namespace.

## Opening

```text
let knot = Knot::new(db: &DB, namespace: &str) -> Result<Knot>
```

- New namespace: creates metadata namespace, ready for schema commands
- Existing namespace: scans metadata, rebuilds in-memory catalog
- Orphaned link tables: logged as warnings, do not block startup

## Schema operations

```text
knot.create_table(name) -> Result<()>
knot.create_table_if_not_exists(name) -> Result<()>
knot.drop_table(name) -> Result<()>
knot.tables() -> Result<Vec<String>>

knot.create_link(name, source, target, bidi, cascade) -> Result<()>
knot.create_link_if_not_exists(name, source, target, bidi, cascade) -> Result<()>
knot.drop_link(name) -> Result<()>
knot.alter_link(name, bidi, cascade) -> Result<()>
knot.links() -> Result<Vec<LinkMeta>>

knot.create_index(table, fields, sparse) -> Result<()>
knot.create_link_index(link, fields, sparse) -> Result<()>
knot.create_spatial_index(table_or_link, field) -> Result<()>
knot.drop_index(table, fields) -> Result<()>
knot.drop_link_index(link, fields) -> Result<()>
knot.drop_spatial_index(table_or_link, field) -> Result<()>
knot.indexes() -> Result<Vec<IndexMeta>>
```

## Node operations

```text
let table = knot.table(name) -> Result<Table>

table.get(key) -> Result<Option<Node>>
table.exists(key) -> Result<bool>
table.insert(key, properties) -> Result<()>
table.insert_set(key) -> Result<()>
table.replace(key, properties) -> Result<()>
table.update(key, changes) -> Result<()>
table.delete(key, cascade: bool) -> Result<()>
table.set_ttl(key, duration) -> Result<()>

table.scan() -> ScanBuilder
table.query(conditions) -> QueryBuilder
table.count() -> CountBuilder
```

## Link operations

```text
let link = knot.link(name) -> Result<Link>

link.get(from, to) -> Result<Option<LinkEntry>>
link.insert(from, to, properties) -> Result<()>
link.insert_bare(from, to) -> Result<()>
link.replace(from, to, properties) -> Result<()>
link.update(from, to, changes) -> Result<()>
link.delete(from, to, cascade: bool) -> Result<()>
link.set_ttl(from, to, duration) -> Result<()>

link.from(key) -> ScanBuilder       // outgoing links
link.to(key) -> ScanBuilder         // incoming links (reverse)
link.scan() -> ScanBuilder
link.query(conditions) -> QueryBuilder
link.count() -> CountBuilder
```

## Query and scan builders

Builder pattern for constructing queries with conditions, sort, projection,
and pagination:

```text
table.scan()
    .prefix("al")
    .sort("age", Asc)
    .project(&["name", "age"])
    .limit(20)
    .cursor(prev_cursor)
    .execute() -> Result<Page<Node>>

table.query(vec![
    Condition::gt("age", 30),
    Condition::eq("role", "teacher"),
])
    .sort("name", Desc)
    .limit(10)
    .execute() -> Result<Page<Node>>
```

## Traversal

Builder pattern for traversal:

```text
// Directed
knot.traverse("person", "alice")
    .follow("attends")
    .filter_link("attends", Condition::gt("year", 2019))
    .follow("located-in")
    .filter_node(Condition::lt("ranking", 50))
    .limit(40)
    .execute() -> Result<Page<TraversalResult>>

// Directed with paths
knot.traverse("person", "alice")
    .follow("attends")
    .with_paths()
    .execute() -> Result<Page<TraversalResult>>

// Discovery
knot.discover("person", "alice", max_hops: 3, bidi: false)
    .limit(40)
    .execute() -> Result<Page<TraversalResult>>

// Traversal from query result set
knot.traverse_from(
    table.query(vec![Condition::eq("role", "teacher")])
)
    .follow("attends")
    .execute() -> Result<Page<TraversalResult>>
```

## Temporal queries

```text
// Point-in-time node lookup
table.get_at(key, timestamp) -> Result<Option<Node>>

// Point-in-time traversal
knot.traverse("person", "alice")
    .at(timestamp)
    .follow("attends")
    .execute() -> Result<Page<TraversalResult>>

// Revision history
table.history(key) -> Result<Vec<Revision>>
table.history_range(key, from_ts, to_ts) -> Result<Vec<Revision>>
table.revision_count(key) -> Result<u64>

// Compaction
knot.compact_before(timestamp) -> Result<CompactionReport>
```

## Result types

```text
Node { key: String, properties: Option<HashMap<String, Value>> }
LinkEntry { from: String, to: String, properties: Option<HashMap<String, Value>> }
Page<T> { items: Vec<T>, cursor: Option<Cursor>, has_more: bool }
TraversalResult { leaves: Vec<String>, paths: Option<Vec<Vec<String>>> }
Revision { timestamp: u128, properties: Option<HashMap<String, Value>> }
CompactionReport { revisions_removed: u64, bytes_reclaimed: u64 }
LinkMeta { name, source, target, bidirectional, cascade }
IndexMeta { name, table_or_link, fields, sparse, kind }
```
