# Storage Mapping to rKV

Knot maps every concept to rKV namespaces using dot-separated type tags.

## Namespace naming convention

| Knot Concept       | rKV Namespace Pattern                   |
| ------------------ | --------------------------------------- |
| Data table         | `knot.{namespace}.t.{table}`            |
| Link table         | `knot.{namespace}.l.{link_name}`        |
| Reverse link index | `knot.{namespace}.r.{link_name}`        |
| Data index         | `knot.{namespace}.ddx.{table}.{fields}` |
| Link index         | `knot.{namespace}.ldx.{link}.{fields}`  |
| Spatial index      | `knot.{namespace}.sdx.{table}.{field}`  |
| Metadata           | `knot.{namespace}.meta`                 |

Type tags (`t`, `l`, `r`, `ddx`, `ldx`, `sdx`, `meta`) are unambiguous because
user-defined names cannot contain dots.

## Data table storage

Each data table maps to one rKV namespace. Nodes are stored as key-value pairs:

```text
┌─────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.t.person                │
│                                                     │
│  Key::Str("alice")                                  │
│    └─► Value::Data(msgpack {role:"teacher", age:45})│
│                                                     │
│  Key::Str("charlie")                                │
│    └─► Value::Null  (set mode — key only)           │
└─────────────────────────────────────────────────────┘
```

- Key: `Key::Str(primary_key)` — always rKV string type
- Value with properties: `Value::Data(msgpack_encoded_properties)`
- Value without properties: `Value::Null` (set mode)
- rKV's `Key::Str` is used exclusively — no `Key::Int` to avoid auto-upgrade

## Link table storage

Each link table maps to one rKV namespace. Link entries use composite keys:

```text
┌─────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.l.attends               │
│                                                     │
│  Key::Str("alice\x00mit")                           │
│    └─► Value::Data(msgpack {year:2020})             │
│                                                     │
│  Key::Str("bob\x00stanford")                        │
│    └─► Value::Null  (bare link)                     │
└─────────────────────────────────────────────────────┘
```

- Key: `Key::Str("{source_key}\x00{target_key}")` — null byte separator
- The `\x00` separator is safe because keys cannot contain control characters
- Forward scan by source: prefix scan on `"{source_key}\x00"`
- One entry per (source, target) pair — duplicate inserts overwrite

## Reverse link index

Each link table has a corresponding reverse index namespace. The reverse index
swaps source and target in the key:

```text
┌─────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.r.attends               │
│                                                     │
│  Key::Str("mit\x00alice")       └─► Value::Null    │
│  Key::Str("stanford\x00bob")    └─► Value::Null    │
└─────────────────────────────────────────────────────┘
```

- Maintained automatically on every link write and delete
- Enables reverse lookup: prefix scan on `"{target_key}\x00"`
- For bidirectional link tables, the reverse index doubles as the reverse
  query path — no additional storage
- For directional link tables, the reverse index is used for deletion cleanup
  and returns empty results for reverse queries (not an error)

## Metadata storage

One metadata namespace per Knot namespace. Stores definitions for tables, link
tables, and indexes:

```text
┌──────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.meta                     │
│                                                      │
│  Key::Str("table:person")                            │
│    └─► Value::Data({name:"person"})                  │
│                                                      │
│  Key::Str("link:attends")                            │
│    └─► Value::Data({                                 │
│           name:"attends",                            │
│           source:"person",                           │
│           target:"school",                           │
│           bidirectional:false,                       │
│           cascade:false                              │
│       })                                             │
│                                                      │
│  Key::Str("ddx:person:age")                          │
│    └─► Value::Data({table:"person",                  │
│           fields:["age"], sparse:false})              │
│                                                      │
│  Key::Str("sdx:school:location")                     │
│    └─► Value::Data({table:"school",                  │
│           field:"location"})                         │
└──────────────────────────────────────────────────────┘
```

Metadata key prefixes match the namespace type tags:

| Prefix   | Contents                                             |
| -------- | ---------------------------------------------------- |
| `table:` | Data table definitions                               |
| `link:`  | Link table definitions (name, source, target, flags) |
| `ddx:`   | Data index definitions (table, fields, sparse)       |
| `ldx:`   | Link index definitions (link, fields, sparse)        |
| `sdx:`   | Spatial index definitions (table or link, field)     |

## Startup and bootstrap

On `Knot::new(&db, namespace)`:

- **New namespace** — creates `knot.{ns}.meta` (empty). Ready for schema commands.
- **Existing namespace** — scans metadata to rebuild in-memory catalog of tables,
  link tables, and indexes. Orphaned link tables (referencing missing data tables)
  are logged as warnings but do not block startup.
