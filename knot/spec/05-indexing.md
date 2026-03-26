# Indexing

## Primary index

Every node name is a primary key. Lookup by key is fast via rKV `get()` — no
additional index needed. Primary keys are the only uniqueness guarantee.

## Secondary indexes

Secondary indexes accelerate property-based queries on both data tables and link
tables. They use order-preserving encoding for range queries.

### Data table index

Maps property values to primary keys:

```text
┌─────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.ddx.person.age          │
│                                                     │
│  Key::Str(hex(encode(25) + 0xFF + "bob"))           │
│    └─► Value::Null                                  │
│                                                     │
│  Key::Str(hex(encode(30) + 0xFF + "alice"))         │
│    └─► Value::Null                                  │
└─────────────────────────────────────────────────────┘
```

### Link table index

Maps link property values to composite link keys:

```text
┌──────────────────────────────────────────────────────────┐
│  rKV namespace: knot.campus.ldx.attends.year             │
│                                                          │
│  Key::Str(hex(encode(2020) + 0xFF + "alice\x00mit"))     │
│    └─► Value::Null                                       │
└──────────────────────────────────────────────────────────┘
```

### Spatial index

Geo properties use a spatial index (geohash or S2 cells) for near/within queries.
The spatial index namespace uses the `sdx` type tag:

```text
rKV namespace: knot.campus.sdx.school.location
```

Implementation details of the spatial encoding are deferred to implementation.

## Index properties

- **Order-preserving encoding** — hex-encoded binary keys stored as `Key::Str`.
  Exact encoding algorithm deferred to implementation.
- **Composite indexes** — cover multiple properties together. Fields are
  concatenated in the encoded key. Must be explicitly created.
- **Non-sparse (default)** — entries missing the indexed field are stored as null
- **Sparse** — entries missing the indexed field are skipped
- **No uniqueness** — indexes never enforce uniqueness (permanent policy)
- **Binary excluded** — binary properties cannot be indexed
- **Auto-used** — the query engine automatically uses matching indexes
- **Maintained on every write** — insert, update, replace, and delete all
  update affected indexes

## Index lifecycle

- Indexes must be explicitly created on specific properties
- Creating an index on an existing table backfills from current data
- Dropping an index removes the index namespace; data is unaffected
- Indexes are listed as metadata entries (`ddx:`, `ldx:`, `sdx:` prefixes)
