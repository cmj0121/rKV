# Data Model

## Hierarchy

```text
Namespace
  └── Table
        └── Node (key + properties)
  └── Link Table (source table → target table)
        └── Link Entry (source key → target key + properties)
```

## Namespace

A namespace is an isolated environment, like a database. All tables, link tables,
and indexes within a namespace are independent from other namespaces.

- Must be explicitly created before use
- Dropping a namespace deletes everything within it (tables, links, indexes, data)
- Namespace names: non-empty UTF-8, max 511 bytes, no control characters, no dots

## Table

A table is a named collection of nodes within a namespace.

- Must be explicitly created before use
- Exists even when empty (zero nodes)
- Dropping a table deletes all its nodes, indexes, and any link tables that
  reference it (as source or target)
- Table names follow the same rules as namespace names

## Node

A node is a key and its optional properties within a table.

- **Key**: unique within its table. Non-empty UTF-8, max 511 bytes, no control
  characters. Dots are allowed in keys (unlike names).
- The same key can exist in different tables — they are separate nodes.
- Nodes without properties act as pure set members (membership only).

## Property types

| Type    | Description                           | Indexable     | Notes                                          |
| ------- | ------------------------------------- | ------------- | ---------------------------------------------- |
| String  | UTF-8 text                            | Yes           |                                                |
| Number  | Integer (i64) or floating point (f64) | Yes           |                                                |
| Boolean | true / false                          | Yes           |                                                |
| Binary  | Raw bytes                             | No            | Large values auto-offloaded to rKV bin objects |
| Geo     | Latitude/longitude point (f64, f64)   | Yes (spatial) | Points only; polygons deferred                 |
| Null    | Removes the property                  | N/A           | null = missing; no distinction                 |

- Property names: non-empty UTF-8, max 511 bytes, no control characters, no dots
- Setting a property to null removes it from the node
- Properties are encoded as MessagePack internally

## Link table

A link table defines a named, directed relationship between two tables.

### Creation parameters

| Parameter     | Required | Default | Description                                              |
| ------------- | -------- | ------- | -------------------------------------------------------- |
| name          | Yes      | —       | Link table name (same naming rules as tables)            |
| source        | Yes      | —       | Source data table                                        |
| target        | Yes      | —       | Target data table                                        |
| bidirectional | No       | false   | If true, reverse direction is queryable                  |
| cascade       | No       | false   | If true, node deletion always cascades through this link |

### Constraints

- Fixed endpoints — always connects the same two tables (set at creation)
- Source and target can be the same table (self-referential, e.g., `friends`)
- Multiple link tables can connect the same table pair
- Must be explicitly created before use
- Can be independently dropped (does not affect data tables)
- Can be listed when inspecting a namespace

### Dropping a link table

Removes the link table and all its entries (forward, reverse, indexes). The data
tables on both sides are unaffected.

## Link entry

A link entry connects a source key to a target key within a link table.

### Link entry rules

- Both endpoints must exist when creating a link — no dangling links
- One entry per (source, target) pair; inserting a duplicate overwrites
  existing properties (upsert)
- Link entries can carry optional flat properties (same types as node properties)
- A node can link to itself (source key = target key)

### Normalization pattern

For multiple instances of the same relationship (e.g., alice attends mit twice
for BS and MS), create an intermediate table:

```text
Tables:       person, school, enrollment
Link tables:  enrolled-by (enrollment → person)
              enrolled-at (enrollment → school)

enrollment["e1"] → {degree:"BS", field:"math", start:2018, end:2022}
enrollment["e2"] → {degree:"MS", field:"cs",   start:2023, end:2025}

enrolled-by: e1 → alice,  e2 → alice
enrolled-at: e1 → mit,    e2 → mit
```

## Bidirectional links

For bidirectional link tables, creating a link `alice → bob` also makes
`bob → alice` queryable. Only one entry is stored; the reverse direction is
derived from the reverse index automatically.

Bidirectional affects traversal and queries:

- Forward lookup: scan by source key — works on all link tables
- Reverse lookup: scan by target key — works on all link tables (returns empty
  on directional if no links exist in that direction)
- Traversal: bidirectional links are followed in both directions;
  directional links are followed in the declared direction only
