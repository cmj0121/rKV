# Knot

> Tie your data together.

**Knot** is a schema-free, graph-based, temporal database built on rKV. It provides the
simplest possible interface to store your data, make relationships between it, and query
it. Without connections, Knot serves as a simple schema-free data store. With connections,
it becomes a powerful graph database. Every change is versioned — query the present or
any point in the past.

## Concept

Knot organizes data into **tables** within a **namespace**. Each table holds **nodes** —
uniquely named entries with optional properties. Properties are flat key-value pairs
attached to nodes or links.

```text
┌──────────────┐         ┌──────────────┐          ┌──────────────┐
│  Data Store  │         │  Add Links   │          │    Graph     │
│  ┌───┐ ┌───┐ │         │  ┌───┐ ┌───┐ │          │  ┌───┐ ┌───┐ │
│  │tbl│ │tbl│ │         │  │tbl│→│tbl│ │          │  │tbl│→│tbl│ │
│  │ A │ │ B │ │         │  │ A │ │ B │ │          │  │ A │ │ B │ │
│  └───┘ └───┘ │         │  └───┘ └───┘ │          │  └─│─┘ └─│─┘ │
│  ┌───┐       │         │  ┌───┐       │          │    └──→┌─▼─┐ │
│  │tbl│       │         │  │tbl│       │          │        │tbl│ │
│  │ C │       │         │  │ C │       │          │        │ C │ │
│  └───┘       │         │  └───┘       │          │        └───┘ │
└──────────────┘         └──────────────┘          └──────────────┘
  tables + nodes          add link tables           query the graph
```

### As data store

A **namespace** is an isolated environment, like a database. Namespaces must be
explicitly created and can be dropped (which deletes everything within). Within a
namespace, **tables** group related nodes. Tables must be explicitly created before
use and exist even when empty. Dropping a table deletes all its nodes, indexes,
and any link tables that reference it.

Each node has a unique name (the primary key) within its table. The same name can
exist in different tables — they are separate nodes.

**Naming rules:**

- Primary keys: non-empty UTF-8, max 511 bytes, no control characters
- Table, namespace, link, and property names: same rules, but no dots

**Property values:**

| Type    | Description                            | Indexable     |
| ------- | -------------------------------------- | ------------- |
| String  | UTF-8 text                             | Yes           |
| Number  | Integer (i64) or floating point (f64)  | Yes           |
| Boolean | true / false                           | Yes           |
| Binary  | Raw bytes; large values auto-offloaded | No            |
| Geo     | Latitude/longitude point               | Yes (spatial) |
| Null    | Removes the property (null = missing)  | N/A           |

Setting a property to null removes it. Nodes without any properties act as a
pure set — membership only, no data attached.

```text
┌─────────────────────────────────────────────────────┐
│  Namespace: campus                                  │
│                                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐   │
│  │  Table: person      │  │  Table: school      │   │
│  │                     │  │                     │   │
│  │  ┌───────────────┐  │  │  ┌───────────────┐  │   │
│  │  │ alice         │  │  │  │ mit           │  │   │
│  │  │   role=teacher│  │  │  │   ranking=1   │  │   │
│  │  │   age=45      │  │  │  │   city=       │  │   │
│  │  └───────────────┘  │  │  │    cambridge  │  │   │
│  │  ┌───────────────┐  │  │  └───────────────┘  │   │
│  │  │ bob           │  │  │                     │   │
│  │  │   role=student│  │  └─────────────────────┘   │
│  │  │   age=22      │  │                            │
│  │  └───────────────┘  │                            │
│  │                     │                            │
│  └─────────────────────┘                            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### As graph database

Knot becomes a graph database when you add **link tables**. A link table defines a
named relationship between two tables — connecting nodes from one table to nodes in
another. Links can carry optional properties just like nodes.

**Link table rules:**

- Declared with specific endpoints: `attends` connects `person → school`
- Both endpoints must exist when creating a link — no dangling links
- Multiple link tables can connect the same table pair
- A table can link to itself (e.g., `friends: person → person`)
- One link entry per (source, target) pair; duplicates overwrite (upsert)
- For multiple instances of the same relationship, use an intermediate table
  (e.g., `enrollment` between person and school for separate BS and MS records)
- Link tables must be explicitly created and can be independently dropped
- Dropping a link table removes its entries but does not affect connected data tables
- Link tables can be listed when inspecting a namespace

Links are directed by default (source → target). They can also be declared
bidirectional, meaning they can be traversed in both directions. For bidirectional
links, creating alice → bob also makes bob → alice queryable. Only one entry is
stored; the reverse direction is derived automatically.

```text
┌───────────┐    teaches (course=cs101)   ┌───────────┐
│   alice   │ ──────────────────────────> │   mit     │
│  (person) │                             │  (school) │
└───────────┘                             └───────────┘
   │                                           ▲
   │ friends (bidirectional)                   │
   │                                           │
┌──▼────────┐    attends (year=2023)           │
│    bob    │ ─────────────────────────────────┘
│  (person) │
└───────────┘
```

**Link queries:** Link tables are queryable just like data tables — scan, query by
properties, count, sort, projection, and pagination all work. Reverse lookup (given
a target, find all sources) works on all link tables including directional ones.

Namespaces provide physical isolation between independent datasets — like separate
databases within the same Knot instance.

### Querying

Nodes and links can be queried by their properties — find all persons where role
is teacher, or all attends links where year is greater than 2019. Queries target
one table at a time; cross-table data discovery uses traversal.

| Operation  | Description                                                      |
| ---------- | ---------------------------------------------------------------- |
| Get by key | Retrieve a node by its primary key                               |
| Exists     | Check if a node exists without fetching properties               |
| Query      | Find nodes matching property conditions                          |
| Scan       | List nodes in a table, optionally by key prefix (lexicographic)  |
| Count      | Count nodes — total or matching a condition                      |
| Sort       | Order results by any property, ascending or descending           |
| Projection | Return only specific properties instead of the full node         |
| Pagination | Position-based cursor on the sort field; stable across mutations |

**Query conditions:**

| Operator      | Description                                 |
| ------------- | ------------------------------------------- |
| Equal         | Property equals a value                     |
| Not equal     | Property does not equal a value             |
| Greater/less  | Numeric or string comparison (>, >=, <, <=) |
| Exists        | Property is present (not null)              |
| Not exists    | Property is missing (null)                  |
| Pattern match | String prefix or wildcard match             |
| In list       | Property matches one of several values      |
| Near          | Geo property within a distance of a point   |
| Within        | Geo property inside a bounding box          |

Conditions can be combined with AND and OR. AND binds tighter than OR. Grouping
overrides precedence.

### Indexing

Every node name is a primary key — lookup by name is always fast. This is built-in
and requires no configuration.

For property-based queries, secondary indexes can be created on any property of a
node or a link. If an index exists on the queried property, the lookup is fast.
Without an index, Knot scans the table.

```text
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  Primary key (built-in)        Secondary index (optional)   │
│                                                             │
│  "alice" ──────► node          age > 30 ───────► [alice]    │
│  "bob"   ──────► node          role = teacher ─► [alice]    │
│                                                             │
│  Link index (optional)                                      │
│                                                             │
│  year > 2019 ──► [alice→mit]                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

Indexes work the same way on both nodes and links. They can cover a single property
or multiple properties together (composite index, explicitly created). Binary
properties cannot be indexed. Indexes are for performance only and never enforce
uniqueness.

### Data operations

Nodes and links support the same set of operations:

| Operation      | Node                                         | Link                            |
| -------------- | -------------------------------------------- | ------------------------------- |
| Insert         | Create a node with optional properties       | Create a link between two nodes |
| Replace        | Overwrite all properties                     | Overwrite all link properties   |
| Update         | Modify specific properties, keep the rest    | Same — partial merge            |
| Delete         | Remove node and clean up all connected links | Remove a link                   |
| Cascade delete | Delete node and propagate to connected nodes | Delete link and target node     |
| TTL            | Auto-delete after a time-to-live expires     | Same — both nodes and links     |
| Batch          | Multiple operations in one call              | Same — not a transaction        |

Deleting a node **always** removes all links to and from it — no dangling links.
Cascade delete optionally propagates to connected nodes (default off, chosen per
operation). A link table can also declare cascade as mandatory at schema level —
deleting a node always cascades through that link, regardless of the operation flag.

Batch operations execute in order but are not a transaction — a failure mid-batch
does not roll back prior operations.

### Traversal

Traversal is the core operation of a graph database — starting from a node or a
set of nodes matching a query, follow links to discover connected data. Knot
supports two modes:

**Directed traversal** — you specify which links to follow, in order. Each step
follows one link type.

```text
Start from alice, follow "teaches", then follow "located-in":

  alice ──teaches──► mit ──located-in──► cambridge

  Result: cambridge (via alice → mit → cambridge)
```

**Discovery traversal** — follow all available links up to N hops. Explore
everything reachable from a starting node.

```text
Start from alice, discover up to 2 hops:

          ┌──teaches──► mit ──located-in──► cambridge
  alice ──┤
          └──friends──► bob ──attends──► mit

  Result: mit, bob, cambridge
```

**Traversal behaviors:**

| Behavior        | Description                                                     |
| --------------- | --------------------------------------------------------------- |
| Cycle detection | Each node visited at most once — no infinite loops              |
| Link filtering  | Filter by link properties at each hop (e.g., year > 2019)       |
| Node filtering  | Filter by destination node properties at each hop               |
| Direction       | Follows link direction; bidirectional links traversed both ways |
| Result shape    | Destination nodes by default; full paths available on request   |
| Pagination      | Same position-based cursor as queries                           |

### Revision history and temporal queries

Every node and link retains its full revision history. Each write creates a new
revision with a timestamp — past versions are never lost.

| Operation          | Description                                            |
| ------------------ | ------------------------------------------------------ |
| History            | List all revisions of a node or link                   |
| Point-in-time      | Query a node or link as it was at a specific timestamp |
| Temporal traversal | Traverse the graph pinned to a past timestamp          |

Temporal traversal resolves every node and link at the specified time — follow
links and read properties as they existed at that moment. This enables questions
like "what schools did alice attend as of 2024-01-01?"

Deleting a node preserves its revision history — temporal queries can still visit
it at past timestamps. TTL expiry is different: it erases the node and all its
history completely.

Revision history also supports time-series patterns naturally. A node whose
property is updated repeatedly over time (e.g., a sensor writing temperature
readings) can be queried by time range — the revision history IS the time series.

Revision history can be compacted — remove all revisions before a specified time
to reclaim storage. Temporal queries before the compaction point return no results.

### Reliability

Knot is safe for concurrent access from multiple threads. Last write wins for
conflicting updates to the same node or link.

Writes are durable — data survives process crashes and restarts. On restart, Knot
automatically recovers from incomplete operations. Corrupted data is detected and
can be repaired.

### Why flat properties

Knot properties are flat key-value pairs — no nested objects, no arrays. This keeps
every property indexable and every query predictable.

When you need structure, model it as nodes and links instead of nesting. An address
isn't a nested object on a person — it's a separate node linked to the person. This
makes every piece of data independently queryable and connected.

## Built on rKV

Knot is not a storage engine. It maps nodes, properties, and links onto rKV's
key-value primitives. rKV handles persistence, compression, replication, and crash
safety. Knot handles the data model and the relationship layer.
