# Knot

> Tie your data together.

**Knot** is a schema-free, graph-based database built on rKV. It provides the simplest
possible interface to store your data, make relationships between it, and query it.
Without connections, Knot serves as a simple schema-free data store. With connections,
it becomes a powerful graph database.

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

A **namespace** is an isolated environment, like a database. Within a namespace,
**tables** group related nodes. Tables must be explicitly created before use and
exist even when empty. Dropping a table deletes all its nodes and indexes.

Each node has a unique name (the primary key) within its table. The same name can
exist in different tables — they are separate nodes.

**Naming rules:**

- Primary keys: non-empty UTF-8, max 511 bytes, no control characters
- Table, namespace, link, and property names: same rules, but no dots

**Property values:**

| Type    | Description                            | Indexable |
| ------- | -------------------------------------- | --------- |
| String  | UTF-8 text                             | Yes       |
| Number  | Integer (i64) or floating point (f64)  | Yes       |
| Boolean | true / false                           | Yes       |
| Binary  | Raw bytes; large values auto-offloaded | No        |
| Null    | Removes the property (null = missing)  | N/A       |

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

Link tables are declared with specific endpoints: `attends` connects `person → school`.
Only person nodes can be the source and only school nodes can be the target. Multiple
link tables can connect the same table pair, and a table can link to itself
(e.g., `friends: person → person`). One link entry per (source, target) pair.

Links are directed by default (source → target). They can also be declared
bidirectional, meaning they can be traversed in both directions.

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

Namespaces provide physical isolation between independent datasets — like separate
databases within the same Knot instance.

### Querying

Nodes can be queried by their properties — find all persons where role is teacher,
or where age is greater than 30. Queries target one table at a time; cross-table
data discovery uses traversal.

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

Cascade delete has two levels of control:

- **Schema-level** — a link table can declare cascade as mandatory. Deleting a node
  always cascades through that link. Cannot be disabled per operation.
- **Operation-level** — the caller chooses cascade at delete time. Default off.

Batch operations execute in order but are not a transaction — a failure mid-batch
does not roll back prior operations.

### Traversal

Traversal is the core operation of a graph database — starting from a node, follow
links to discover connected data. Knot supports two modes:

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
| Link filtering  | Filter by link properties during traversal (e.g., year > 2019)  |
| Direction       | Follows link direction; bidirectional links traversed both ways |
| Path tracking   | Results include the path taken, not just the destination        |
| Pagination      | Same position-based cursor as queries                           |

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
