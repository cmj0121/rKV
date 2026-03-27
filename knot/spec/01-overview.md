# Overview

Knot is a schema-free, graph-based, temporal database built on rKV. It provides
tables of nodes with flat properties, link tables for relationships, traversal
for graph queries, and revision history for temporal queries.

## Terminology

| Term                | Definition                                                                                |
| ------------------- | ----------------------------------------------------------------------------------------- |
| Namespace           | Physical isolation boundary, like a database                                              |
| Table               | Named collection of nodes within a namespace                                              |
| Node                | A key and its properties within a table                                                   |
| Key                 | Primary identity of a node — non-empty UTF-8 string, max 511 bytes, no control characters |
| Properties          | Flat key-value pairs on a node or link entry                                              |
| Link table          | Named, directed relationship between two tables                                           |
| Link entry          | Connection from a source key to a target key within a link table                          |
| Directional         | Property of a link table — writes restricted to declared direction                        |
| Bidirectional       | Property of a link table — traversal works in both directions                             |
| Directed traversal  | Traversal mode — caller specifies which links to follow                                   |
| Discovery traversal | Traversal mode — follow all applicable links up to N hops                                 |
| Revision            | A timestamped snapshot of a node or link entry created on each write                      |

## Scope

Knot v1 includes:

- Schema-free data storage (namespaces, tables, nodes, flat properties)
- Graph relationships (link tables, directed/bidirectional, link properties)
- Querying (filter, scan, count, sort, projection, pagination)
- Indexing (primary, secondary, composite, spatial — never unique)
- Data operations (insert, replace, update, delete, cascade, TTL, batch)
- Traversal (directed, discovery, link/node filtering, path tracking)
- Temporal (revision history, point-in-time queries, temporal traversal, compaction)
- Geo (lat/lon property type with spatial index, near/within queries)
- Reliability (concurrent access, durability, crash recovery)
- Embedded Rust library API
- HTTP API
- CLI REPL

Knot v1 does **not** include:

- Transactions (atomic multi-table operations)
- Aggregation (count/sum/avg on query results)
- Graph algorithms (shortest path, connected components)
- Full-text search
- Change streams / subscriptions
- Pattern matching / subgraph queries
- Variable-length traversal (min..max hops)
- Permissions / access control
- Remote mode (Knot as HTTP client to rKV)

## Design principles

- **Schema-free** — no predefined structure. Properties are optional and untyped
  at the schema level. Tables and links are created explicitly but their contents
  are not constrained.
- **Flat properties** — no nested objects or arrays. Every property is indexable.
  Structure is modeled as nodes and links, not nesting.
- **Explicit relationships** — link tables are declared with typed endpoints.
  No ad-hoc foreign keys. Referential integrity enforced on write.
- **Temporal by default** — every write creates a revision. History is always
  available unless explicitly compacted or expired via TTL.
- **Built on rKV** — Knot is a layer on top of rKV, not a replacement. It borrows
  the database, does not own it. rKV handles persistence, compression, replication,
  and crash safety.

## Naming rules

All names (tables, namespaces, link tables, property names) follow the same rules:

- Non-empty UTF-8 string
- Max 511 bytes (null-terminated at 512)
- No control characters
- No dots (reserved as structural separators in rKV namespace paths)

Primary keys follow the same rules but **dots are allowed** in keys.
