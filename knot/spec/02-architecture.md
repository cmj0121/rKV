# System Architecture

## Layers

```text
┌─────────────────────────────────────────────────────────┐
│                     Client Layer                        │
│  ┌───────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ CLI REPL  │  │  HTTP API    │  │ Rust Library API │  │
│  └─────┬─────┘  └──────┬───────┘  └────────┬─────────┘  │
└────────┼───────────────┼───────────────────┼────────────┘
         └───────────────┼───────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────┐
│                     Knot Engine                          │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐   │
│  │ Table Manager│  │ Link Manager │  │ Index Manager │   │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘   │
│         └─────────────────┼───────────────────┘          │
│                           │                              │
│  ┌────────────────────────▼─────────────────────────┐    │
│  │  Traversal Engine   │   Cascade Controller       │    │
│  └────────────────────────┬─────────────────────────┘    │
│                           │                              │
│  ┌────────────────────────▼─────────────────────────┐    │
│  │              Revision Manager                    │    │
│  └────────────────────────┬─────────────────────────┘    │
└───────────────────────────┼──────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────┐
│                       rKV                                │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────┐  │
│  │ Namespace A│  │ Namespace B│  │   Namespace ...    │  │
│  └────────────┘  └────────────┘  └────────────────────┘  │
│                                                          │
│              Key ──► Value Storage (LSM-tree)            │
└──────────────────────────────────────────────────────────┘
```

## Client layer

Three interfaces to the same Knot Engine:

- **Rust Library API** — embedded, direct function calls. Primary interface.
  Borrows an rKV `DB` reference. One `Knot` instance per namespace.
- **HTTP API** — standalone server with rKV embedded. RESTful routes with
  single-letter type prefixes (`/m/`, `/t/`, `/l/`, `/g/`).
- **CLI REPL** — interactive shell. Text commands for schema, symbol-based
  expressions for data operations.

## Knot Engine

The engine is composed of six components:

| Component              | Responsibility                                                                     |
| ---------------------- | ---------------------------------------------------------------------------------- |
| **Table Manager**      | CRUD for tables and nodes; validates keys; coordinates with Index Manager          |
| **Link Manager**       | CRUD for link tables and entries; maintains reverse index; validates endpoints     |
| **Index Manager**      | CRUD for secondary indexes (data + link + spatial); maintains on every write       |
| **Traversal Engine**   | Directed and discovery modes; cursor pagination; cycle detection; filtering        |
| **Cascade Controller** | Metadata-guided link scan on delete; recursive cascade; index cleanup              |
| **Revision Manager**   | Revision creation on writes; point-in-time lookups; temporal traversal; compaction |

All components operate through rKV — they read and write to rKV namespaces.
The engine does not have its own storage layer.

## rKV layer

Knot borrows an rKV `DB` reference. It does not own or manage the database
lifecycle. rKV provides:

- **Persistence** — LSM-tree with crash-safe append-only log (AOL)
- **Namespaces** — isolated key-value spaces (one per Knot table, link table, index)
- **Revisions** — every write creates a timestamped revision ID
- **Compression** — LZ4 for values, bin objects for large binary data
- **Replication** — primary-replica and peer-to-peer sync
- **Crash recovery** — AOL replay, checksum verification, repair

## Data flow

```text
Client request
    │
    ▼
Knot Engine
    │
    ├── Table/Link Manager ──► rKV namespace (data)
    │
    ├── Index Manager ──► rKV namespace (index)
    │
    ├── Revision Manager ──► rKV revisions (built-in)
    │
    └── Cascade Controller ──► metadata scan ──► multiple rKV namespaces
```

All writes go through the engine to rKV. The engine coordinates across
namespaces but each individual rKV write is atomic. Cross-namespace
operations (cascade delete, link creation) are not transactional — crash
recovery handles incomplete operations on restart.
