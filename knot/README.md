# Knot

> Tie your data together.

The **Knot** is a schema-free, graph-based database built on rKV. It provides the simplest
possible interface to store your data, make relationships between it, and query it. Without
connections, Knot serves as a simple schema-free data store. With connections, it becomes a
powerful graph database.

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
**tables** group related nodes. Each node has a unique name (the primary key) within
its table. The same name can exist in different tables — they are separate nodes.

Properties are key-value pairs where the value can be a string, number, boolean,
or null. Nodes without properties act as a pure set — membership only.

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
This means only person nodes can be the source and only school nodes can be the target.
Multiple link tables can connect the same table pair, and a table can link to itself
(e.g., `friends: person → person`). One link entry per (source, target) pair.

Links are directed by default (source → target). They can also be declared
bidirectional, meaning they can be traversed in both directions.

```text
┌───────────┐    teaches (course=cs101)    ┌───────────┐
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

Namespaces provide physical isolation between independent datasets, like separate
databases within the same Knot instance.

## Built on rKV

Knot is not a storage engine. It maps nodes, properties, and links onto rKV's key-value
primitives. rKV handles persistence, compression, replication, and crash safety. Knot
handles the data model and the relationship layer.
