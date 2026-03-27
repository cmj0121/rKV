# Engine Components

The Knot Engine is composed of six components. All operate through rKV — the
engine has no independent storage.

## Responsibility matrix

| Component              | Responsibility                                 | Namespaces                        |
| ---------------------- | ---------------------------------------------- | --------------------------------- |
| **Table Manager**      | CRUD for tables and nodes                      | `t.*`, `meta`                     |
| **Link Manager**       | CRUD for link tables/entries, reverse index    | `l.*`, `r.*`, `meta`              |
| **Index Manager**      | CRUD for indexes, backfill, maintain on writes | `ddx.*`, `ldx.*`, `sdx.*`, `meta` |
| **Traversal Engine**   | Directed/discovery traversal (read-only)       | `l.*`, `r.*`, `t.*`               |
| **Cascade Controller** | Delete propagation, link cleanup               | `l.*`, `r.*`, `t.*`, `meta`       |
| **Revision Manager**   | History, point-in-time, compaction             | all (via rKV revisions)           |

## Component interactions

```text
Table Manager ──────────► Index Manager (on every write)
Link Manager ───────────► Index Manager (on every write)
Cascade Controller ─────► Link Manager (delete links)
                    ────► Table Manager (delete nodes)
                    ────► Index Manager (cleanup)
Traversal Engine ──────► Link Manager (scan links)
                   ────► Table Manager (read nodes)
Revision Manager ──────► (built on rKV revision API, no component dependency)
```

## Concurrency

- Table Manager and Link Manager serialize writes within their respective
  rKV namespaces (rKV per-namespace mutex)
- Reads are concurrent with writes (rKV MVCC via revisions)
- Cross-namespace operations (cascade delete, link creation) are not atomic —
  crash recovery handles incomplete operations on restart
- Last write wins for conflicting updates to the same key
