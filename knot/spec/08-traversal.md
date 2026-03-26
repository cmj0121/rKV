# Traversal

Traversal follows links from a starting point to discover connected data.

## Starting point

Traversal can start from:

- A single node (table + key)
- A set of nodes matching a query (table + filter conditions)

## Directed traversal

The caller provides an ordered list of link tables. Level N follows the Nth link
table in the list. Depth equals the list length.

```text
Start from alice, follow [attends, located-in]:

  alice ──attends──► mit ──located-in──► cambridge

  Level 1: scan knot.campus.l.attends  prefix="alice\x00"
  Level 2: scan knot.campus.l.located-in  prefix="mit\x00"
```

Each level expands all matching nodes before proceeding to the next level
(breadth-first within each hop).

## Discovery traversal

No link tables specified. At each level, follow all link tables where the
current node's table is a source (or target, for bidirectional links).
The `max` parameter is required — no unbounded traversal.

```text
Start from alice, discover up to 2 hops:

          ┌──teaches──► mit ──located-in──► cambridge
  alice ──┤
          └──friends──► bob ──attends──► mit
```

The engine determines applicable link tables by querying metadata for link
tables where `source = current_node's_table`. For bidirectional links, link
tables where `target = current_node's_table` are also included.

## Filtering

Two types of filtering during traversal:

- **Link filtering** — filter by link properties at each hop.
  Example: only follow attends links where `year > 2019`
- **Node filtering** — filter by destination node properties at each hop.
  Example: only reach schools where `ranking < 50`

Filters use the same condition operators as queries (equal, not equal,
greater/less, exists, pattern match, in list, near, within).

## Result shape

Traversal returns destination nodes by default (leaf nodes of the traversal).
Full paths are available on request — each path is an ordered list of nodes
and links from start to destination.

## Cycle detection

Each node is visited at most once per traversal. If a traversal path reaches
a node that has already been visited, that branch stops. This prevents infinite
loops in cyclic graphs (e.g., mutual friend links).

## Direction

- Directional link tables: traversal follows the declared direction only
  (source → target)
- Bidirectional link tables: traversal follows both directions (the forward
  namespace and the reverse index are both scanned)
- A single traversal can mix directional and bidirectional hops

## Pagination

Results are returned in pages using a position-based cursor:

- **Page size** — configurable, default 40 results per page
- **Cursor** — opaque token representing the current position in the result set;
  pass it to retrieve the next page
- **Stable** — the cursor is based on the sort field value, not an offset.
  Results are stable even when data changes between pages.
- When no more results remain, the cursor is null
