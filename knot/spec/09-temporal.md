# Temporal Queries

Every write to a node or link creates a new revision with a timestamp. Past
versions are retained — Knot is a temporal database by default.

## Revision history

Each node and link has a list of revisions ordered by timestamp. A revision
captures the complete state (all properties) at the time of the write.

| Operation       | Description                                                  |
| --------------- | ------------------------------------------------------------ |
| List history    | Return all revisions of a node or link, ordered by timestamp |
| Get revision    | Retrieve a specific revision by index or timestamp           |
| Count revisions | Number of revisions for a node or link                       |

## Point-in-time queries

Query a node or link as it was at a specific timestamp. The engine finds the
revision that was current at that time (the latest revision at or before the
requested timestamp).

Point-in-time queries work on all read operations:

- Get by key at timestamp
- Query with filter at timestamp
- Scan at timestamp
- Exists at timestamp

## Temporal traversal

Traverse the graph pinned to a past timestamp. Every hop resolves nodes and
links as they existed at the specified time:

- Links that didn't exist yet are not followed
- Links that were deleted before the timestamp are not visible
- Node properties reflect their state at that time
- Deleted nodes are visible at timestamps before their deletion

This enables questions like "what schools did alice attend as of 2024-01-01?"

## Delete vs TTL

| Action     | Revision history                          | Temporal queries           |
| ---------- | ----------------------------------------- | -------------------------- |
| Delete     | Preserved — a "deleted" revision is added | Past states visible        |
| TTL expiry | Erased completely                         | No access at any timestamp |

Delete preserves history for audit and temporal access. TTL means "I want this
data gone" — no trace remains.

## Compaction

Revision history grows unboundedly. Compaction reclaims storage by removing
old revisions:

- **Compact before timestamp** — remove all revisions older than the specified
  time. The latest revision at the compaction point is retained as the new
  earliest revision.
- Temporal queries before the compaction point return no results
- Compaction is an explicit operation, not automatic
- Compaction runs per namespace

## Time-series patterns

Revision history naturally supports time-series use cases. A node whose property
is updated repeatedly over time (e.g., a sensor writing temperature readings)
can be queried by time range:

- List revisions between timestamp A and B
- Get the value at a specific time
- Count how many updates occurred in a time window

No special time-series API — standard revision history queries cover these
patterns.
