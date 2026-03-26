# Deletion and Cascade

## Delete node

Deleting a node always removes all links to and from it. The process:

1. Query metadata for link tables where source or target = this node's table
2. For each link table where this table is the source:
   scan forward namespace with prefix `"{key}\x00"` — find all outgoing links
3. For each link table where this table is the target:
   scan reverse namespace with prefix `"{key}\x00"` — find all incoming links
4. Remove each found link entry (forward + reverse + link indexes)
5. Remove data index entries for this node
6. Delete the node from the data table

### Cascade delete

Cascade optionally propagates deletion to connected nodes. Two levels of control:

- **Schema-level** — a link table declares `cascade: true` at creation. Deleting
  a node always cascades through that link, regardless of the operation flag.
- **Operation-level** — the caller requests cascade at delete time. Default off.

When cascade is active, each connected node found in step 2-3 is recursively
deleted using the same process. Cycle detection prevents infinite loops — each
node is deleted at most once per cascade operation.

### Delete node diagram

```text
  DELETE node "alice" from person table
  ════════════════════════════════════════

  Step 1: Query metadata
    source = "person" → [attends, teaches]
    target = "person" → [friends]

  Step 2-3: Scan relevant namespaces
    knot.campus.l.attends:  prefix "alice\x00" → [alice→mit]
    knot.campus.l.teaches:  prefix "alice\x00" ��� [alice→mit]
    knot.campus.r.friends:  prefix "alice\x00" → [alice→bob]

  Step 4: Remove link entries + reverse + indexes
    DELETE l.attends["alice\x00mit"]    + r.attends["mit\x00alice"]
    DELETE l.teaches["alice\x00mit"]    + r.teaches["mit\x00alice"]
    DELETE l.friends["bob\x00alice"]    + r.friends["alice\x00bob"]

  Step 5: Remove data indexes for alice

  Step 6: Delete alice from t.person

  If cascade ON → recurse: delete mit, delete bob (if not already visited)
```

## Delete link

Removing a link entry:

1. Delete the forward entry from `l.{link_name}`
2. Delete the reverse entry from `r.{link_name}`
3. Remove any link index entries for this link entry

If cascade is active (schema-level or operation-level), the target node is also
deleted, triggering its own deletion process.

## Cascade warning

Cascade on bidirectional or cyclic link tables (e.g., `friends: person → person`)
can propagate to all reachable nodes in the connected component. A single
cascading delete could remove a large portion of the graph.

## TTL and deletion

- **Delete** — marks the node as deleted. Revision history is preserved. Temporal
  queries can still visit the node at past timestamps.
- **TTL expiry** — erases the node and all its revision history completely. No
  temporal access after expiry.

Both delete and TTL trigger link cleanup (same as step 1-4 of delete node).
