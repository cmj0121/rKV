# Replication

> Replication protocol details for [rKV](../CONCEPTS.md). For core concepts (keys, values,
> namespaces, revisions, configuration), see the main [Concepts](../CONCEPTS.md) document.

## Primary-Replica Replication

rKV supports asynchronous primary-replica replication over TCP, enabling read scaling and basic high
availability. The replication subsystem uses `std::net` (no Tokio dependency), running on dedicated
background threads alongside the existing flush and compaction threads.

### Roles

Each node operates in one of four roles, configured at startup:

- **Standalone** (default): No replication. The node accepts reads and writes independently.
- **Primary**: Accepts all reads and writes. Streams every write to connected replicas in real time.
- **Replica**: Connects to a primary, receives data, and serves reads. Rejects all local writes with
  a `ReadOnlyReplica` error.
- **Peer**: Master-master replication. Each node accepts reads and writes. Writes propagate
  bidirectionally using last-writer-wins (LWW) conflict resolution (see [Peer Replication](#peer-replication)
  below).

### Replication Flow (Primary-Replica)

```text
  Primary                              Replica
    |                                    |
    |<--- TCP connect -------------------|
    |                                    |
    |---- Handshake (cluster_id) ------->|
    |<--- Handshake (cluster_id) --------|
    |                                    |
    |<--- SyncRequest (last_rev) --------|
    |                                    |
    |  [if incremental possible]         |
    |---- IncrementalSyncStart --------->|
    |---- AolRecord × N --------------->|  replay_fn()
    |                                    |
    |  [else full sync]                  |
    |---- FullSyncStart ---------------->|
    |---- SstChunk × N ---------------->|  write to disk
    |---- ObjectChunk × N -------------->|  write to disk
    |---- FullSyncEnd ------------------>|  post_sync_fn()
    |                                    |
    |  [live streaming]                  |
    |---- AolRecord -------------------->|  replay to memtable + AOL
    |---- DropNamespace ---------------->|  remove ns data + files
    |---- Heartbeat -------------------->|
    |                                    |
```

### How It Works

When a replica connects to a primary, replication proceeds in two phases:

1. **Initial sync**: The replica sends a `SyncRequest` with its last known revision (from a persisted
   checkpoint). The primary decides between two strategies:
   - **Incremental sync**: If the replica's revision is non-zero and the primary's AOL contains records
     after that revision, only the new records are sent. This is fast and avoids re-transferring data
     the replica already has.
   - **Full sync**: If the replica's revision is zero, the AOL has been truncated (after a flush), or
     `force_full` is set, the primary streams all SSTable and bin-object files. The replica writes
     these files to local storage, then calls `post_sync_fn` to reload the SSTable index, reset
     memtables in-place, truncate its local AOL, and register new namespaces.
2. **Live streaming**: After initial sync, the primary forwards every AOL (append-only log) record to
   the replica as it is written. The replica applies each record to its local AOL and in-memory write
   buffer, staying current with the primary's writes.

A **checkpoint file** (`repl_checkpoint`) persists the highest revision seen across restarts, enabling
incremental sync on reconnection. The checkpoint is saved on clean shutdown and on every disconnect.

### Wire Protocol

Messages are framed as `[type: 1 byte][payload length: 4 bytes BE][payload][checksum: 5 bytes CRC32C]`.
The checksum covers the type, length, and payload bytes, providing integrity verification on every
message. A handshake exchange at connection start verifies cluster membership via matching cluster IDs.

### Failure Handling

- **Replica reconnection**: If the connection to the primary drops, the replica automatically reconnects
  with exponential backoff (1 second to 30 seconds). On reconnection, the replica attempts incremental
  sync using its persisted checkpoint. If the primary's AOL has been truncated (e.g. after a flush),
  it falls back to a full sync.
- **Primary tolerance**: The primary accepts multiple concurrent replicas. If a replica disconnects,
  the primary cleans up its resources without affecting other replicas or write throughput.

### Read-Only Enforcement

Replicas reject mutations at every layer (defense-in-depth):

- **Engine**: `put`, `delete`, `delete_range`, `delete_prefix` return `ReadOnlyReplica`
- **HTTP routes**: Guards on `PUT /keys`, `DELETE /keys`, `DELETE /scan`, namespace routes
- **REPL**: Commands `put`, `del`, `wipe`, `drop`, `config set` blocked with error message
- **Web UI**: Mutation buttons disabled; role badge shown in header

Maintenance operations (`flush`, `sync`, `compact`) are **not** writes — they reorganize local
storage and are allowed on replicas.

### Namespace Synchronization

When a primary or peer node creates a new namespace (via `db.namespace(name, pw)`), it broadcasts a
**sentinel record** — an AOL entry with an empty key (`Key::Str("")`) and `Value::Null` — to
all connected replicas or peers. This ensures other nodes learn about new namespaces immediately,
even before any data is written to them. The sentinel is detected and skipped during replay (no key
is inserted); only the namespace registration side-effect is applied.

When a primary or peer node drops a namespace (via `db.drop_namespace(name)`), it broadcasts a
`DropNamespace` message to all connected replicas and peers. The receiving node removes the
namespace from in-memory maps (memtable, SSTables, object stores) and deletes on-disk files.

### Post-Sync Memory Safety

After a full sync, the replica reloads its SSTable index via `post_sync_fn`. This callback resets
each in-memory MemTable **in-place** (replacing the contents of each `Mutex<MemTable>` without
removing map entries) and registers any new namespace entries. The HashMap of namespace data only
grows and is never shrunk, preserving the safety invariant that raw pointers returned by
`get_or_create_memtable()` remain valid for the lifetime of the `DB`.

### Observability

The node's role is exposed through multiple channels:

- **Stats**: `stats` command (CLI) and `GET /api/admin/stats` include a `role` field.
- **Health**: `GET /health` returns JSON with `status`, `role`, and `uptime_secs`.
- **Config**: `config` command shows the current replication settings.
- **REPL prompt**: Shows `[primary]>` or `[replica]>` when not in standalone mode.
- **Web UI**: A role badge appears next to the logo; the admin stats grid includes the role.

### CLI Arguments

| Flag             | Default      | Description                                              |
| ---------------- | ------------ | -------------------------------------------------------- |
| `--role`         | `standalone` | Node role: `standalone`, `primary`, `replica`, or `peer` |
| `--repl-port`    | `8322`       | TCP port for replication connections                     |
| `--primary-addr` | _(none)_     | Primary address (replica only, e.g. `10.0.0.1:8322`)     |
| `--peers`        | _(none)_     | Comma-separated peer addresses (peer only)               |
| `--cluster-id`   | _(random)_   | Unique cluster ID for this node (peer only)              |

### Docker Compose Topology

A `docker-compose.yml` in the project root defines a five-node topology with two write nodes
and three read nodes, all using peer replication:

- **write-1** (port 8321, cluster-id 1): Peer, connects to write-2
- **write-2** (port 8323, cluster-id 2): Peer, connects to write-1
- **read-1** (port 8324, cluster-id 3): Peer, connects to both write nodes
- **read-2** (port 8325, cluster-id 4): Peer, connects to both write nodes
- **read-3** (port 8326, cluster-id 5): Peer, connects to both write nodes

All nodes are technically peers (can accept writes), but the read nodes are designated
for read traffic. Writes on any node propagate to all others via peer replication.
Data is bind-mounted to `.data/` subdirectories.

## Peer Replication

Peer (master-master) replication allows two or more nodes to accept writes independently, with
changes propagating bidirectionally. Conflicts are resolved using last-writer-wins (LWW) based on
revision timestamps.

### Peer Sync Protocol

When a peer connector establishes a TCP connection to another peer's listener, initial sync follows
the same incremental-or-full strategy as primary-replica:

1. **Connector** sends `SyncRequest` with its `last_revision` (loaded from `peer_checkpoint` file).
2. **Listener** decides:
   - If `last_revision > 0` and AOL has records after that revision → **incremental sync** (send
     only new records). The listener flushes its AOL buffer before reading to ensure all records
     are visible.
   - Otherwise → **full sync** (stream SSTable and object files).
3. **Connector** processes the response:
   - **Non-empty full sync**: Writes SST/object files to disk, then calls `post_sync_fn` to reload
     the SSTable index, reset memtables, truncate the local AOL, and register namespaces.
   - **Empty full sync** (0 SSTs, 0 objects): Skipped — an empty sync means the sender has no
     persisted data and should not wipe the receiver's local state.
   - **Incremental sync**: Records are replayed via `replay_fn` (LWW resolution), which updates
     the memtable and appends to the local AOL.

After initial sync, both sides enter **bidirectional live streaming**.

### Replication Flow (Peer-Peer)

```text
  Peer A (connector)                   Peer B (listener)
    |                                    |
    |--- TCP connect ------------------->|
    |                                    |
    |--- Handshake (cluster=1, Peer) --->|
    |<-- Handshake (cluster=2, Peer) ----|
    |                                    |
    |--- SyncRequest (last_rev) -------->|
    |                                    |
    |  [full or incremental sync]        |
    |<-- sync response (SSTs/records) ---|
    |                                    |
    |  [bidirectional live stream]       |
    |--- AolRecord --------------------->|  replay_fn (LWW)
    |<-- AolRecord ----------------------|  replay_fn (LWW)
    |--- DropNamespace ----------------->|  drop_ns_fn
    |<-- DropNamespace ------------------|  drop_ns_fn
    |--- Heartbeat --------------------->|
    |<-- Heartbeat ----------------------|
    |                                    |

  Meanwhile, Peer B also connects to Peer A (reverse direction):

  Peer B (connector)                   Peer A (listener)
    |--- TCP connect ------------------->|
    |--- Handshake + SyncRequest ------->|
    |<-- sync response ------------------|
    |  [bidirectional live stream]       |
    |<=> AolRecord / DropNamespace /<===>|
```

Each peer broadcasts accepted records to all other connected peers
(excluding the sender) for N-node mesh topologies. Loop prevention
uses the cluster ID embedded in each revision.

### Conflict Resolution

Incoming peer records are applied using `put_if_newer` (LWW). A record is accepted only if its
revision is strictly newer than the existing revision for that key. Rejected records increment
the `conflicts_resolved` counter.

### Loop Prevention

Each AOL record carries the originating cluster ID in its revision. When a peer receives a record
whose cluster ID matches its own, the record is silently dropped (it originated locally and was
forwarded back by another peer).

### Checkpoint Persistence

Each peer connector maintains a `peer_checkpoint` file (16-byte big-endian u128 revision). The
checkpoint is saved on clean shutdown (`stop_replication`), on session death (before backoff),
and on clean exit from the connector loop. On startup, the connector loads the checkpoint to
enable incremental sync.

### Bidirectional Connections

In a two-node setup, each node runs both a `PeerListener` and a `PeerConnector`, resulting in
two TCP connections (A→B and B→A). Each connection independently handles initial sync and live
streaming. This is redundant but harmless — LWW ensures idempotent application of records.
