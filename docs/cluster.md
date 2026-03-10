# Cluster / Sharding

> Cluster architecture specification for [rKV](../CONCEPTS.md). For replication within a shard
> group, see the [Replication](replication.md) document.

## Overview

rKV's replication modes (primary-replica and peer) replicate **all** data to every node. This
provides high availability but does not scale storage beyond single-node capacity. Cluster mode
introduces **namespace-level sharding** — distributing namespaces across node groups so each
group holds a subset of the data, while a gateway layer presents a unified API to clients.

### Design Principles

1. **Build on existing primitives** — a shard group IS a peer cluster (or primary + replicas).
   No new intra-group replication protocol is needed.
2. **Namespace is the shard unit** — rKV already isolates everything by namespace (MemTable,
   SSTables, object store, encryption state, AOL records, HTTP routes). Sharding at the
   namespace level preserves scan/rscan correctness and avoids splitting key ranges.
3. **Incremental adoption** — start with static routing and offline migration, add dynamic
   routing and online migration later.
4. **Pragmatic trade-offs** — namespace-level sharding is coarser than hash or range sharding,
   but avoids the complexity of cross-shard scans, metadata services, and key redistribution.

### Why Namespace-Level (Not Hash or Range)

| Approach      | Pros                           | Cons for rKV                       |
| ------------- | ------------------------------ | ---------------------------------- |
| Hash          | Even distribution              | Breaks prefix scans, resharding    |
| Range         | Ordered scans possible         | Metadata service, range splits     |
| **Namespace** | Reuses isolation, scans intact | Coarse-grained, hot ns = hot shard |

## Cluster Topology

```text
                        ┌──────────────┐
                        │ Load Balancer│
                        └──────┬───────┘
                               │
              ┌────────────────┼────────────────┐
              │                                 │
       ┌──────▼──────┐                  ┌──────▼──────┐
       │   Gateway   │                  │   Gateway   │
       │ (stateless) │                  │ (stateless) │
       └──────┬──────┘                  └──────┬──────┘
              │  route by                      │
              │  hash(namespace) % N           │
              └───────────┬────────────────────┘
                          │
           ┌──────────────┼──────────────┐
           │                             │
   ┌───────▼──────┐             ┌───────▼──────┐
   │ Shard Group 1│             │ Shard Group 2│
   │              │             │              │
   │  "users"     │             │  "orders"    │
   │  "sessions"  │             │  "products"  │
   │              │             │              │
   │ ┌──────────┐ │             │ ┌──────────┐ │
   │ │  Peer A  │◄── LWW ──────►│  Peer C  │ │
   │ │ (master) │ │   sync      │ │ (master) │ │
   │ └──────────┘ │             │ └──────────┘ │
   │      ▲       │             │      ▲       │
   │  LWW │ sync  │             │  LWW │ sync  │
   │      ▼       │             │      ▼       │
   │ ┌──────────┐ │             │ ┌──────────┐ │
   │ │  Peer B  │ │             │ │  Peer D  │ │
   │ │ (master) │ │             │ │ (master) │ │
   │ └──────────┘ │             │ └──────────┘ │
   └──────────────┘             └──────────────┘
```

### Node Roles

- **Gateway** — Stateless HTTP proxy. Extracts namespace from URL, looks up
  routing table, forwards to the owning shard group. Multiple gateways sit
  behind a load balancer.
- **Shard Node** — Owns a set of namespaces. Runs the full rKV engine with
  peer or primary-replica replication within its shard group.
- **Combined** — A shard node that also acts as a gateway — routes requests
  for remote namespaces to other shard groups while serving local namespaces
  directly. Useful for small clusters.

### Shard Groups

A shard group is 1–3 nodes using existing peer replication (or primary + replicas) for
intra-group HA. Each shard group:

- Owns a configured set of namespaces
- Handles all reads and writes for those namespaces
- Replicates data within the group using existing replication
- Is unaware of other shard groups (routing is the gateway's responsibility)

### Namespace Ownership

Each namespace maps to exactly one shard group. The mapping is stored in a **routing table**
distributed to all gateways. A **default shard group** catches namespaces not explicitly
assigned — useful for dynamic namespace creation.

## Routing

### Gateway Proxy

The gateway is HTTP middleware that intercepts incoming requests, extracts the namespace from
the URL path (`/api/{ns}/...`), and either:

1. **Proxies** the request to a healthy node in the owning shard group, or
2. **Handles locally** if the gateway is also a shard node that owns the namespace (combined mode)

Non-namespaced routes (`/health`, `/metrics`, `/ui`) are handled locally by the gateway.
Admin routes (`/api/admin/*`) are handled by the gateway and may fan out to shard groups.

### MOVED Responses

When a shard node receives a request for a namespace it does not own, it returns:

```http
HTTP/1.1 307 Temporary Redirect
Location: http://shard2-a:8321/api/orders/keys/foo
X-RKV-Shard: 2
```

Smart clients can cache the `X-RKV-Shard` header to route subsequent requests directly,
bypassing the gateway for hot paths. The 307 preserves the HTTP method (PUT, DELETE, etc.).

### Routing Table

```rust
struct RoutingTable {
    version: u64,
    routes: HashMap<String, ShardGroup>,
    default_group: ShardGroup,
}
```

- **version**: Monotonically increasing counter, incremented on every route change.
- **routes**: Maps namespace name to its owning shard group.
- **default_group**: Handles namespaces not in the routes map.

Gateways load the routing table from the cluster config file on startup and refresh it
via admin endpoints or gossip (Phase 5).

### Admin Endpoints

| Endpoint             | Method | Description                                                     |
| -------------------- | ------ | --------------------------------------------------------------- |
| `/api/admin/cluster` | `GET`  | Returns cluster state: routing table, node health, shard groups |
| `/api/admin/route`   | `POST` | Updates a namespace-to-shard mapping (increments version)       |
| `/api/admin/migrate` | `POST` | Triggers namespace migration (see Migration below)              |

## Cluster Configuration

Static TOML config file, loaded at startup via `--cluster-config`:

```toml
[cluster]
shard_group = 1

[[cluster.nodes]]
addr = "10.0.0.1:8321"
cluster_id = 1
shard_group = 1
namespaces = ["users", "sessions"]

[[cluster.nodes]]
addr = "10.0.0.2:8321"
cluster_id = 2
shard_group = 1
namespaces = ["users", "sessions"]

[[cluster.nodes]]
addr = "10.0.0.3:8321"
cluster_id = 10
shard_group = 2
namespaces = ["orders", "products"]

[[cluster.nodes]]
addr = "10.0.0.4:8321"
cluster_id = 11
shard_group = 2
namespaces = ["orders", "products"]
```

### CLI Arguments

| Flag                 | Default  | Description                                       |
| -------------------- | -------- | ------------------------------------------------- |
| `--shard-group`      | _(none)_ | Shard group ID for this node                      |
| `--cluster-config`   | _(none)_ | Path to cluster TOML config file                  |
| `--owned-namespaces` | _(none)_ | Comma-separated list of namespaces this node owns |

### New Config Fields

| Field              | Type              | Default | Description                   |
| ------------------ | ----------------- | ------- | ----------------------------- |
| `shard_group`      | `u16`             | 0       | Shard group ID                |
| `cluster_config`   | `Option<PathBuf>` | `None`  | Path to cluster config file   |
| `owned_namespaces` | `Vec<String>`     | `[]`    | Namespaces owned by this node |

## Health Monitoring

A background thread polls `GET /health` on all known nodes at a configurable interval
(default 5 seconds).

### Health State Machine

```text
healthy ──[3 consecutive failures]──> unhealthy ──[1 success]──> healthy
```

- **Unhealthy** nodes are skipped in routing (gateway selects another node in the shard group).
- If all nodes in a shard group are unhealthy, the gateway returns **503 Service Unavailable**
  for that namespace.
- Health checks are lightweight — the `/health` endpoint already exists and returns JSON with
  `status`, `role`, and `uptime_secs`.

### Extended Health Response

In cluster mode, the health response includes shard group membership:

```json
{
  "status": "ok",
  "role": "peer",
  "uptime_secs": 3600,
  "shard_group": 1,
  "owned_namespaces": ["users", "sessions"]
}
```

## Namespace Migration

Migration moves a namespace from one shard group to another. The existing SSTable/object
streaming protocol (used for full sync in replication) provides the transport layer.

### Trigger

```http
POST /api/admin/migrate
Content-Type: application/json

{
  "namespace": "orders",
  "target_group": 2
}
```

### Phase 1: Offline Migration

Brief unavailability during transfer. Suitable for maintenance windows.

**Steps:**

1. Gateway marks namespace as **migrating** (returns 503 for that namespace)
2. Source shard flushes the namespace (memtable to SSTables)
3. Source streams SSTables and bin objects to a node in the target group, reusing the
   existing `SstChunk` / `ObjectChunk` wire protocol
4. Target node loads the received files and registers the namespace
5. Gateway updates the routing table (namespace now points to target group)
6. Source drops the namespace locally
7. Gateway removes the migrating flag

### Phase 2: Online Migration (Zero-Downtime)

Leverages the existing `broadcast_aol` pattern, scoped to one namespace:

1. Source begins **dual-write**: new writes for the migrating namespace are forwarded to
   both the source shard group and a designated node in the target group
2. Source streams existing SSTables/objects to target (background, while serving reads)
3. Once caught up, gateway atomically switches the routing table to the target group
4. Source stops dual-write and drops the namespace

During dual-write, reads continue from the source. After switchover, reads go to the target.
The brief overlap window is safe because LWW conflict resolution handles any duplicates.

## Failure Handling

- **Shard node failure** — Existing peer replication provides HA within the
  shard group. Gateway routes to a surviving peer.
- **Gateway failure** — Stateless, multiple gateways behind a load balancer.
  No data loss, no state to recover.
- **Split brain** — Within a shard group, existing LWW conflict resolution
  handles reconciliation. Cross-shard split brain is impossible (namespaces
  don't span groups).
- **Stale routing** — MOVED responses self-correct. A client or gateway with
  an outdated routing table gets a 307, follows it, and optionally caches the
  new route. Small latency bump, no errors.
- **All nodes in shard down** — Gateway returns 503 for namespaces owned by
  that group. Other shard groups are unaffected.

## Wire Protocol Extensions

Intra-shard replication uses the existing binary protocol — no changes needed.
Gateway-to-shard routing uses HTTP — no binary protocol needed.

Future protocol extensions for gossip-based membership discovery:

| Type                 | Code   | Direction | Description                                                    |
| -------------------- | ------ | --------- | -------------------------------------------------------------- |
| `ClusterRouteUpdate` | `0x10` | Broadcast | Routing table diff (version, added/removed namespace mappings) |
| `ClusterHeartbeat`   | `0x11` | Broadcast | Node health + shard group membership announcement              |

These are optional (Phase 5) and would replace the static config file + admin endpoint
approach with automatic membership discovery.

## New Types

```rust
/// Maps namespaces to shard groups for request routing.
struct RoutingTable {
    version: u64,
    routes: HashMap<String, ShardGroup>,
    default_group: ShardGroup,
}

/// A group of nodes that collectively own a set of namespaces.
struct ShardGroup {
    id: u16,
    nodes: Vec<NodeInfo>,
}

/// A node in the cluster with health tracking.
struct NodeInfo {
    addr: String,
    cluster_id: u16,
    healthy: bool,
}
```

### New Error Variants

| Variant                   | Description                                                               |
| ------------------------- | ------------------------------------------------------------------------- |
| `NotMyShard(String, u16)` | Namespace is owned by a different shard group (triggers MOVED response)   |
| `ClusterError(String)`    | General cluster operation failure (config parse, migration, health check) |

## Implementation Phases

| Phase | Scope                                                                 | Builds On                           |
| ----- | --------------------------------------------------------------------- | ----------------------------------- |
| 1     | Routing table + gateway proxy middleware + MOVED responses + CLI args | Existing HTTP server                |
| 2     | Health monitoring background thread + dynamic routing updates         | Phase 1                             |
| 3     | Namespace migration (offline) via existing SST/object streaming       | Phase 2 + existing replication      |
| 4     | Online migration with dual-write                                      | Phase 3                             |
| 5     | Gossip-based membership (optional)                                    | Phase 2 + existing peer connections |

Each phase is independently deployable. Phases 1–2 provide a functional cluster.
Phase 3 adds operational flexibility. Phase 4 enables zero-downtime operations.
Phase 5 removes the need for static configuration.

## Interaction with Existing Features

| Feature            | Impact                                                                           |
| ------------------ | -------------------------------------------------------------------------------- |
| CLI / REPL         | Connect to gateway or shard node directly. `use ns` works via routing.           |
| HTTP API           | Transparent. Gateway routes by `{ns}` path parameter.                            |
| Web UI             | Shows namespaces on connected node. Full cluster view planned for Phase 2.       |
| Peer replication   | Unchanged within shard group. Shard group = peer cluster.                        |
| Primary-replica    | Unchanged within shard group. Alternative to peer mode.                          |
| WriteBatch         | Scoped to single namespace = single shard. No cross-shard batches.               |
| Encryption         | Per-namespace, stays within shard group. No cross-shard key management.          |
| Bin objects        | Per-namespace object store, stays within shard group. Migrated with namespace.   |
| Prometheus metrics | Each node exposes its own metrics. Gateway aggregation planned for Phase 2.      |
| Docker Compose     | Single file with profiles: default = replication, `--profile cluster` = sharded. |
| Helm               | Future: add `shardGroup` value to chart.                                         |

## Limitations (By Design)

- No cross-namespace transactions or batch writes across shard groups
- No automatic namespace-to-shard assignment (admin-configured)
- No automatic shard splitting (namespace is the smallest unit)
- No cross-shard scan/rscan (each namespace is fully contained in one shard)
- Gateway aggregation for `GET /api/namespaces` requires querying all groups
- Hot namespace = hot shard (acceptable trade-off for simplicity)

## Docker Compose Cluster Profile

A single `docker-compose.yml` supports both topologies via Docker Compose profiles:

- `docker compose up` — **default**: 1 primary + 2 replicas (primary-replica replication)
- `docker compose --profile cluster up` — **cluster**: default services + 2 shard groups

### Default Services (always run)

| Service      | Role    | Cluster ID | Port | Notes              |
| ------------ | ------- | ---------- | ---- | ------------------ |
| `primary`    | Primary | 1          | 8321 | Default shard, R+W |
| `replica-01` | Replica | 2          | 8324 | Read-only          |
| `replica-02` | Replica | 3          | 8325 | Read-only          |

### Cluster Services (with `--profile cluster`)

| Service    | Role | Shard Group | Cluster ID | Port | Namespaces       |
| ---------- | ---- | ----------- | ---------- | ---- | ---------------- |
| `shard1-a` | Peer | 1           | 10         | 8331 | users, sessions  |
| `shard1-b` | Peer | 1           | 11         | 8332 | users, sessions  |
| `shard2-a` | Peer | 2           | 20         | 8333 | orders, products |
| `shard2-b` | Peer | 2           | 21         | 8334 | orders, products |

Cluster IDs use tens-based grouping (10–11 for shard 1, 20–21 for shard 2) to visually
distinguish shard groups in logs and RevisionIDs.

### Makefile Targets

| Target              | Command                                          |
| ------------------- | ------------------------------------------------ |
| `make cluster-up`   | `docker compose --profile cluster up --build -d` |
| `make cluster-down` | `docker compose --profile cluster down`          |
