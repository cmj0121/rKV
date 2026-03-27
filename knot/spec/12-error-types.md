# Error Types

All errors are represented as a single `Error` enum in the Rust API. The HTTP
API maps each variant to an HTTP status code.

## Error enum

| Error               | HTTP | Description                                                     |
| ------------------- | ---- | --------------------------------------------------------------- |
| `NamespaceNotFound` | 404  | Namespace does not exist                                        |
| `TableNotFound`     | 404  | Data table does not exist                                       |
| `LinkTableNotFound` | 404  | Link table does not exist                                       |
| `KeyNotFound`       | 404  | Node key does not exist in table                                |
| `LinkNotFound`      | 404  | Link entry does not exist                                       |
| `TableExists`       | 409  | Table already exists (use `IF NOT EXISTS` to suppress)          |
| `LinkTableExists`   | 409  | Link table already exists (use `IF NOT EXISTS` to suppress)     |
| `IndexExists`       | 409  | Index already exists                                            |
| `NotBidirectional`  | 400  | Used bidirectional syntax on a directional link table           |
| `InvalidKey`        | 400  | Key is empty, contains control characters, or exceeds 511 bytes |
| `InvalidName`       | 400  | Name contains dots, control characters, or exceeds 511 bytes    |
| `InvalidProperty`   | 400  | Property name or value is invalid                               |
| `InvalidFilter`     | 400  | Malformed query condition or filter expression                  |
| `EndpointNotFound`  | 400  | Source or target node does not exist when creating a link       |
| `StorageError`      | 500  | rKV error propagated (I/O, corruption, etc.)                    |

## Error behavior

- **Schema operations** — return the specific error variant. `IF NOT EXISTS`
  suppresses `TableExists`, `LinkTableExists`.
- **Get / exists** — `KeyNotFound` or `LinkNotFound` when the entity does not
  exist. `get()` returns `Option<Node>` (None for not found); `exists()` returns
  `bool`.
- **Insert / update / replace** — `KeyNotFound` on update if the node does not
  exist. Insert creates or overwrites (upsert). `EndpointNotFound` if a link's
  source or target node is missing.
- **Delete** — deleting a non-existent node or link is a no-op (not an error).
- **Cascade** — errors during cascade (e.g., a storage error mid-cascade) stop
  the cascade. Partially completed operations are recovered on restart.
- **Batch** — each operation in a batch reports its own success or failure.
  A failure does not roll back prior operations.
