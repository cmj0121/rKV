# Knot — Technical Specification

> See [knot/README.md](../README.md) for the concept document.

## Sections

1. [Overview](01-overview.md) — terminology, scope, design principles
2. [Architecture](02-architecture.md) — system layers and engine components
3. [Data Model](03-data-model.md) — namespaces, tables, nodes, properties, links
4. [Storage Mapping](04-storage-mapping.md) — how Knot maps to rKV namespaces
5. [Indexing](05-indexing.md) — primary, secondary, composite, spatial
6. [Schema Operations](06-schema-operations.md) — create, drop, alter
7. [Deletion and Cascade](07-deletion-cascade.md) — delete flows, cascade rules
8. [Traversal](08-traversal.md) — directed, discovery, filtering, pagination
9. [Temporal](09-temporal.md) — revisions, point-in-time, temporal traversal
10. [Engine Components](10-engine-components.md) — responsibility matrix
11. [Rust API](11-rust-api.md) — embedded library interface
12. [Error Types](12-error-types.md) — error enum and HTTP mappings
13. [HTTP API](13-http-api.md) — routes, request/response bodies
14. [CLI REPL](14-cli-repl.md) — expression language, schema commands
15. [Open Questions](15-open-questions.md) — prioritized roadmap
