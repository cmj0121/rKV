# Open Questions

Prioritized roadmap for post-v1:

| Priority | Item                                    | Rationale                                        |
| -------- | --------------------------------------- | ------------------------------------------------ |
| P1       | Transaction support                     | Atomic multi-table operations for consistency    |
| P1       | HTTP traversal filters via POST         | Complex filters beyond GET query params          |
| P2       | Aggregation (count, sum, avg, group-by) | Count exists, extend to full aggregation         |
| P2       | Variable binding                        | Name intermediate traversal nodes for projection |
| P2       | Intermediate node filtering             | Filter on mid-path nodes (not just destination)  |
| P2       | Index key encoding specification        | Define order-preserving encoding algorithm       |
| P2       | Change streams / subscriptions          | Subscribe to data changes for reactive apps      |
| P2       | Full-text search                        | Inverted index for string property search        |
| P3       | Variable-length traversal (min..max)    | Specify range of hops, not just max              |
| P3       | Weighted traversal / shortest path      | Graph algorithms using link properties           |
| P3       | Subgraph / pattern matching             | Match structural patterns in the graph           |
| P3       | TTL on links                            | rKV supports it, surface in Knot link API        |
| P3       | Import / export                         | Bulk load from CSV/JSON, export to file          |
| P3       | Permissions / access control            | Per-table or per-namespace rules (server mode)   |
| P3       | Remote mode                             | Knot as HTTP client to rKV instead of embedded   |
| P3       | Computed / virtual properties           | Derived property values                          |
| P3       | Views / saved queries                   | Named reusable queries                           |
| P3       | Geo polygons / lines                    | Extend geo type beyond points                    |
