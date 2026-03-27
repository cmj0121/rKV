# HTTP API

Knot runs its own HTTP server with rKV embedded. A remote mode (Knot connects
to rKV over HTTP) is on the roadmap but not in v1.

## URL structure

Routes use single-letter type prefixes:

- `/m/` — metadata (schema operations)
- `/t/` — data table access
- `/l/` — link entry access
- `/g/` — graph traversal

## Namespace routes

| Method | Path              | Description                          |
| ------ | ----------------- | ------------------------------------ |
| GET    | `/api/namespaces` | List namespaces                      |
| POST   | `/api/namespaces` | Create namespace `{"name":"campus"}` |
| DELETE | `/api/{ns}`       | Drop namespace and everything within |

## Metadata — tables

| Method | Path                         | Description                                       |
| ------ | ---------------------------- | ------------------------------------------------- |
| GET    | `/api/{ns}/m/tables`         | List tables (`?detail=true` for full metadata)    |
| POST   | `/api/{ns}/m/tables`         | Create `{"name":"person", "if_not_exists":false}` |
| DELETE | `/api/{ns}/m/tables/{table}` | Drop table (cascades to link tables)              |

## Metadata — link tables

| Method | Path                       | Description                       |
| ------ | -------------------------- | --------------------------------- |
| GET    | `/api/{ns}/m/links`        | List link tables (`?detail=true`) |
| POST   | `/api/{ns}/m/links`        | Create (see body below)           |
| DELETE | `/api/{ns}/m/links/{link}` | Drop link table                   |
| PATCH  | `/api/{ns}/m/links/{link}` | Alter (bidirectional, cascade)    |

Create link body:

```text
{"name":"attends", "source":"person", "target":"school",
 "bidirectional":false, "cascade":false, "if_not_exists":false}
```

## Metadata — indexes

| Method | Path                                         | Description             |
| ------ | -------------------------------------------- | ----------------------- |
| GET    | `/api/{ns}/m/indexes`                        | List all indexes        |
| POST   | `/api/{ns}/m/indexes`                        | Create (see body below) |
| DELETE | `/api/{ns}/m/indexes/{type}/{name}/{fields}` | Drop index              |

Create index body:

```text
{"table":"person", "fields":["age"], "sparse":false}
{"link":"attends", "fields":["year"], "sparse":false}
{"table":"school", "field":"location", "spatial":true}
```

## Data table CRUD

| Method | Path                        | Description                               |
| ------ | --------------------------- | ----------------------------------------- |
| GET    | `/api/{ns}/t/{table}/{key}` | Get node                                  |
| PUT    | `/api/{ns}/t/{table}/{key}` | Insert/replace (body: properties or null) |
| PATCH  | `/api/{ns}/t/{table}/{key}` | Update (body: partial properties)         |
| DELETE | `/api/{ns}/t/{table}/{key}` | Delete (`?cascade=true`)                  |
| HEAD   | `/api/{ns}/t/{table}/{key}` | Check existence                           |
| GET    | `/api/{ns}/t/{table}`       | Scan/query/count (see query params below) |

Scan/query params: `?prefix=`, `?filter.{field}.{op}={value}`, `?sort={field}`,
`?order=asc|desc`, `?limit=`, `?cursor=`, `?detail=true`, `?count=true`,
`?project={field},{field}`.

Filter operators: `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `like`, `in`,
`exists`, `near`, `within`.

PUT with `?ttl=60s` sets a time-to-live.

## Link entry CRUD

| Method | Path                             | Description                                    |
| ------ | -------------------------------- | ---------------------------------------------- |
| GET    | `/api/{ns}/l/{link}/{from}/{to}` | Get link entry                                 |
| PUT    | `/api/{ns}/l/{link}/{from}/{to}` | Insert/replace (body: properties or null)      |
| PATCH  | `/api/{ns}/l/{link}/{from}/{to}` | Update (body: partial properties)              |
| DELETE | `/api/{ns}/l/{link}/{from}/{to}` | Delete (`?cascade=true`)                       |
| GET    | `/api/{ns}/l/{link}`             | Scan links (`?from=&to=&filter.*&sort&detail`) |

Reverse queries (`?to=`) work on all link tables including directional ones —
they return empty results if no links exist in that direction, never an error.

## Graph traversal

| Method | Path                                   | Description                   |
| ------ | -------------------------------------- | ----------------------------- |
| GET    | `/api/{ns}/g/{table}/{key}/{links...}` | Directed traversal            |
| GET    | `/api/{ns}/g/{table}/{key}`            | Discovery (`?max=N` required) |
| GET    | `/api/{ns}/g/next/{cursor}`            | Fetch next page               |

Traversal params: `?detail=true` (include paths), `?page_size=40`,
`?max=N` (discovery), `?bidi=true` (discovery both directions),
`?filter.{field}.{op}={value}` (link property filter).

Bidirectional links use `<->` between path segments instead of `/`:

```text
Directed:   GET /api/campus/g/person/alice/attends/located-in
BIDI:       GET /api/campus/g/person/alice<->friends/attends
Discovery:  GET /api/campus/g/person/alice?max=2
Discovery+: GET /api/campus/g/person/alice?max=2&bidi=true
```

Note: `<` and `>` are percent-encoded by most HTTP clients; the server accepts
both raw and encoded forms. The server splits the link chain on both `/` and
`<->` when parsing path segments.

## Temporal routes

| Method | Path                                                  | Description                                 |
| ------ | ----------------------------------------------------- | ------------------------------------------- |
| GET    | `/api/{ns}/t/{table}/{key}?at={timestamp}`            | Point-in-time node                          |
| GET    | `/api/{ns}/t/{table}/{key}/history`                   | List revisions                              |
| GET    | `/api/{ns}/t/{table}/{key}/history?from={ts}&to={ts}` | Revision range                              |
| GET    | `/api/{ns}/g/{table}/{key}/{links...}?at={timestamp}` | Temporal traversal                          |
| POST   | `/api/{ns}/compact`                                   | Compact `{"before":"2024-01-01T00:00:00Z"}` |

## Batch operations

| Method | Path              | Description   |
| ------ | ----------------- | ------------- |
| POST   | `/api/{ns}/batch` | Execute batch |

```text
Request:  {"ops": [
  {"op":"put", "table":"person", "key":"alice", "properties":{"age":30}},
  {"op":"put-link", "link":"attends", "from":"alice", "to":"mit"},
  {"op":"update", "table":"person", "key":"alice", "changes":{"age":31}},
  {"op":"del", "table":"person", "key":"bob", "cascade":true},
  {"op":"del-link", "link":"attends", "from":"bob", "to":"mit"}
]}
Response: {"results": [
  {"op":"put", "key":"alice", "ok":true},
  {"op":"put-link", "from":"alice", "to":"mit", "ok":true},
  ...
]}
```

Not a transaction — operations apply in order, failures do not roll back prior ops.

## Health

| Method | Path      | Description  |
| ------ | --------- | ------------ |
| GET    | `/health` | Health check |

## Response conventions

- **200** — success with body
- **201** — created (body: created metadata)
- **404** — not found
- **400** — invalid request
- **409** — conflict (table exists, index exists)
- **500** — storage error

All responses are JSON. Error body: `{"error":"description"}`.
POST creation routes return 201 with the created metadata as body.
Cursor `null` means no more results.

## Response body shapes

```text
GET node:     {"key":"alice", "properties":{"age":30}}
              {"key":"charlie", "properties":null}        (set mode)

GET link:     {"from":"alice", "to":"mit", "properties":{"year":2020}}
              {"from":"alice", "to":"mit", "properties":null}  (bare)

Scan:         {"keys":["alice","bob"], "has_more":true, "cursor":"..."}
Scan detail:  {"entries":[{"key":"alice","properties":{...}}, ...],
               "has_more":false, "cursor":null}
Count:        {"count":42}

Traversal:    {"leaves":["cambridge"], "cursor":"abc123"}
Traversal     {"leaves":["cambridge"],
  detail:       "paths":[["alice","mit","cambridge"]],
                "cursor":null}

History:      {"revisions":[{"timestamp":1234567890,"properties":{...}}, ...]}

Batch:        {"results":[{"op":"put","key":"alice","ok":true}, ...]}
Error:        {"error":"table 'person' does not exist"}
```
