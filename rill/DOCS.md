# Rill API Documentation

## Overview

Rill is a FIFO message queue powered by rKV. Push data in, pop data out.

## CLI

### Global Options

| Option     | Env Var       | Description                     |
| ---------- | ------------- | ------------------------------- |
| `--config` | `RILL_CONFIG` | Path to config file (YAML/TOML) |

### `rill init`

Generate a default config file to stdout.

```sh
rill init > rill.yaml
rill init --format toml > rill.toml
```

| Option     | Default | Description               |
| ---------- | ------- | ------------------------- |
| `--format` | `yaml`  | Output format (yaml/toml) |

### `rill serve`

Start the HTTP server.

```sh
rill serve [OPTIONS]
rill --config rill.yaml serve
```

| Option           | Env Var             | Default                 | Description                  |
| ---------------- | ------------------- | ----------------------- | ---------------------------- |
| `--host`         | `RILL_HOST`         | `0.0.0.0`               | Bind address                 |
| `--port`         | `RILL_PORT`         | `3000`                  | Listen port                  |
| `--admin-token`  | `RILL_ADMIN_TOKEN`  |                         | Admin bearer token           |
| `--writer-token` | `RILL_WRITER_TOKEN` |                         | Writer bearer token          |
| `--reader-token` | `RILL_READER_TOKEN` |                         | Reader bearer token          |
| `--ui`           | `RILL_UI`           | `false`                 | Enable web UI at /ui         |
| `--rkv-mode`     | `RILL_RKV_MODE`     | `embed`                 | Backend mode (embed/remote)  |
| `--data`         | `RILL_DATA`         | `./rill-data`           | Data directory (embed mode)  |
| `--rkv-url`      | `RILL_RKV_URL`      | `http://localhost:8321` | rKV server URL (remote mode) |

## Authentication

All authenticated endpoints require a `Authorization: Bearer <token>` header.

If no tokens are configured, all endpoints are open (no auth enforced).

### Roles

| Role   | Description                              |
| ------ | ---------------------------------------- |
| admin  | Full access — queue management + all ops |
| writer | Push messages + read ops                 |
| reader | Pop messages + view queue info           |

### Endpoint Permissions

| Endpoint                     | Admin  | Writer | Reader |
| ---------------------------- | ------ | ------ | ------ |
| `POST   /queues`             | yes    | -      | -      |
| `DELETE /queues/:name`       | yes    | -      | -      |
| `GET    /queues`             | yes    | yes    | yes    |
| `POST   /queues/:name`       | yes    | yes    | -      |
| `GET    /queues/:name`       | yes    | yes    | yes    |
| `GET    /queues/:name/info`  | yes    | yes    | yes    |
| `POST   /queues/:name/batch` | yes    | yes    | -      |
| `GET    /queues/:name/batch` | yes    | yes    | yes    |
| `GET    /auth/me`            | public | public | public |
| `GET    /ui`                 | public | public | public |
| `GET    /docs`               | public | public | public |
| `GET    /health`             | public | public | public |
| `GET    /`                   | public | public | public |

## HTTP API

### Auth Info

```http
GET /auth/me
```

Returns the caller's role based on the provided bearer token.

Response: `{"role": "admin", "authenticated": true, "auth_required": true}`

When no token is provided: `{"role": "anonymous", "authenticated": false, "auth_required": true}`

When no tokens are configured (open mode): `{"role": "admin", "authenticated": true, "auth_required": false}`

### Health Check

```http
GET /health
```

Response:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "mode": "embed",
  "queues": 3,
  "uptime_seconds": 120
}
```

### Root

```http
GET /
```

Response: `""` (200, empty body)

### Create Queue

```http
POST /queues
Content-Type: application/json

{"name": "my-queue"}
```

Response: `{"queue": "my-queue", "created": true}`

### Delete Queue

```http
DELETE /queues/:name
```

Response: `{"queue": "my-queue", "deleted": true}`

### List Queues

```http
GET /queues
```

Response: `{"queues": []}`

### Push Message

```http
POST /queues/:name[?ttl=<duration>]

<raw body>
```

Query parameters:

| Parameter | Required | Example                 | Description                         |
| --------- | -------- | ----------------------- | ----------------------------------- |
| `ttl`     | no       | `30s`, `5m`, `1h`, `2d` | Message time-to-live (auto-expires) |

Supported TTL units: `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
If no unit is specified, seconds are assumed.

Response: `{"id": "01jqx...", "pushed": true}`

The `id` field contains a 26-character ULID (monotonic, lexicographically sortable).

### Pop Message

```http
GET /queues/:name
```

Response: `{"message": "hello world"}` (or `{"message": null}` when queue is empty)

### Batch Push

```http
POST /queues/:name/batch
Content-Type: application/json

{"messages": [{"body": "msg1"}, {"body": "msg2", "ttl": "5m"}]}
```

Response: `{"ids": ["01jqx...", "01jqx..."], "count": 2}`

Each item may include an optional `ttl` field (same format as the single push `ttl` query param).
Requires Writer role or above.

### Batch Pop

```http
GET /queues/:name/batch[?count=N]
```

Query parameters:

| Parameter | Required | Default | Max  | Description               |
| --------- | -------- | ------- | ---- | ------------------------- |
| `count`   | no       | 1       | 1000 | Number of messages to pop |

Response: `{"messages": ["msg1", "msg2"], "count": 2}`

Returns up to `count` messages in FIFO order. Messages are removed from the queue.
Requires Reader role or above.

### Queue Info

```http
GET /queues/:name/info
```

Response: `{"queue": "my-queue", "length": 42}`

### API Docs

```http
GET /docs
```

Interactive OpenAPI documentation (Swagger UI). Always available.

The raw OpenAPI spec is at `GET /docs/openapi.yaml`.

### Web UI

```http
GET /ui
```

Returns an HTML dashboard. Requires `--ui` flag; returns 404 if disabled.

## Docker

### Build and Run

```sh
# Using docker-compose (from project root)
docker compose up rill -d

# Standalone
docker build -f rill/Dockerfile -t rill .
docker run -p 3000:3000 \
  -e RILL_ADMIN_TOKEN=secret \
  -e RILL_UI=true \
  rill
```

### Environment Variables

| Variable            | Description                  |
| ------------------- | ---------------------------- |
| `RILL_HOST`         | Bind address                 |
| `RILL_PORT`         | Listen port                  |
| `RILL_ADMIN_TOKEN`  | Admin bearer token           |
| `RILL_WRITER_TOKEN` | Writer bearer token          |
| `RILL_READER_TOKEN` | Reader bearer token          |
| `RILL_UI`           | Enable web UI                |
| `RILL_RKV_MODE`     | Backend mode (embed/remote)  |
| `RILL_DATA`         | Data directory (embed mode)  |
| `RILL_RKV_URL`      | rKV server URL (remote mode) |
| `RILL_CONFIG`       | Path to config file          |

## Examples

```sh
# Start server
rill serve --port 3000 --admin-token secret

# Create a queue
curl -X POST http://localhost:3000/queues \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"name": "tasks"}'

# Push a message
curl -X POST http://localhost:3000/queues/tasks \
  -H "Authorization: Bearer secret" \
  -d "hello world"

# Push a message with 5-minute TTL
curl -X POST "http://localhost:3000/queues/tasks?ttl=5m" \
  -H "Authorization: Bearer secret" \
  -d "expires soon"

# Pop a message
curl http://localhost:3000/queues/tasks \
  -H "Authorization: Bearer secret"

# List queues
curl http://localhost:3000/queues \
  -H "Authorization: Bearer secret"
```
