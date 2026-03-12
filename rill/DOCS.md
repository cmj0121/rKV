# Rill API Documentation

## Overview

Rill is a FIFO message queue powered by rKV. Push data in, pop data out.

## CLI

```sh
rill serve [OPTIONS]
```

| Option           | Env Var             | Default   | Description             |
| ---------------- | ------------------- | --------- | ----------------------- |
| `--host`         | `RILL_HOST`         | `0.0.0.0` | Bind address            |
| `--port`         | `RILL_PORT`         | `3000`    | Listen port             |
| `--admin-token`  | `RILL_ADMIN_TOKEN`  |           | Admin bearer token      |
| `--writer-token` | `RILL_WRITER_TOKEN` |           | Writer bearer token     |
| `--reader-token` | `RILL_READER_TOKEN` |           | Reader bearer token     |
| `--ui`           | `RILL_UI`           | `false`   | Enable web UI at /admin |

## Authentication

All authenticated endpoints require a `Authorization: Bearer <token>` header.

If no tokens are configured, all endpoints are open (no auth enforced).

### Roles

| Role   | Description                              |
| ------ | ---------------------------------------- |
| admin  | Full access — queue management + all ops |
| writer | Push messages + read ops                 |
| reader | Pop/peek messages only                   |

### Endpoint Permissions

| Endpoint               | Admin  | Writer | Reader |
| ---------------------- | ------ | ------ | ------ |
| `POST   /queues`       | yes    | -      | -      |
| `DELETE /queues/:name` | yes    | -      | -      |
| `GET    /queues`       | yes    | yes    | yes    |
| `POST   /queues/:name` | yes    | yes    | -      |
| `GET    /queues/:name` | yes    | yes    | yes    |
| `GET    /admin`        | yes    | -      | -      |
| `GET    /health`       | public | public | public |
| `GET    /`             | public | public | public |

## HTTP API

### Health Check

```http
GET /health
```

Response: `{"status": "ok"}`

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

Response: `{"message": null}` (or message data when available)

### Admin UI

```http
GET /admin
```

Returns an HTML dashboard. Requires `--ui` flag; returns 404 if disabled.

## Docker

### Build and Run

```sh
# Using docker-compose (from project root)
docker compose --profile rill up -d

# Standalone
docker build -f rill/Dockerfile -t rill .
docker run -p 3000:3000 \
  -e RILL_ADMIN_TOKEN=secret \
  -e RILL_UI=true \
  rill
```

### Environment Variables

| Variable            | Description         |
| ------------------- | ------------------- |
| `RILL_HOST`         | Bind address        |
| `RILL_PORT`         | Listen port         |
| `RILL_ADMIN_TOKEN`  | Admin bearer token  |
| `RILL_WRITER_TOKEN` | Writer bearer token |
| `RILL_READER_TOKEN` | Reader bearer token |
| `RILL_UI`           | Enable admin web UI |

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
