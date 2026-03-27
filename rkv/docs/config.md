# Configuration

rKV supports three layers of configuration, applied in order of precedence
(highest wins):

1. **CLI arguments** — flags passed to `rkv` or `rkv serve`
2. **Environment variables** — `RKV_*` prefixed vars
3. **Config file** — YAML or TOML
4. **Built-in defaults** — hardcoded in `Config::new()`

## Config File

### Formats

Both YAML and TOML are supported. The format is auto-detected by file
extension:

| Extension       | Format |
| --------------- | ------ |
| `.yaml`, `.yml` | YAML   |
| `.toml`         | TOML   |

### Loading Order

1. If `--config <path>` is passed, that file is loaded (error if missing).
2. Otherwise, `~/.rkv_config.yaml` is tried (silently skipped if missing).

### Generating a Template

```bash
# YAML (default)
rkv init > ~/.rkv_config.yaml

# TOML
rkv init --format toml > ~/.rkv_config.toml
```

### Sections

The config file has four top-level sections: `storage`, `server`,
`replication`, and `cluster`. All sections and all fields within them are
optional — omitted fields use built-in defaults.

#### YAML Example

```yaml
storage:
  path: /data/rkv
  write_buffer_size: 4mb
  compression: lz4

server:
  port: 8321
  ui: true

replication:
  role: standalone

cluster:
  shard_group: 0
```

#### TOML Example

```toml
[storage]
path = "/data/rkv"
write_buffer_size = "4mb"
compression = "lz4"

[server]
port = 8321
ui = true

[replication]
role = "standalone"

[cluster]
shard_group = 0
```

### Size Values

Size fields accept either plain integers (bytes) or human-readable strings:

| Format | Example   | Bytes         |
| ------ | --------- | ------------- |
| bytes  | `4194304` | 4,194,304     |
| `kb`   | `4kb`     | 4,096         |
| `mb`   | `4mb`     | 4,194,304     |
| `gb`   | `2gb`     | 2,147,483,648 |

Fractional values like `1.5mb` are supported.

## Environment Variables

Every config field can be overridden via an environment variable with the
format `RKV_<SECTION>_<FIELD>`. Variables are uppercase with underscores.

Invalid values print a warning to stderr and are silently ignored (the
previous value from the config file or built-in default is kept).

### Storage

| Variable                        | Type | Default | Description                              |
| ------------------------------- | ---- | ------- | ---------------------------------------- |
| `RKV_STORAGE_PATH`              | path | —       | Database directory path                  |
| `RKV_STORAGE_CREATE_IF_MISSING` | bool | `true`  | Create directory if missing              |
| `RKV_STORAGE_WRITE_BUFFER_SIZE` | size | `4mb`   | Write buffer size before flush           |
| `RKV_STORAGE_MAX_LEVELS`        | int  | `3`     | Maximum LSM levels                       |
| `RKV_STORAGE_BLOCK_SIZE`        | size | `4kb`   | SSTable block size                       |
| `RKV_STORAGE_CACHE_SIZE`        | size | `8mb`   | Block cache size (0 = disabled)          |
| `RKV_STORAGE_OBJECT_SIZE`       | size | `1kb`   | Bin object size threshold                |
| `RKV_STORAGE_COMPRESS`          | bool | `true`  | LZ4-compress bin objects                 |
| `RKV_STORAGE_BLOOM_BITS`        | int  | `10`    | Bloom filter bits per key                |
| `RKV_STORAGE_BLOOM_PREFIX_LEN`  | int  | `0`     | Prefix bloom length (0 = disabled)       |
| `RKV_STORAGE_VERIFY_CHECKSUMS`  | bool | `true`  | Verify checksums on read                 |
| `RKV_STORAGE_COMPRESSION`       | enum | `lz4`   | Block compression: `none`, `lz4`, `zstd` |
| `RKV_STORAGE_IO_MODEL`          | enum | `mmap`  | I/O strategy: `none`, `directio`, `mmap` |
| `RKV_STORAGE_AOL_BUFFER_SIZE`   | int  | `128`   | AOL flush threshold (records)            |
| `RKV_STORAGE_L0_MAX_COUNT`      | int  | `4`     | L0 SSTable count compaction trigger      |
| `RKV_STORAGE_L0_MAX_SIZE`       | size | `64mb`  | L0 total size compaction trigger         |
| `RKV_STORAGE_L1_MAX_SIZE`       | size | `256mb` | L1 max size before merge to L2           |
| `RKV_STORAGE_DEFAULT_MAX_SIZE`  | size | `2gb`   | Default max size for L2+ levels          |
| `RKV_STORAGE_WRITE_STALL_SIZE`  | size | `8mb`   | Backpressure threshold (0 = off)         |

### Server

| Variable                | Type   | Default     | Description                        |
| ----------------------- | ------ | ----------- | ---------------------------------- |
| `RKV_SERVER_BIND`       | string | `127.0.0.1` | Bind address                       |
| `RKV_SERVER_PORT`       | u16    | `8321`      | Listen port                        |
| `RKV_SERVER_BODY_LIMIT` | size   | `2mb`       | Max request body size              |
| `RKV_SERVER_TIMEOUT`    | u64    | `30`        | Request timeout in seconds (0=off) |
| `RKV_SERVER_UI`         | bool   | `false`     | Enable web UI at `/ui`             |
| `RKV_SERVER_ALLOW_ALL`  | bool   | `false`     | Disable IP restriction             |

### Replication

| Variable                       | Type   | Default      | Description                                      |
| ------------------------------ | ------ | ------------ | ------------------------------------------------ |
| `RKV_REPLICATION_ROLE`         | enum   | `standalone` | Role: `standalone`, `primary`, `replica`, `peer` |
| `RKV_REPLICATION_CLUSTER_ID`   | u16    | (random)     | Cluster ID for RevisionID (0-65535)              |
| `RKV_REPLICATION_REPL_PORT`    | u16    | `8322`       | Replication listen port                          |
| `RKV_REPLICATION_PRIMARY_ADDR` | string | —            | Primary address (replica mode)                   |
| `RKV_REPLICATION_PEERS`        | string | —            | Comma-separated peer addresses                   |

### Cluster

| Variable                       | Type   | Default | Description                      |
| ------------------------------ | ------ | ------- | -------------------------------- |
| `RKV_CLUSTER_SHARD_GROUP`      | u16    | `0`     | Shard group ID (0 = standalone)  |
| `RKV_CLUSTER_OWNED_NAMESPACES` | string | —       | Comma-separated owned namespaces |

### Value Types

| Type   | Accepted values                                |
| ------ | ---------------------------------------------- |
| bool   | `true`, `false`, `1`, `0`, `yes`, `no`         |
| size   | integer (bytes) or string: `1kb`, `4mb`, `2gb` |
| enum   | see individual field descriptions              |
| string | any UTF-8 string                               |
| int    | unsigned integer                               |
| u16    | unsigned 16-bit integer (0-65535)              |
| u64    | unsigned 64-bit integer                        |
| path   | filesystem path                                |

## Docker and Kubernetes

Environment variables are the recommended configuration method for
containerized deployments:

```yaml
# docker-compose.yml
services:
  rkv:
    image: rkv:latest
    environment:
      RKV_STORAGE_PATH: /data/rkv
      RKV_SERVER_BIND: 0.0.0.0
      RKV_SERVER_PORT: "8321"
      RKV_SERVER_ALLOW_ALL: "true"
      RKV_STORAGE_WRITE_BUFFER_SIZE: 16mb
      RKV_STORAGE_CACHE_SIZE: 64mb
```

```yaml
# Helm values.yaml (env section)
env:
  - name: RKV_STORAGE_PATH
    value: /data/rkv
  - name: RKV_SERVER_BIND
    value: 0.0.0.0
  - name: RKV_STORAGE_CACHE_SIZE
    value: 64mb
```

## Precedence Example

Given all three layers:

```bash
# Config file (~/.rkv_config.yaml)
# server:
#   port: 9000

# Environment
export RKV_SERVER_PORT=9500

# CLI
rkv serve --port 8000
```

The effective port is **8000** (CLI wins). If `--port` is omitted, **9500**
(env wins over file). If the env var is also unset, **9000** (file wins over
default). If the file also omits it, **8321** (built-in default).
