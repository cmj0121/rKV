# Rill

![Rill](../docs/icon-rill.svg)

> Save your time, not your data.

The unlimited message queue for your data.

The **Rill** project is an unlimited message queue for your data. It provides a simple and efficient way to store
and retrieve data with high performance and reliability based on the rKV. All the data is stored in the rKV, and
Rill provides a simple FIFO queue interface — push data in, pop data out.

All you need to do is put your data into the Rill, and take it out when you need it. The Rill will take care of
the rest, ensuring that your data is stored safely and can be retrieved quickly.

## rKV Backend

Rill uses rKV as its storage backend. Two modes are available:

### Embed Mode (default)

Rill opens an embedded rKV database directly. No separate server needed.
Best for standalone deployments, development, and lightweight use cases.

```yaml
rkv:
  mode: embed
  data: ./rill-data
  storage:
    cache_size: 64mb
    compression: lz4
```

### Remote Mode

Rill connects to an external rKV HTTP server. The rKV server manages storage,
replication, and clustering independently. Best for large-scale deployments
where you want shared storage, horizontal scaling, or separation of concerns.

```yaml
rkv:
  mode: remote
  url: http://rkv-server:8321
```

Generate a default config with `rill init > rill.yaml`, then customize and
start with `rill --config rill.yaml serve`.

## DDD (Dream-Driven Development)

This project follows the DDD (Dream-Driven Development) methodology, which means the project
is driven by what I envision.

All features are based on my needs and my dreams.
