# rKV Workspace

A family of data services built on a shared storage foundation.

## Projects

### ![rKV](docs/icon-rkv.svg) [rKV](rkv/) — Key-Value Store

A persistent, revision-aware key-value store in a single binary. LSM-tree
architecture with peer replication, per-namespace encryption, content-addressed
dedup, and pluggable I/O. Ships as an embedded Rust library, REPL, HTTP API
with Web UI, and FFI bindings (C/Python/Go).

### ![Rill](docs/icon-rill.svg) [Rill](rill/) — Message Queue

An unlimited message queue powered by rKV. Simple FIFO interface — push data
in, pop data out. Supports embedded and remote rKV backends, role-based auth,
and batch operations.

### ![Knot](docs/icon-knot.svg) [Knot](knot/) — Graph Database

A schema-free, graph-based, temporal database built on rKV. Store nodes in
tables, connect them with link tables, traverse the graph, and query any point
in time. Flat properties keep every field indexable; relationships are
first-class.

## Architecture

```text
┌─────────┐  ┌─────────┐  ┌─────────┐
│  Knot   │  │  Rill   │  │   CLI   │
│ (graph) │  │ (queue) │  │  / FFI  │
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     └────────────┼────────────┘
                  │
           ┌──────┴──────┐
           │     rKV     │
           │  (storage)  │
           └─────────────┘
```

Knot and Rill are independent services that use rKV as their storage backend —
either embedded (library) or remote (HTTP). The rKV binary also serves its own
REPL, HTTP API, and Web UI directly.

## Quick Start

```sh
# Build the workspace
cargo build --workspace

# Run rKV
cargo run --bin rkv -- serve --ui

# Run Rill
cargo run --bin rill -- serve --ui

# Run Knot
cargo run --bin knot -- serve

# Docker (all services)
docker compose up --build
```

## DDD (Dream-Driven Development)

This project follows the DDD (Dream-Driven Development) methodology, which
means the project is driven by what I envision.

All features are based on my needs and my dreams.
