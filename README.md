# rKV (revisioned key-value store)

> Simple, fast, and embeddable revision data storage

**rKV** is a persistent, revision-aware key-value store built on LSM-tree architecture.
It can be used as an embedded Rust library, through FFI bindings (C/Python/Go), or as
a standalone CLI tool.

## Quick Start

You can just run the rKV in the command-line, like this:

```sh
# Install the rKV CLI tool
cargo install --path rkv

# run the rKV in the default settings
rkv
> put name "hello world"
> get name
hello world
> scan name*
name
> exit
```

## Concept

- **Revision-aware** — every write produces a unique RevisionID; query history with `rev_get`/`rev_count`
- **Dual key types** — `Int(i64)` for ordered mode, `Str` for unordered; first Str key triggers irreversible auto-upgrade
- **Namespace isolation** — isolated key spaces within one DB, created implicitly on first use
- **Single-key operations** — every operation targets exactly one key

The following features are intentionally **not supported**:

| Feature                | Why not                                            |
| ---------------------- | -------------------------------------------------- |
| Batch operations       | No WriteBatch, mget, mput, or mdel                 |
| Compare-and-swap (CAS) | RevisionID is for history, not concurrency control |
| Iterator / Cursor      | Bounded `scan`/`rscan` with limit is sufficient    |
| Snapshots              | Every read sees the latest state                   |
| Watch / Subscribe      | rKV is a storage engine, not a message broker      |

For full architecture and design details, see [CONCEPTS.md](CONCEPTS.md).

## DDD (Dream-Driven Development)

This project follows the DDD (Dream-Driven Development) methodology, which means the project
is driven by what I envision.

All features are based on my needs and my dreams.
