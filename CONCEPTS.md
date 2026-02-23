# Concepts

## Overview

**rKV** is a persistent, revision-aware key-value store built on LSM-tree architecture. It is designed to be embedded
directly into Rust applications, accessed through FFI bindings (C/Python/Go), or used as a standalone CLI tool.

Every write produces a new **revision**, enabling history queries and compare-and-swap (CAS) operations without
external coordination.

## Architecture

```text
                     ┌───────┐   ┌──────────────┐
   Client API ──────►│  AOL  │──►│  WriteBuffer │──► Response
                     └───────┘   └──────┬───────┘
                                        │ background flush
                                 ┌──────▼───────┐
                                 │   L1 SSTable │
                                 └──────┬───────┘
                                        │ merge
                                 ┌──────▼───────┐
                                 │   L2 SSTable │
                                 └──────┬───────┘
                                        │ merge
                                 ┌──────▼───────┐
                                 │   L3 SSTable │
                                 └──────────────┘
```

- **Write path**: Client -> AOL (append-only log, fsync for durability) -> WriteBuffer (in-memory) -> respond to
  caller. Background flush moves WriteBuffer to L1 SSTable; merge compacts L1->L2->L3.
- **Read path**: WriteBuffer -> frozen buffer -> SSTable files (newest first), with a block cache for decompressed
  blocks.
- **Revisions**: Each key-value pair carries a monotonically increasing revision ID. Reads return the latest revision
  by default; history queries retrieve older revisions.

## Core Concepts

### Key-Value Store

The fundamental unit is a `(key, value)` pair. The store provides `put`, `get`, `delete`, `exists`, `scan`, and `count`
operations.

### Key

A Key identifies a record. It is one of two variants:

- **Int** — a signed 64-bit integer (`i64`). Supports ordering and comparison.
- **Str** — a UTF-8 string (max 255 bytes, no interior nulls). No ordering guarantee.

Booleans are syntax sugar: `true` → `Int(1)`, `false` → `Int(0)`.

**Auto-upgrade**: A database starts in **ordered mode** where all keys are `Int` and support comparison. When the first
`Str` key is inserted, the engine performs an irreversible **key type upgrade**: all existing `Int` keys are widened to
`Str` (e.g., `Int(42)` becomes `Str("42")`), and the database enters **unordered mode** permanently. Exact-match
operations (`get`, `exists`, `delete`) continue to work normally in both modes.

**Scan behavior** depends on the database mode:

- **Ordered mode** (Int keys): `scan(prefix, limit)` returns keys in ascending order starting from `prefix`;
  `rscan` returns keys in descending order.
- **Unordered mode** (Str keys): `scan(prefix, limit)` returns keys whose string representation starts with `prefix`;
  `rscan` returns the same prefix-matched keys in reverse order. Ordering-based range queries are not available.

### Value

A Value is the payload associated with a key. It has three internal states:

- **Data** — arbitrary-length byte vector. An empty `Data` (zero bytes) is a valid, distinct value.
- **Null** — the key exists but carries no payload. `Null` is semantically different from empty `Data`.
- **Tombstone** — an internal deletion marker. When a key is deleted, the engine writes a tombstone instead of
  physically removing the entry. Tombstones are invisible to the public API — `get` on a tombstoned key returns
  "key not found", indistinguishable from a key that never existed. Tombstones are resolved (garbage-collected) during
  SSTable compaction.

### Revision Awareness

Every mutation produces a **Revision ID** — an unsigned 128-bit integer (`u128`) that increases monotonically. `put`
returns the new revision ID to the caller. `get` returns the latest value; `rev <key>` retrieves the full history.
CAS uses revision IDs for optimistic concurrency control.

### LSM-Tree Storage

Data is organized in levels (L1-L3). Fresh writes land in an in-memory buffer and are periodically flushed to sorted
SSTable files on disk. Background merge compaction keeps read amplification bounded.

### Embeddable Library

The engine is a Rust library crate (`rkv`) that can be linked into any Rust program. FFI bindings expose the same API
to C, Python, and Go consumers.

### CLI Tool

A REPL binary built on top of the library provides interactive access for debugging, exploration, and scripting.

## Design Decisions

- **Interior mutability**: `DB` is `Send + Sync`. All mutable fields use `Mutex<T>` or `RwLock<T>` so public methods
  take `&self`.
- **Stub-first development**: The initial scaffold returns `NotImplemented` for all engine methods, allowing the CLI
  and test harness to be built before any storage logic exists.
- **Binary/library boundary**: The CLI (`main.rs`) is strictly binary-only. `lib.rs` exports only engine types
  (`DB`, `Config`, `Error`, `Result`). Nothing from the REPL leaks into the library or FFI surface.
