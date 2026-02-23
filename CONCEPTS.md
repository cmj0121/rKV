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

The fundamental unit is a `(key, value)` pair. Keys are byte strings; values are arbitrary byte vectors. The store
provides `put`, `get`, `delete`, `exists`, `scan`, and `count` operations.

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
