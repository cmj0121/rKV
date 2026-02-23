# rKV Initial Scaffold Specification

## Status

Approved

## Context

The rKV repository contains only environment setup (Makefile stubs, pre-commit config, README). No Rust code exists
yet. This specification defines the initial project scaffold: a Cargo workspace with a working CLI binary backed by
stub engine methods. The goal is a skeleton that compiles, passes lint, and runs — ready for incremental feature
development.

See also: [CONCEPTS.md](../CONCEPTS.md) for architecture and design rationale.

## Requirements

### Functional Requirements

1. **Workspace layout** — A Cargo workspace at the repo root with a single member crate `rkv` that produces both a
   library and a CLI binary.

2. **Engine API** — A public Rust API exposing:

   - `DB::open(config: Config) -> Result<DB>` — open or create a database.
   - `DB::close(self) -> Result<()>` — close the database.
   - `DB::path(&self) -> &Path` — return the database directory.
   - `DB::put(&self, key, value) -> Result<u128>` — store a key-value pair, return revision ID.
   - `DB::get(&self, key) -> Result<Vec<u8>>` — retrieve a value by key.
   - `DB::delete(&self, key) -> Result<()>` — remove a key.
   - `DB::exists(&self, key) -> Result<bool>` — check key existence.
   - `DB::scan(&self, prefix, limit) -> Result<Vec<Vec<u8>>>` — forward scan.
   - `DB::rscan(&self, prefix, limit) -> Result<Vec<Vec<u8>>>` — reverse scan.
   - `DB::count(&self) -> Result<u64>` — count all keys.

3. **Stub behavior** — All engine methods (except `open`, `close`, `path`) return
   `Err(Error::NotImplemented("..."))` in this scaffold phase.

4. **CLI REPL** — An interactive command-line interface that:

   - Accepts an optional path argument (positional) and `--create`/`-c` flag.
   - Provides a `rustyline`-based REPL with persistent history (`~/.rkv_history`).
   - Supports commands: `put`, `get`, `delete`/`del`, `exists`, `scan`, `rscan`, `count`, `help`/`?`, `exit`/`quit`.
   - Displays stub error messages when engine methods are called.

5. **Makefile targets** — `make build`, `make test`, `make run`, `make clean` delegate to the corresponding `cargo`
   commands.

6. **Integration tests** — Four tests verifying:
   - `open` creates a new directory when it does not exist.
   - `open` succeeds on an existing directory.
   - `put` returns `NotImplemented`.
   - `get` returns `NotImplemented`.

### Non-Functional Requirements

- All code passes `cargo clippy -- -D warnings`.
- `cargo test --workspace` exits 0.
- The REPL binary starts and responds to `help` and `exit` without panic.
- No `unsafe` code in this scaffold phase.

## Design

### Module Structure

```text
rKV/
  Cargo.toml                    # workspace root
  rkv/
    Cargo.toml                  # lib + bin crate
    src/
      lib.rs                    # re-exports engine::{DB, Config, Error, Result}
      main.rs                   # CLI binary (REPL, Args, command dispatch)
      engine/
        mod.rs                  # DB struct, Config struct, stub methods
        error.rs                # Error enum, Result type alias
    tests/
      db_basic.rs               # integration tests
```

### Public API Surface (`lib.rs`)

```rust
pub mod engine;
pub use engine::{Config, DB, Error, Result};
```

Only these four types are publicly accessible. Everything in `main.rs` is binary-internal and invisible to library
consumers.

### Error Type (`engine/error.rs`)

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("key not found")]
    KeyNotFound,

    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

### Config (`engine/mod.rs`)

```rust
pub struct Config {
    pub path: PathBuf,
    pub create_if_missing: bool,
}
```

`create_if_missing` defaults to `true`.

### DB Struct (`engine/mod.rs`)

```rust
pub struct DB {
    config: Config,
}
```

All methods take `&self`. Stub methods return `Err(Error::NotImplemented(...))`. `open` creates the directory if
`create_if_missing` is true and returns a `DB` instance. `close` is a consuming method that takes `self`.

### CLI Binary (`main.rs`)

- `Args` struct parsed by `clap::Parser`:
  - `path: Option<String>` — database directory (positional, optional).
  - `-c`/`--create` — create if missing (default true).
- `Action` enum: `Continue`, `Exit`.
- `execute(db, line) -> Action` — private function, dispatches on first token.
- `run_repl(db)` — REPL loop using `rustyline`.
- `main()` — parses args, opens DB, runs REPL.

### Dependencies

| Crate     | Version | Purpose                |
| --------- | ------- | ---------------------- |
| clap      | 4.5     | CLI argument parsing   |
| rustyline | 17      | REPL line editing      |
| dirs-sys  | 0.5     | Home directory lookup  |
| thiserror | 2       | Error derive macro     |
| tempfile  | 3       | (dev) temp directories |

## Open Questions

None — this is a scaffold. All engine behavior is stubbed.
