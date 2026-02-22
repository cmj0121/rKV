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

## DDD (Dream-Driven Development)

This project follows the DDD (Dream-Driven Development) methodology, which means the project
is driven by what I envision.

All features are based on my needs and my dreams.
