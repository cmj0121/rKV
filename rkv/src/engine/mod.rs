mod error;
mod key;
mod namespace;
mod revision;
mod stats;
mod value;

pub use error::{Error, Result};
pub use key::Key;
pub use namespace::Namespace;
pub use revision::RevisionID;
pub use stats::Stats;
pub use value::Value;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Default namespace name.
pub const DEFAULT_NAMESPACE: &str = "_";

#[derive(Clone, Debug)]
pub struct Config {
    pub path: PathBuf,
    pub create_if_missing: bool,
    /// Write buffer size in bytes (default: 4 MB).
    pub write_buffer_size: usize,
    /// Maximum number of LSM levels (default: 3).
    pub max_levels: usize,
    /// SSTable block size in bytes (default: 4 KB).
    pub block_size: usize,
    /// Block cache size in bytes (default: 8 MB).
    pub cache_size: usize,
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            create_if_missing: true,
            write_buffer_size: 4 * 1024 * 1024,
            max_levels: 3,
            block_size: 4 * 1024,
            cache_size: 8 * 1024 * 1024,
        }
    }
}

pub struct DB {
    config: Config,
    opened_at: Instant,
}

impl DB {
    pub fn open(config: Config) -> Result<Self> {
        if config.create_if_missing {
            fs::create_dir_all(&config.path)?;
        }
        Ok(Self {
            config,
            opened_at: Instant::now(),
        })
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.config.path
    }

    pub fn stats(&self) -> Stats {
        Stats {
            level_count: self.config.max_levels,
            uptime: self.opened_at.elapsed(),
            ..Stats::default()
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Switch to a namespace, creating it if it does not exist.
    pub fn namespace(&self, name: &str) -> Result<Namespace<'_>> {
        Namespace::open(self, name)
    }

    /// List all namespace names.
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(Error::NotImplemented("list_namespaces".into()))
    }

    /// Drop a namespace and all its data. The default namespace cannot be dropped.
    pub fn drop_namespace(&self, name: &str) -> Result<()> {
        if name == DEFAULT_NAMESPACE {
            return Err(Error::InvalidNamespace(
                "cannot drop the default namespace".into(),
            ));
        }
        if name.is_empty() {
            return Err(Error::InvalidNamespace(
                "namespace name must not be empty".into(),
            ));
        }
        Err(Error::NotImplemented("drop_namespace".into()))
    }

    // --- Flush / Sync ---

    /// Flush the in-memory write buffer to disk.
    pub fn flush(&self) -> Result<()> {
        Err(Error::NotImplemented("flush".into()))
    }

    /// Flush and fsync all data to durable storage.
    pub fn sync(&self) -> Result<()> {
        Err(Error::NotImplemented("sync".into()))
    }

    // --- Destroy / Repair ---

    /// Destroy the database at the given path, deleting all data.
    pub fn destroy(path: impl Into<PathBuf>) -> Result<()> {
        let _path = path.into();
        Err(Error::NotImplemented("destroy".into()))
    }

    /// Attempt to repair a corrupted database at the given path.
    pub fn repair(path: impl Into<PathBuf>) -> Result<()> {
        let _path = path.into();
        Err(Error::NotImplemented("repair".into()))
    }

    // --- Dump / Load ---

    /// Export the database to a portable backup file.
    pub fn dump(&self, path: impl Into<PathBuf>) -> Result<()> {
        let _path = path.into();
        Err(Error::NotImplemented("dump".into()))
    }

    /// Import a database from a portable backup file.
    pub fn load(path: impl Into<PathBuf>) -> Result<DB> {
        let _path = path.into();
        Err(Error::NotImplemented("load".into()))
    }

    // --- Compaction ---

    /// Trigger a manual compaction of SSTable levels.
    pub fn compact(&self) -> Result<()> {
        Err(Error::NotImplemented("compact".into()))
    }
}
