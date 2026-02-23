mod error;
mod key;
mod namespace;
mod value;

pub use error::{Error, Result};
pub use key::Key;
pub use namespace::Namespace;
pub use value::Value;

use std::fs;
use std::path::{Path, PathBuf};

/// Default namespace name.
pub const DEFAULT_NAMESPACE: &str = "_";

pub struct Config {
    pub path: PathBuf,
    pub create_if_missing: bool,
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            create_if_missing: true,
        }
    }
}

pub struct DB {
    config: Config,
}

impl DB {
    pub fn open(config: Config) -> Result<Self> {
        if config.create_if_missing {
            fs::create_dir_all(&config.path)?;
        }
        Ok(Self { config })
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.config.path
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
}
