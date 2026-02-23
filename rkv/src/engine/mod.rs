mod error;

pub use error::{Error, Result};

use std::fs;
use std::path::{Path, PathBuf};

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

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<u128> {
        Err(Error::NotImplemented("put".into()))
    }

    pub fn get(&self, _key: &[u8]) -> Result<Vec<u8>> {
        Err(Error::NotImplemented("get".into()))
    }

    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        Err(Error::NotImplemented("delete".into()))
    }

    pub fn exists(&self, _key: &[u8]) -> Result<bool> {
        Err(Error::NotImplemented("exists".into()))
    }

    pub fn scan(&self, _prefix: &[u8], _limit: usize) -> Result<Vec<Vec<u8>>> {
        Err(Error::NotImplemented("scan".into()))
    }

    pub fn rscan(&self, _prefix: &[u8], _limit: usize) -> Result<Vec<Vec<u8>>> {
        Err(Error::NotImplemented("rscan".into()))
    }

    pub fn count(&self) -> Result<u64> {
        Err(Error::NotImplemented("count".into()))
    }
}
