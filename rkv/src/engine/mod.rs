mod error;
mod key;
mod value;

pub use error::{Error, Result};
pub use key::Key;
pub use value::Value;

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

    pub fn put(&self, _key: impl Into<Key>, _value: impl Into<Value>) -> Result<u128> {
        let _key = _key.into();
        let _value = _value.into();
        Err(Error::NotImplemented("put".into()))
    }

    pub fn get(&self, _key: impl Into<Key>) -> Result<Value> {
        let _key = _key.into();
        Err(Error::NotImplemented("get".into()))
    }

    pub fn delete(&self, _key: impl Into<Key>) -> Result<()> {
        let _key = _key.into();
        Err(Error::NotImplemented("delete".into()))
    }

    pub fn exists(&self, _key: impl Into<Key>) -> Result<bool> {
        let _key = _key.into();
        Err(Error::NotImplemented("exists".into()))
    }

    pub fn scan(&self, _prefix: &Key, _limit: usize) -> Result<Vec<Key>> {
        Err(Error::NotImplemented("scan".into()))
    }

    pub fn rscan(&self, _prefix: &Key, _limit: usize) -> Result<Vec<Key>> {
        Err(Error::NotImplemented("rscan".into()))
    }

    pub fn count(&self) -> Result<u64> {
        Err(Error::NotImplemented("count".into()))
    }
}
