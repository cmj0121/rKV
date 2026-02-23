use std::fmt;
use std::time::Duration;

use super::error::{Error, Result};
use super::key::Key;
use super::revision::RevisionID;
use super::value::Value;
use super::DB;

/// A handle to an isolated key-value table within a database.
///
/// All data operations (`put`, `get`, `delete`, `exists`, `scan`, `rscan`,
/// `count`) live on this handle. Obtain one via [`DB::namespace`].
pub struct Namespace<'db> {
    #[allow(dead_code)]
    db: &'db DB,
    name: String,
}

impl fmt::Debug for Namespace<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Namespace")
            .field("name", &self.name)
            .finish()
    }
}

impl<'db> Namespace<'db> {
    /// Open (or create) a namespace within the given database.
    pub(crate) fn open(db: &'db DB, name: &str) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::InvalidNamespace(
                "namespace name must not be empty".into(),
            ));
        }
        Ok(Self {
            db,
            name: name.to_owned(),
        })
    }

    /// Returns the namespace name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn put(&self, _key: impl Into<Key>, _value: impl Into<Value>) -> Result<RevisionID> {
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

    /// Returns the total number of revisions for a key.
    pub fn rev_count(&self, _key: impl Into<Key>) -> Result<u64> {
        let _key = _key.into();
        Err(Error::NotImplemented("rev_count".into()))
    }

    /// Returns the value at a specific revision index (0 = oldest).
    pub fn rev_get(&self, _key: impl Into<Key>, _index: u64) -> Result<Value> {
        let _key = _key.into();
        Err(Error::NotImplemented("rev_get".into()))
    }

    /// Store a key-value pair with a time-to-live. The key expires after `ttl`.
    pub fn put_with_ttl(
        &self,
        _key: impl Into<Key>,
        _value: impl Into<Value>,
        _ttl: Duration,
    ) -> Result<RevisionID> {
        let _key = _key.into();
        let _value = _value.into();
        Err(Error::NotImplemented("put_with_ttl".into()))
    }

    /// Returns the remaining TTL for a key, or `None` if the key has no expiration.
    pub fn ttl(&self, _key: impl Into<Key>) -> Result<Option<Duration>> {
        let _key = _key.into();
        Err(Error::NotImplemented("ttl".into()))
    }
}
