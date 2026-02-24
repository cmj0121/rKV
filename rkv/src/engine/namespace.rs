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
    db: &'db DB,
    name: String,
    encrypted: bool,
}

impl fmt::Debug for Namespace<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Namespace")
            .field("name", &self.name)
            .field("encrypted", &self.encrypted)
            .finish()
    }
}

impl<'db> Namespace<'db> {
    /// Open (or create) a namespace within the given database.
    pub(crate) fn open(db: &'db DB, name: &str, password: Option<&str>) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::InvalidNamespace(
                "namespace name must not be empty".into(),
            ));
        }
        Ok(Self {
            db,
            name: name.to_owned(),
            encrypted: password.is_some(),
        })
    }

    /// Returns the namespace name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether this namespace was opened with encryption.
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    pub fn put(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
    ) -> Result<RevisionID> {
        let key = key.into();
        let value = value.into();
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &value, ttl)?;
        let mt = self.db.get_or_create_memtable(&self.name);
        let mut mt = mt.lock().unwrap();
        let actual_rev = mt.put(key, value, rev, ttl);
        Ok(actual_rev)
    }

    pub fn get(&self, key: impl Into<Key>) -> Result<Value> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        mt.get(&key).cloned().ok_or(Error::KeyNotFound)
    }

    pub fn delete(&self, key: impl Into<Key>) -> Result<()> {
        let key = key.into();
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
        let mt = self.db.get_or_create_memtable(&self.name);
        let mut mt = mt.lock().unwrap();
        mt.delete(key, rev);
        Ok(())
    }

    pub fn exists(&self, key: impl Into<Key>) -> Result<bool> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        Ok(mt.exists(&key))
    }

    pub fn scan(&self, prefix: &Key, limit: usize, offset: usize) -> Result<Vec<Key>> {
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        Ok(mt.scan(prefix, limit, offset))
    }

    pub fn rscan(&self, prefix: &Key, limit: usize, offset: usize) -> Result<Vec<Key>> {
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        Ok(mt.rscan(prefix, limit, offset))
    }

    pub fn count(&self) -> Result<u64> {
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        Ok(mt.count())
    }

    /// Returns the total number of revisions for a key.
    pub fn rev_count(&self, key: impl Into<Key>) -> Result<u64> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        mt.rev_count(&key).ok_or(Error::KeyNotFound)
    }

    /// Returns the value at a specific revision index (0 = oldest).
    pub fn rev_get(&self, key: impl Into<Key>, index: u64) -> Result<Value> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        mt.rev_get(&key, index).cloned().ok_or(Error::KeyNotFound)
    }

    /// Returns the remaining TTL for a key, or `None` if the key has no expiration.
    pub fn ttl(&self, key: impl Into<Key>) -> Result<Option<Duration>> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        mt.ttl(&key).ok_or(Error::KeyNotFound)
    }
}
