use std::fmt;
use std::time::Duration;

use super::crypto;
use super::error::{Error, Result};
use super::key::Key;
use super::memtable::MemLookup;
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
    encryption_key: Option<[u8; 32]>,
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
        let encryption_key = if let Some(pw) = password {
            let salt = crypto::load_or_create_salt(&db.config().path, name)?;
            Some(crypto::derive_key(pw, &salt))
        } else {
            None
        };
        Ok(Self {
            db,
            name: name.to_owned(),
            encrypted: password.is_some(),
            encryption_key,
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

    /// Encrypt a value's data bytes if this namespace is encrypted.
    /// Non-Data variants (Null, Tombstone, Pointer) pass through unchanged.
    fn encrypt_value(&self, value: Value) -> Value {
        if let Some(ref key) = self.encryption_key {
            if let Value::Data(ref plaintext) = value {
                return Value::Data(crypto::encrypt(key, plaintext));
            }
        }
        value
    }

    /// Decrypt a value's data bytes if this namespace is encrypted.
    /// Non-Data variants pass through unchanged.
    fn decrypt_value(&self, value: Value) -> Result<Value> {
        if let Some(ref key) = self.encryption_key {
            if let Value::Data(ref ciphertext) = value {
                let plaintext = crypto::decrypt(key, ciphertext)?;
                return Ok(Value::Data(plaintext));
            }
        }
        Ok(value)
    }

    pub fn put(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
    ) -> Result<RevisionID> {
        let key = key.into();
        let value = self.encrypt_value(value.into());
        let value = self.db.maybe_separate_value(&self.name, value)?;
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &value, ttl)?;
        let mt = self.db.get_or_create_memtable(&self.name);
        let mut mt = mt.lock().unwrap();
        let actual_rev = mt.put(key, value, rev, ttl);
        self.db.inc_op_puts();
        Ok(actual_rev)
    }

    pub fn get(&self, key: impl Into<Key>) -> Result<Value> {
        let key = key.into();
        self.db.inc_op_gets();

        // 1. Check MemTable first (3-state lookup)
        let value = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            match mt.lookup(&key) {
                MemLookup::Found(v) => v.clone(),
                MemLookup::Tombstone => return Err(Error::KeyNotFound),
                MemLookup::NotFound => {
                    // 2. Fall through to SSTables only when key was never in memtable
                    drop(mt);
                    match self.db.get_from_sstables(&self.name, &key)? {
                        Some(v) if v.is_tombstone() => return Err(Error::KeyNotFound),
                        Some(v) => v,
                        None => return Err(Error::KeyNotFound),
                    }
                }
            }
        };

        let value = self.db.resolve_value(&self.name, &value)?;
        self.decrypt_value(value)
    }

    pub fn delete(&self, key: impl Into<Key>) -> Result<()> {
        let key = key.into();
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
        let mt = self.db.get_or_create_memtable(&self.name);
        let mut mt = mt.lock().unwrap();
        mt.delete(key, rev);
        self.db.inc_op_deletes();
        Ok(())
    }

    /// Delete all live keys in a range.
    ///
    /// When `inclusive` is false the range is `[start, end)` (half-open).
    /// When `inclusive` is true the range is `[start, end]` (closed).
    ///
    /// Returns the number of keys actually deleted.
    pub fn delete_range(
        &self,
        start: impl Into<Key>,
        end: impl Into<Key>,
        inclusive: bool,
    ) -> Result<u64> {
        let start = start.into();
        let end = end.into();

        // Collect keys to delete while holding the memtable lock briefly
        let keys = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            mt.keys_in_range(&start, &end, inclusive)
        };

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mut mt = mt.lock().unwrap();
            mt.delete(key, rev);
        }

        if count > 0 {
            self.db.inc_op_deletes_by(count);
        }
        Ok(count)
    }

    /// Delete all live keys whose string representation starts with `prefix`.
    ///
    /// Returns the number of keys actually deleted.
    pub fn delete_prefix(&self, prefix: &str) -> Result<u64> {
        let keys = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            mt.keys_with_prefix(prefix)
        };

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mut mt = mt.lock().unwrap();
            mt.delete(key, rev);
        }

        if count > 0 {
            self.db.inc_op_deletes_by(count);
        }
        Ok(count)
    }

    pub fn exists(&self, key: impl Into<Key>) -> Result<bool> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        Ok(mt.exists(&key))
    }

    pub fn scan(&self, prefix: &Key, limit: usize, offset: usize) -> Result<Vec<Key>> {
        let (mt_entries, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            (mt.scan_all_raw(prefix), mt.is_ordered())
        };

        let mut merged = self
            .db
            .scan_from_sstables(&self.name, prefix, ordered_mode)?;

        for (key, value) in mt_entries {
            merged.insert(key, value);
        }

        Ok(merged
            .into_iter()
            .filter(|(_, v)| !v.is_tombstone())
            .map(|(k, _)| k)
            .skip(offset)
            .take(limit)
            .collect())
    }

    pub fn rscan(&self, prefix: &Key, limit: usize, offset: usize) -> Result<Vec<Key>> {
        let (mt_entries, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            (mt.rscan_all_raw(prefix), mt.is_ordered())
        };

        let mut merged = self
            .db
            .rscan_from_sstables(&self.name, prefix, ordered_mode)?;

        for (key, value) in mt_entries {
            merged.insert(key, value);
        }

        // Collect and reverse for rscan
        let all: Vec<Key> = merged
            .into_iter()
            .filter(|(_, v)| !v.is_tombstone())
            .map(|(k, _)| k)
            .collect();

        Ok(all.into_iter().rev().skip(offset).take(limit).collect())
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
        let value = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap();
            mt.rev_get(&key, index).cloned().ok_or(Error::KeyNotFound)?
        };
        let value = self.db.resolve_value(&self.name, &value)?;
        self.decrypt_value(value)
    }

    /// Returns the remaining TTL for a key, or `None` if the key has no expiration.
    pub fn ttl(&self, key: impl Into<Key>) -> Result<Option<Duration>> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap();
        mt.ttl(&key).ok_or(Error::KeyNotFound)
    }
}
