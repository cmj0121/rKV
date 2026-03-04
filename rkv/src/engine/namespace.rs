use std::fmt;
use std::time::Duration;

use super::crypto;
use super::error::{Error, Result};
use super::key::Key;
use super::memtable::{MemLookup, MemLookupRev};
use super::metrics;
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
        let _timer = metrics::Timer::start(&self.db.metrics().op_put);
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        let key = key.into();
        let value = self.encrypt_value(value.into());
        let value = self.db.maybe_separate_value(&self.name, value)?;
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &value, ttl)?;
        let (actual_rev, should_flush, should_stall) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mut mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            let actual_rev = mt.put(key, value, rev, ttl);
            self.db.inc_op_puts();
            let size = mt.approximate_size();
            let config = self.db.config();
            let stall = config.write_stall_size > 0 && size >= config.write_stall_size;
            (actual_rev, size >= config.write_buffer_size, stall)
        };
        if should_stall {
            // Backpressure: flush synchronously and block the writer
            self.db.flush()?;
        } else if should_flush {
            let _ = self.db.flush(); // best-effort; data is safe in AOL
        }
        Ok(actual_rev)
    }

    pub fn get(&self, key: impl Into<Key>) -> Result<Value> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_get);
        let key = key.into();
        self.db.inc_op_gets();

        // 1. Check MemTable first (3-state lookup)
        let value = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            match mt.lookup(&key) {
                MemLookup::Found(v) => v.clone(),
                MemLookup::Tombstone => return Err(Error::KeyNotFound),
                MemLookup::NotFound => {
                    // 2. Fall through to SSTables only when key was never in memtable
                    drop(mt);
                    match self.db.get_from_sstables(&self.name, &key)? {
                        Some((v, _rev)) if v.is_tombstone() => return Err(Error::KeyNotFound),
                        Some((v, _rev)) => v,
                        None => return Err(Error::KeyNotFound),
                    }
                }
            }
        };

        let value = self.db.resolve_value(&self.name, &value)?;
        self.decrypt_value(value)
    }

    /// Like `get()`, but also returns the `RevisionID` for the value.
    pub fn get_with_revision(&self, key: impl Into<Key>) -> Result<(Value, RevisionID)> {
        let key = key.into();
        self.db.inc_op_gets();

        let (value, rev) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            match mt.lookup_with_revision(&key) {
                MemLookupRev::Found(v, rev) => (v.clone(), rev),
                MemLookupRev::Tombstone => return Err(Error::KeyNotFound),
                MemLookupRev::NotFound => {
                    drop(mt);
                    match self.db.get_from_sstables(&self.name, &key)? {
                        Some((v, _rev)) if v.is_tombstone() => return Err(Error::KeyNotFound),
                        Some((v, rev)) => (v, rev),
                        None => return Err(Error::KeyNotFound),
                    }
                }
            }
        };

        let value = self.db.resolve_value(&self.name, &value)?;
        let value = self.decrypt_value(value)?;
        Ok((value, rev))
    }

    /// Like `get()`, but returns `Some(Value::Tombstone)` for deleted keys
    /// instead of `Err(KeyNotFound)`. Returns `None` when the key never existed.
    #[allow(dead_code)] // used by server feature (routes/keys.rs)
    pub(crate) fn get_raw(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let key = key.into();
        self.db.inc_op_gets();

        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
        match mt.lookup(&key) {
            MemLookup::Found(v) => {
                let v = v.clone();
                drop(mt);
                let v = self.db.resolve_value(&self.name, &v)?;
                Ok(Some(self.decrypt_value(v)?))
            }
            MemLookup::Tombstone => Ok(Some(Value::tombstone())),
            MemLookup::NotFound => {
                drop(mt);
                match self.db.get_from_sstables(&self.name, &key)? {
                    Some((v, _rev)) if v.is_tombstone() => Ok(Some(Value::tombstone())),
                    Some((v, _rev)) => {
                        let v = self.db.resolve_value(&self.name, &v)?;
                        Ok(Some(self.decrypt_value(v)?))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    pub fn delete(&self, key: impl Into<Key>) -> Result<()> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_delete);
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        let key = key.into();
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
        let mt = self.db.get_or_create_memtable(&self.name);
        let mut mt = mt.lock().unwrap_or_else(|e| e.into_inner());
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
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        let start = start.into();
        let end = end.into();

        // Collect keys from memtable
        let (mt_keys, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            (mt.keys_in_range(&start, &end, inclusive), mt.is_ordered())
        };

        // Collect keys from SSTables in the same range
        let empty_prefix = Key::Str(String::new());
        let sst_entries = self
            .db
            .scan_from_sstables(&self.name, &empty_prefix, ordered_mode)?;

        // Union memtable + SSTable keys, filtering to range and excluding tombstones
        let mut keys: std::collections::BTreeSet<Key> = mt_keys.into_iter().collect();
        for (key, value) in sst_entries {
            if value.is_tombstone() {
                continue;
            }
            let in_range = if inclusive {
                key >= start && key <= end
            } else {
                key >= start && key < end
            };
            if in_range {
                keys.insert(key);
            }
        }
        // Remove keys that the memtable already knows are tombstoned
        {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            keys.retain(|k| !matches!(mt.lookup(k), MemLookup::Tombstone));
        }

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mut mt = mt.lock().unwrap_or_else(|e| e.into_inner());
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
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        let (mt_keys, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            (mt.keys_with_prefix(prefix), mt.is_ordered())
        };

        // Scan SSTables for matching prefix keys
        let prefix_key = Key::Str(prefix.to_owned());
        let sst_entries = self
            .db
            .scan_from_sstables(&self.name, &prefix_key, ordered_mode)?;

        // Union memtable + SSTable keys, filtering by prefix and excluding tombstones
        let mut keys: std::collections::BTreeSet<Key> = mt_keys.into_iter().collect();
        for (key, value) in sst_entries {
            if value.is_tombstone() {
                continue;
            }
            if key.to_string().starts_with(prefix) {
                keys.insert(key);
            }
        }
        // Remove keys that the memtable already knows are tombstoned
        {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            keys.retain(|k| !matches!(mt.lookup(k), MemLookup::Tombstone));
        }

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mut mt = mt.lock().unwrap_or_else(|e| e.into_inner());
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
        let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
        match mt.lookup(&key) {
            MemLookup::Found(_) => return Ok(true),
            MemLookup::Tombstone => return Ok(false),
            MemLookup::NotFound => {}
        }
        drop(mt);
        // Fall through to SSTables
        match self.db.get_from_sstables(&self.name, &key)? {
            Some((v, _rev)) if v.is_tombstone() => Ok(false),
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn scan(
        &self,
        prefix: &Key,
        limit: usize,
        offset: usize,
        include_deleted: bool,
    ) -> Result<Vec<Key>> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_scan);
        let (mt_entries, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
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
            .filter(|(_, v)| include_deleted || !v.is_tombstone())
            .map(|(k, _)| k)
            .skip(offset)
            .take(limit)
            .collect())
    }

    pub fn rscan(
        &self,
        prefix: &Key,
        limit: usize,
        offset: usize,
        include_deleted: bool,
    ) -> Result<Vec<Key>> {
        let (mt_entries, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
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
            .filter(|(_, v)| include_deleted || !v.is_tombstone())
            .map(|(k, _)| k)
            .collect();

        Ok(all.into_iter().rev().skip(offset).take(limit).collect())
    }

    pub fn count(&self) -> Result<u64> {
        let (mt_entries, ordered_mode) = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            (mt.scan_all_raw(&Key::Str(String::new())), mt.is_ordered())
        };

        // Merge SSTable entries with memtable entries (memtable wins)
        let empty_prefix = Key::Str(String::new());
        let mut merged = self
            .db
            .scan_from_sstables(&self.name, &empty_prefix, ordered_mode)?;
        for (key, value) in mt_entries {
            merged.insert(key, value);
        }

        Ok(merged
            .into_iter()
            .filter(|(_, v)| !v.is_tombstone())
            .count() as u64)
    }

    /// Returns the total number of revisions for a key across memtable and
    /// SSTables.
    pub fn rev_count(&self, key: impl Into<Key>) -> Result<u64> {
        let key = key.into();
        let mt_count = {
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            mt.rev_count(&key).unwrap_or(0)
        };
        let sst_count = self.db.count_revisions_from_sstables(&self.name, &key)?;
        let total = mt_count + sst_count;
        if total == 0 {
            Err(Error::KeyNotFound)
        } else {
            Ok(total)
        }
    }

    /// Returns the value at a specific revision index (0 = oldest).
    ///
    /// SSTable revisions come first (oldest), then memtable revisions.
    pub fn rev_get(&self, key: impl Into<Key>, index: u64) -> Result<Value> {
        let key = key.into();
        let sst_count = self.db.count_revisions_from_sstables(&self.name, &key)?;

        let value = if index < sst_count {
            // Fetch from SSTables
            match self
                .db
                .get_revision_from_sstables(&self.name, &key, index)?
            {
                Some((v, _, _)) => v,
                None => return Err(Error::KeyNotFound),
            }
        } else {
            // Fetch from memtable (shift index past SSTable revisions)
            let mt_index = index - sst_count;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            match mt.rev_get(&key, mt_index) {
                Some(v) => v.clone(),
                None => return Err(Error::KeyNotFound),
            }
        };

        let value = self.db.resolve_value(&self.name, &value)?;
        self.decrypt_value(value)
    }

    /// Returns the value, expiry status, and remaining TTL at a specific revision.
    pub fn rev_get_with_ttl(
        &self,
        key: impl Into<Key>,
        index: u64,
    ) -> Result<(Value, bool, Option<Duration>)> {
        let key = key.into();
        let sst_count = self.db.count_revisions_from_sstables(&self.name, &key)?;

        let (value, expired, remaining) = if index < sst_count {
            match self
                .db
                .get_revision_from_sstables(&self.name, &key, index)?
            {
                Some((v, _, expires_at_ms)) => {
                    if expires_at_ms == 0 {
                        (v, false, None)
                    } else {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        if now_ms >= expires_at_ms {
                            (v, true, None)
                        } else {
                            let rem = Duration::from_millis(expires_at_ms - now_ms);
                            (v, false, Some(rem))
                        }
                    }
                }
                None => return Err(Error::KeyNotFound),
            }
        } else {
            let mt_index = index - sst_count;
            let mt = self.db.get_or_create_memtable(&self.name);
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            match mt.rev_get_with_ttl(&key, mt_index) {
                Some((v, exp, rem)) => (v.clone(), exp, rem),
                None => return Err(Error::KeyNotFound),
            }
        };

        let value = self.db.resolve_value(&self.name, &value)?;
        let value = self.decrypt_value(value)?;
        Ok((value, expired, remaining))
    }

    /// Returns the remaining TTL for a key, or `None` if the key has no expiration.
    pub fn ttl(&self, key: impl Into<Key>) -> Result<Option<Duration>> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name);
        let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
        mt.ttl(&key).ok_or(Error::KeyNotFound)
    }
}
