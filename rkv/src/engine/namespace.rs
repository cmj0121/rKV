use std::fmt;
use std::time::Duration;

use super::batch::{BatchOp, WriteBatch};
use super::crypto;
use super::error::{Error, Result};
use super::iterator::{EntryIterator, KeyIterator};
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

    /// Check if a write can be skipped via dedup. Returns `Some(existing_rev)`
    /// if the value is identical and neither side has a TTL.
    ///
    /// Uses raw memtable/SSTable lookups to avoid inflating `op_gets` stats.
    fn try_dedup(
        &self,
        key: &Key,
        value: &Value,
        ttl: &Option<Duration>,
        dedup: Option<bool>,
    ) -> Result<Option<RevisionID>> {
        let dedup_active = dedup.unwrap_or_else(|| self.db.dedup_enabled(&self.name));
        if !dedup_active || ttl.is_some() {
            return Ok(None);
        }

        // Look up existing value+revision and TTL status without inflating op_gets.
        // `has_ttl` tracks whether the existing entry has a non-zero expiration,
        // regardless of whether it lives in the memtable or SSTables.
        let (existing, rev, has_ttl) = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            match mt.lookup_with_revision(key) {
                MemLookupRev::Found(v, rev) => {
                    let has_ttl = matches!(mt.ttl(key), Some(Some(_)));
                    (v.clone(), rev, has_ttl)
                }
                MemLookupRev::Tombstone | MemLookupRev::NotFound => {
                    drop(mt);
                    match self.db.get_from_sstables_full(&self.name, key)? {
                        Some((v, rev, expires_at_ms)) if !v.is_tombstone() => {
                            (v, rev, expires_at_ms != 0)
                        }
                        _ => return Ok(None),
                    }
                }
            }
        };

        if has_ttl {
            return Ok(None);
        }

        // Resolve pointer + decrypt to get raw value for comparison
        let existing = self.db.resolve_value(&self.name, &existing)?;
        let existing = self.decrypt_value(existing)?;
        if existing != *value {
            return Ok(None);
        }

        self.db.inc_dedup_skips();
        Ok(Some(rev))
    }

    /// Store a key-value pair, returning its revision ID.
    ///
    /// Write path: generate revision → AOL append → memtable insert.
    /// If the memtable exceeds `write_stall_size`, a synchronous flush is
    /// triggered before returning (backpressure).
    pub fn put(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
    ) -> Result<RevisionID> {
        self.put_opt(key, value, ttl, None)
    }

    /// Like `put`, but with an explicit per-request dedup override.
    ///
    /// `dedup`: `Some(true)` forces dedup on, `Some(false)` forces it off,
    /// `None` uses the namespace/global setting.
    pub fn put_opt(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
        dedup: Option<bool>,
    ) -> Result<RevisionID> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_put);
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        let key = key.into();
        let value = value.into();

        if let Some(rev) = self.try_dedup(&key, &value, &ttl, dedup)? {
            return Ok(rev);
        }

        let value = self.encrypt_value(value);
        let value = self.db.maybe_separate_value(&self.name, value)?;
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &value, ttl)?;
        let (actual_rev, should_flush, should_stall) = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
            let actual_rev = mt.put(key, value, rev, ttl);
            self.db.inc_op_puts();
            let size = mt.approximate_size();
            let config = self.db.config();
            let stall = config.write_stall_size > 0 && size >= config.write_stall_size;
            (actual_rev, size >= config.write_buffer_size, stall)
        };
        if !self.db.config().in_memory {
            if should_stall {
                // Backpressure: flush synchronously and block the writer
                self.db.flush()?;
            } else if should_flush {
                let _ = self.db.flush(); // best-effort; data is safe in AOL
            }
        }
        Ok(actual_rev)
    }

    /// Apply multiple operations atomically.
    ///
    /// All operations are appended to the AOL under a single lock hold, then
    /// applied to the memtable under a single lock hold. Either all operations
    /// succeed or none are visible.
    ///
    /// Returns a revision ID for each operation, in order.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<Vec<RevisionID>> {
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Pre-process: dedup check, encrypt values, separate bin objects
        enum PreparedOp {
            Write(Key, Value, Option<Duration>),
            Deduped(RevisionID),
        }
        let mut ops: Vec<PreparedOp> = Vec::with_capacity(batch.len());
        for op in batch.ops {
            match op {
                BatchOp::Put {
                    key,
                    value,
                    ttl,
                    dedup,
                } => {
                    if let Some(rev) = self.try_dedup(&key, &value, &ttl, dedup)? {
                        ops.push(PreparedOp::Deduped(rev));
                    } else {
                        let value = self.encrypt_value(value);
                        let value = self.db.maybe_separate_value(&self.name, value)?;
                        ops.push(PreparedOp::Write(key, value, ttl));
                    }
                }
                BatchOp::Delete { key } => {
                    ops.push(PreparedOp::Write(key, Value::tombstone(), None));
                }
            }
        }

        // Generate revisions only for non-deduped ops
        let mut revisions: Vec<Option<RevisionID>> = Vec::with_capacity(ops.len());
        for op in &ops {
            match op {
                PreparedOp::Write(..) => revisions.push(Some(self.db.generate_revision())),
                PreparedOp::Deduped(_) => revisions.push(None),
            }
        }

        // 2. Append non-deduped ops to AOL under a single lock
        {
            if let Some(mut aol) = self.db.aol_lock(&self.name)? {
                for (i, op) in ops.iter().enumerate() {
                    if let PreparedOp::Write(key, value, ttl) = op {
                        self.db.append_to_aol_locked(
                            &mut aol,
                            &self.name,
                            revisions[i].unwrap().as_u128(),
                            key,
                            value,
                            *ttl,
                        )?;
                    }
                }
            }
        }

        // 3. Apply non-deduped ops to memtable under a single lock
        let (actual_revs, should_flush, should_stall) = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
            let mut actual_revs = Vec::with_capacity(ops.len());
            let mut puts = 0u64;
            let mut deletes = 0u64;
            for (i, op) in ops.into_iter().enumerate() {
                match op {
                    PreparedOp::Deduped(rev) => actual_revs.push(rev),
                    PreparedOp::Write(key, value, ttl) => {
                        let rev = revisions[i].unwrap();
                        if value.is_tombstone() {
                            mt.delete(key, rev);
                            actual_revs.push(rev);
                            deletes += 1;
                        } else {
                            let actual_rev = mt.put(key, value, rev, ttl);
                            actual_revs.push(actual_rev);
                            puts += 1;
                        }
                    }
                }
            }
            self.db.inc_op_puts_by(puts);
            self.db.inc_op_deletes_by(deletes);
            let size = mt.approximate_size();
            let config = self.db.config();
            let stall = config.write_stall_size > 0 && size >= config.write_stall_size;
            (actual_revs, size >= config.write_buffer_size, stall)
        };

        // 4. Flush if needed
        if !self.db.config().in_memory {
            if should_stall {
                self.db.flush()?;
            } else if should_flush {
                let _ = self.db.flush();
            }
        }

        Ok(actual_revs)
    }

    /// Retrieve the value for a key.
    ///
    /// Read path: memtable (3-state: Found/Tombstone/NotFound) → SSTables
    /// (newest level first). Tombstoned or expired keys return `KeyNotFound`.
    /// Large values stored as bin objects are resolved transparently.
    pub fn get(&self, key: impl Into<Key>) -> Result<Value> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_get);
        let key = key.into();
        self.db.inc_op_gets();

        // 1. Check MemTable first (3-state lookup)
        let value = {
            metrics::prof_timer!(self.db.metrics(), prof_memtable_lookup);
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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

        {
            metrics::prof_timer!(self.db.metrics(), prof_value_resolve);
            let value = self.db.resolve_value(&self.name, &value)?;
            self.decrypt_value(value)
        }
    }

    /// Like `get()`, but also returns the `RevisionID` for the value.
    pub fn get_with_revision(&self, key: impl Into<Key>) -> Result<(Value, RevisionID)> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_get);
        let key = key.into();
        self.db.inc_op_gets();

        let (value, rev) = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    pub(crate) fn get_raw(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_get);
        let key = key.into();
        self.db.inc_op_gets();

        let mt = self.db.get_or_create_memtable(&self.name)?;
        let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
        let mt = self.db.get_or_create_memtable(&self.name)?;
        let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
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

        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        // Use merge iterator to collect all live keys in range
        let empty_prefix = Key::Str(String::new());
        let mut iter = self
            .db
            .build_merge_iterator(&self.name, &empty_prefix, ordered_mode)?;

        let mut keys = Vec::new();
        while let Some((key, value)) = iter.next()? {
            if value.is_tombstone() {
                continue;
            }
            let in_range = if inclusive {
                key >= start && key <= end
            } else {
                key >= start && key < end
            };
            if in_range {
                keys.push(key);
            }
        }

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
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
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        let prefix_key = Key::Str(prefix.to_owned());
        let mut iter = self
            .db
            .build_merge_iterator(&self.name, &prefix_key, ordered_mode)?;

        let mut keys = Vec::new();
        while let Some((key, value)) = iter.next()? {
            if value.is_tombstone() {
                continue;
            }
            if key.as_str().is_some_and(|s| s.starts_with(prefix)) {
                keys.push(key);
            }
        }

        let count = keys.len() as u64;
        for key in keys {
            let rev = self.db.generate_revision();
            self.db
                .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
            mt.delete(key, rev);
        }

        if count > 0 {
            self.db.inc_op_deletes_by(count);
        }
        Ok(count)
    }

    pub fn exists(&self, key: impl Into<Key>) -> Result<bool> {
        let key = key.into();
        let mt = self.db.get_or_create_memtable(&self.name)?;
        let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        let mut iter = self
            .db
            .build_merge_iterator(&self.name, prefix, ordered_mode)?;

        let mut result = Vec::new();
        let mut skipped = 0usize;
        while let Some((key, value)) = iter.next()? {
            if !include_deleted && value.is_tombstone() {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            result.push(key);
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }

    pub fn rscan(
        &self,
        prefix: &Key,
        limit: usize,
        offset: usize,
        include_deleted: bool,
    ) -> Result<Vec<Key>> {
        let _timer = metrics::Timer::start(&self.db.metrics().op_scan);
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        let mut rscan = self
            .db
            .build_rscan_merge_iterator(&self.name, prefix, ordered_mode)?;

        let mut result = Vec::new();
        let mut skipped = 0usize;
        while let Some((key, value)) = rscan.next()? {
            if !include_deleted && value.is_tombstone() {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            result.push(key);
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }

    pub fn count(&self) -> Result<u64> {
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        let empty_prefix = Key::Str(String::new());
        let mut iter = self
            .db
            .build_merge_iterator(&self.name, &empty_prefix, ordered_mode)?;

        let mut count = 0u64;
        while let Some((_key, value)) = iter.next()? {
            if !value.is_tombstone() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Returns the total number of revisions for a key across memtable and
    /// SSTables.
    pub fn rev_count(&self, key: impl Into<Key>) -> Result<u64> {
        let key = key.into();
        let mt_count = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
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
        let mt = self.db.get_or_create_memtable(&self.name)?;
        let mt = mt.read().unwrap_or_else(|e| e.into_inner());
        mt.ttl(&key).ok_or(Error::KeyNotFound)
    }

    // --- Iterator API ---

    /// Returns a lazy forward iterator over keys matching `prefix`.
    ///
    /// Tombstoned keys are skipped automatically. In ordered mode (integer keys),
    /// `prefix` acts as a range start. In unordered mode (string keys), it acts
    /// as a prefix filter.
    pub fn keys(&self, prefix: &Key) -> Result<KeyIterator> {
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };
        let iter = self
            .db
            .build_merge_iterator(&self.name, prefix, ordered_mode)?;
        Ok(KeyIterator::forward(iter))
    }

    /// Returns a lazy forward iterator over (key, value) pairs matching `prefix`.
    ///
    /// Tombstoned keys are skipped. `ValuePointer`s are resolved transparently
    /// and encrypted values are decrypted.
    pub fn entries(&self, prefix: &Key) -> Result<EntryIterator<'db>> {
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };
        let iter = self
            .db
            .build_merge_iterator(&self.name, prefix, ordered_mode)?;
        Ok(EntryIterator::forward(
            iter,
            self.db,
            self.name.clone(),
            self.encryption_key,
        ))
    }

    /// Returns a lazy reverse iterator over keys matching `prefix`.
    pub fn rkeys(&self, prefix: &Key) -> Result<KeyIterator> {
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };
        let reverse_iter = self
            .db
            .build_rscan_merge_iterator(&self.name, prefix, ordered_mode)?;
        Ok(KeyIterator::reverse(reverse_iter))
    }

    /// Returns a lazy reverse iterator over (key, value) pairs matching `prefix`.
    pub fn rentries(&self, prefix: &Key) -> Result<EntryIterator<'db>> {
        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };
        let reverse_iter = self
            .db
            .build_rscan_merge_iterator(&self.name, prefix, ordered_mode)?;
        Ok(EntryIterator::reverse(
            reverse_iter,
            self.db,
            self.name.clone(),
            self.encryption_key,
        ))
    }

    /// Atomically pop the first live key matching `prefix`.
    ///
    /// Returns the key and its resolved value, or `None` if no matching
    /// live entry exists. The key is tombstoned in a single operation,
    /// avoiding the separate scan+delete round-trip.
    pub fn pop_first(&self, prefix: &Key) -> Result<Option<(Key, Value)>> {
        if self.db.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }

        let ordered_mode = {
            let mt = self.db.get_or_create_memtable(&self.name)?;
            let mt = mt.read().unwrap_or_else(|e| e.into_inner());
            mt.is_ordered()
        };

        // Build merge iterator to find the first live entry
        let mut iter = self
            .db
            .build_merge_iterator(&self.name, prefix, ordered_mode)?;

        let (key, value) = loop {
            match iter.next()? {
                Some((_k, v)) if v.is_tombstone() => continue,
                Some((k, v)) => break (k, v),
                None => return Ok(None),
            }
        };

        // Drop the iterator before writing (releases SSTable read locks)
        drop(iter);

        // Delete the key
        let rev = self.db.generate_revision();
        self.db
            .append_to_aol(&self.name, rev.as_u128(), &key, &Value::tombstone(), None)?;
        let mt = self.db.get_or_create_memtable(&self.name)?;
        let mut mt = mt.write().unwrap_or_else(|e| e.into_inner());
        mt.delete(key.clone(), rev);
        drop(mt);
        self.db.inc_op_deletes();

        // Resolve value pointers and decrypt
        let value = self.db.resolve_value(&self.name, &value)?;
        let value = self.decrypt_value(value)?;
        Ok(Some((key, value)))
    }
}
