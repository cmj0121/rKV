use rkv::{Key, RevisionID, Value, DB};

use super::backend::Backend;
use super::error::{self, Result};

/// Embedded backend — wraps a direct rKV `DB` reference.
pub struct EmbeddedBackend {
    // Safety: the DB reference must outlive this backend. We use a raw pointer
    // because the Backend trait needs to be object-safe (no lifetimes).
    // The caller guarantees the DB is alive for the lifetime of this backend.
    db: *const DB,
}

// SAFETY: DB is thread-safe (uses internal RwLock/Mutex).
unsafe impl Send for EmbeddedBackend {}
unsafe impl Sync for EmbeddedBackend {}

impl EmbeddedBackend {
    /// Create an embedded backend from a DB reference.
    ///
    /// # Safety
    /// The DB must outlive this backend.
    pub unsafe fn new(db: &DB) -> Self {
        Self {
            db: db as *const DB,
        }
    }

    fn db(&self) -> &DB {
        // SAFETY: caller guarantees DB outlives this backend.
        unsafe { &*self.db }
    }
}

impl Backend for EmbeddedBackend {
    fn ensure_namespace(&self, ns: &str) -> Result<()> {
        let _ = self.db().namespace(ns, None).map_err(error::storage)?;
        Ok(())
    }

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        let _ = self.db().drop_namespace(ns);
        Ok(())
    }

    fn get(&self, ns: &str, key: &str) -> Result<Option<Value>> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        match namespace.get(key) {
            Ok(v) => Ok(Some(v)),
            Err(rkv::Error::KeyNotFound) => Ok(None),
            Err(e) => Err(error::storage(e)),
        }
    }

    fn put(&self, ns: &str, key: &str, value: Value) -> Result<()> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        namespace.put(key, value, None).map_err(error::storage)?;
        Ok(())
    }

    fn delete(&self, ns: &str, key: &str) -> Result<()> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        match namespace.delete(key) {
            Ok(()) => Ok(()),
            Err(rkv::Error::KeyNotFound) => Ok(()),
            Err(e) => Err(error::storage(e)),
        }
    }

    fn exists(&self, ns: &str, key: &str) -> Result<bool> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        namespace.exists(key).map_err(error::storage)
    }

    fn scan(&self, ns: &str, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        let keys = namespace
            .scan(&Key::Str(prefix.to_owned()), limit, 0, false)
            .map_err(error::storage)?;
        Ok(keys
            .into_iter()
            .filter_map(|k| k.as_str().map(|s| s.to_owned()))
            .collect())
    }

    fn count(&self, ns: &str) -> Result<u64> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        namespace.count().map_err(error::storage)
    }

    fn rev_count(&self, ns: &str, key: &str) -> Result<u64> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        match namespace.rev_count(key) {
            Ok(c) => Ok(c),
            Err(rkv::Error::KeyNotFound) => Ok(0),
            Err(e) => Err(error::storage(e)),
        }
    }

    fn rev_get(&self, ns: &str, key: &str, index: u64) -> Result<Option<Value>> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        match namespace.rev_get(key, index) {
            Ok(v) => Ok(Some(v)),
            Err(rkv::Error::KeyNotFound) => Ok(None),
            Err(e) => Err(error::storage(e)),
        }
    }

    fn get_revision_id(&self, ns: &str, key: &str) -> Result<RevisionID> {
        let namespace = self.db().namespace(ns, None).map_err(error::storage)?;
        match namespace.get_with_revision(key) {
            Ok((_, rev)) => Ok(rev),
            Err(_) => Ok(RevisionID::ZERO),
        }
    }

    fn list_namespaces(&self, prefix: &str) -> Result<Vec<String>> {
        let all = self.db().list_namespaces().map_err(error::storage)?;
        Ok(all.into_iter().filter(|n| n.starts_with(prefix)).collect())
    }
}
