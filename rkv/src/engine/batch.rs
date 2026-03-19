use std::time::Duration;

use super::key::Key;
use super::value::Value;

/// A single operation within a [`WriteBatch`].
#[derive(Clone, Debug)]
pub enum BatchOp {
    /// Insert or update a key with an optional TTL.
    Put {
        key: Key,
        value: Value,
        ttl: Option<Duration>,
        /// Per-op dedup override: `Some(true)` forces dedup, `Some(false)` forces
        /// write, `None` uses the namespace/global setting.
        dedup: Option<bool>,
    },
    /// Delete a key (writes a tombstone).
    Delete { key: Key },
}

/// A collection of key-value operations to be applied atomically.
///
/// All operations in a `WriteBatch` are written to the append-only log and
/// applied to the memtable as a single unit — either all succeed or none are
/// visible. This is **not** a transaction (no read isolation, no rollback);
/// it is an atomic write group.
///
/// # Example
///
/// ```rust,ignore
/// use rkv::{Config, DB, WriteBatch};
///
/// let db = DB::open(Config::new("/tmp/batch_example")).unwrap();
/// let ns = db.namespace("_", None).unwrap();
///
/// let batch = WriteBatch::new()
///     .put("key1", "value1", None)
///     .put("key2", "value2", None)
///     .delete("old_key");
///
/// let revisions = ns.write_batch(batch).unwrap();
/// assert_eq!(revisions.len(), 3);
/// ```
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    pub(crate) ops: Vec<BatchOp>,
}

impl WriteBatch {
    /// Create an empty write batch.
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add a put operation to the batch.
    pub fn put(
        mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
    ) -> Self {
        self.ops.push(BatchOp::Put {
            key: key.into(),
            value: value.into(),
            ttl,
            dedup: None,
        });
        self
    }

    /// Add a put operation with an explicit per-op dedup override.
    pub fn put_dedup(
        mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl: Option<Duration>,
        dedup: Option<bool>,
    ) -> Self {
        self.ops.push(BatchOp::Put {
            key: key.into(),
            value: value.into(),
            ttl,
            dedup,
        });
        self
    }

    /// Add a delete operation to the batch.
    pub fn delete(mut self, key: impl Into<Key>) -> Self {
        self.ops.push(BatchOp::Delete { key: key.into() });
        self
    }

    /// Returns the number of operations in the batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns `true` if the batch contains no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Returns an iterator over the operations in the batch.
    pub fn iter(&self) -> std::slice::Iter<'_, BatchOp> {
        self.ops.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch() {
        let batch = WriteBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn put_operations() {
        let batch =
            WriteBatch::new()
                .put("k1", "v1", None)
                .put("k2", "v2", Some(Duration::from_secs(60)));
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn delete_operations() {
        let batch = WriteBatch::new().delete("k1").delete("k2");
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn mixed_operations() {
        let batch = WriteBatch::new().put("k1", "v1", None).delete("k2").put(
            "k3",
            "v3",
            Some(Duration::from_secs(3600)),
        );
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn builder_chain() {
        let batch = WriteBatch::new()
            .put(42_i64, "int_value", None)
            .delete(99_i64)
            .put("str_key", "str_value", None);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn default_is_empty() {
        let batch = WriteBatch::default();
        assert!(batch.is_empty());
    }

    #[test]
    fn clone_batch() {
        let batch = WriteBatch::new().put("k1", "v1", None).delete("k2");
        let cloned = batch.clone();
        assert_eq!(cloned.len(), 2);
    }
}
