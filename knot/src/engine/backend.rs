use rkv::{RevisionID, Value};

use super::error::Result;

/// Abstraction over rKV storage. Implemented for embedded (`&DB`) and
/// remote (HTTP client) backends.
pub trait Backend: Send + Sync {
    /// Ensure a namespace exists (create if needed). No-op if exists.
    fn ensure_namespace(&self, ns: &str) -> Result<()>;

    /// Drop a namespace and all its data.
    fn drop_namespace(&self, ns: &str) -> Result<()>;

    /// Get a value by key in a namespace.
    fn get(&self, ns: &str, key: &str) -> Result<Option<Value>>;

    /// Put a value by key in a namespace.
    fn put(&self, ns: &str, key: &str, value: Value) -> Result<()>;

    /// Delete a key in a namespace. No-op if not found.
    fn delete(&self, ns: &str, key: &str) -> Result<()>;

    /// Check if a key exists in a namespace.
    fn exists(&self, ns: &str, key: &str) -> Result<bool>;

    /// Scan keys by prefix in a namespace.
    fn scan(&self, ns: &str, prefix: &str, limit: usize) -> Result<Vec<String>>;

    /// Count all keys in a namespace.
    fn count(&self, ns: &str) -> Result<u64>;

    /// Get the number of revisions for a key. Returns 0 if key doesn't exist.
    fn rev_count(&self, ns: &str, key: &str) -> Result<u64>;

    /// Get a specific revision of a key by index.
    fn rev_get(&self, ns: &str, key: &str, index: u64) -> Result<Option<Value>>;

    /// Get the current revision ID for a key.
    fn get_revision_id(&self, ns: &str, key: &str) -> Result<RevisionID>;

    /// List all rKV namespaces matching a prefix. Used to discover knot namespaces.
    fn list_namespaces(&self, prefix: &str) -> Result<Vec<String>>;
}
