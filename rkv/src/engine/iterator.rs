use super::error::Result;
use super::key::Key;
use super::merge_iter::{MergeIterator, ReverseMergeIterator};
use super::value::Value;
use super::DB;

// ---------------------------------------------------------------------------
// IterInner — forward or reverse iteration
// ---------------------------------------------------------------------------

enum IterInner {
    Forward(MergeIterator),
    Reverse(ReverseMergeIterator),
}

// ---------------------------------------------------------------------------
// KeyIterator — yields only keys, skips tombstones
// ---------------------------------------------------------------------------

/// Lazy iterator over keys in a namespace.
///
/// Skips tombstoned entries automatically. Obtained via
/// [`Namespace::keys`](crate::Namespace::keys) or
/// [`Namespace::rkeys`](crate::Namespace::rkeys).
pub struct KeyIterator {
    inner: IterInner,
    error: bool,
}

impl KeyIterator {
    pub(crate) fn forward(iter: MergeIterator) -> Self {
        Self {
            inner: IterInner::Forward(iter),
            error: false,
        }
    }

    pub(crate) fn reverse(iter: ReverseMergeIterator) -> Self {
        Self {
            inner: IterInner::Reverse(iter),
            error: false,
        }
    }
}

impl Iterator for KeyIterator {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error {
            return None;
        }
        loop {
            let result = match &mut self.inner {
                IterInner::Forward(iter) => iter.next(),
                IterInner::Reverse(iter) => iter.next(),
            };
            match result {
                Ok(Some((key, value))) => {
                    if value.is_tombstone() {
                        continue;
                    }
                    return Some(Ok(key));
                }
                Ok(None) => return None,
                Err(e) => {
                    self.error = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EntryIterator — yields (Key, Value) pairs, resolves ValuePointers
// ---------------------------------------------------------------------------

/// Lazy iterator over key-value pairs in a namespace.
///
/// Skips tombstoned entries automatically and resolves `ValuePointer`s
/// into their actual data transparently. Obtained via
/// [`Namespace::entries`](crate::Namespace::entries) or
/// [`Namespace::rentries`](crate::Namespace::rentries).
pub struct EntryIterator<'db> {
    inner: IterInner,
    db: &'db DB,
    ns: String,
    encryption_key: Option<[u8; 32]>,
    error: bool,
}

impl<'db> EntryIterator<'db> {
    pub(crate) fn forward(
        iter: MergeIterator,
        db: &'db DB,
        ns: String,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            inner: IterInner::Forward(iter),
            db,
            ns,
            encryption_key,
            error: false,
        }
    }

    pub(crate) fn reverse(
        iter: ReverseMergeIterator,
        db: &'db DB,
        ns: String,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            inner: IterInner::Reverse(iter),
            db,
            ns,
            encryption_key,
            error: false,
        }
    }

    /// Resolve a value: dereference pointers and decrypt if needed.
    fn resolve(&self, value: Value) -> Result<Value> {
        let value = self.db.resolve_value(&self.ns, &value)?;
        if let Some(ref key) = self.encryption_key {
            if let Value::Data(ref ciphertext) = value {
                let plaintext = super::crypto::decrypt(key, ciphertext)?;
                return Ok(Value::Data(plaintext));
            }
        }
        Ok(value)
    }
}

impl Iterator for EntryIterator<'_> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error {
            return None;
        }
        loop {
            let result = match &mut self.inner {
                IterInner::Forward(iter) => iter.next(),
                IterInner::Reverse(iter) => iter.next(),
            };
            match result {
                Ok(Some((key, value))) => {
                    if value.is_tombstone() {
                        continue;
                    }
                    match self.resolve(value) {
                        Ok(v) => return Some(Ok((key, v))),
                        Err(e) => {
                            self.error = true;
                            return Some(Err(e));
                        }
                    }
                }
                Ok(None) => return None,
                Err(e) => {
                    self.error = true;
                    return Some(Err(e));
                }
            }
        }
    }
}
