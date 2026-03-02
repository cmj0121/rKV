use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use super::key::Key;
use super::revision::RevisionID;
use super::value::Value;

/// A single revision record stored in the MemTable.
pub(crate) struct MemEntry {
    #[allow(dead_code)]
    pub revision: RevisionID,
    pub value: Value,
    pub expires_at: Option<Instant>,
}

/// Result of a memtable key lookup — distinguishes "key is tombstoned"
/// from "key was never written".
pub(crate) enum MemLookup<'a> {
    /// Key exists with a live (non-expired) value.
    Found(&'a Value),
    /// Key exists but is tombstoned or expired — do NOT fall through to SSTables.
    Tombstone,
    /// Key was never written to this memtable — caller should check SSTables.
    NotFound,
}

/// Like `MemLookup` but also carries the revision for the found value.
pub(crate) enum MemLookupRev<'a> {
    Found(&'a Value, RevisionID),
    Tombstone,
    NotFound,
}

/// In-memory sorted write buffer (memtable).
///
/// Holds all active writes for a namespace before they are flushed to disk.
/// Keys are stored in a BTreeMap for ordered iteration; each key maps to a
/// Vec of MemEntry records (oldest first) for revision history.
pub(crate) struct MemTable {
    entries: BTreeMap<Key, Vec<MemEntry>>,
    last_rev: HashMap<Key, RevisionID>,
    approximate_size: usize,
    ordered_mode: bool,
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_rev: HashMap::new(),
            approximate_size: 0,
            ordered_mode: true,
        }
    }

    /// Insert a key-value pair with an optional TTL.
    ///
    /// Returns the actual RevisionID used (may differ from `rev` due to
    /// per-key monotonicity enforcement).
    pub(crate) fn put(
        &mut self,
        key: Key,
        value: Value,
        rev: RevisionID,
        ttl: Option<Duration>,
    ) -> RevisionID {
        // Auto-upgrade: first Str key triggers irreversible widening
        if matches!(key, Key::Str(_)) && self.ordered_mode {
            self.upgrade_to_unordered();
        }

        // Per-key monotonicity: ensure rev > last_rev for this key
        let actual_rev = if let Some(&last) = self.last_rev.get(&key) {
            if rev <= last {
                RevisionID::from(last.as_u128() + 1)
            } else {
                rev
            }
        } else {
            rev
        };
        self.last_rev.insert(key.clone(), actual_rev);

        let expires_at = ttl.map(|d| Instant::now() + d);

        // Track approximate size
        self.approximate_size += Self::entry_size(&key, &value);

        let entry = MemEntry {
            revision: actual_rev,
            value,
            expires_at,
        };

        self.entries.entry(key).or_default().push(entry);

        actual_rev
    }

    /// Insert a value only if `rev` is strictly greater than the current
    /// revision for this key. Returns `true` if the write was applied.
    #[allow(dead_code)]
    ///
    /// Unlike `put()`, this does NOT bump the revision for monotonicity —
    /// the incoming revision is used as-is. This is the LWW path used for
    /// peer-replicated writes.
    pub(crate) fn put_if_newer(
        &mut self,
        key: Key,
        value: Value,
        rev: RevisionID,
        ttl: Option<Duration>,
    ) -> bool {
        if let Some(&last) = self.last_rev.get(&key) {
            if rev <= last {
                return false;
            }
        }

        // Auto-upgrade: first Str key triggers irreversible widening
        if matches!(key, Key::Str(_)) && self.ordered_mode {
            self.upgrade_to_unordered();
        }

        self.last_rev.insert(key.clone(), rev);

        let expires_at = ttl.map(|d| Instant::now() + d);
        self.approximate_size += Self::entry_size(&key, &value);

        let entry = MemEntry {
            revision: rev,
            value,
            expires_at,
        };
        self.entries.entry(key).or_default().push(entry);

        true
    }

    /// Insert a tombstone for the given key.
    pub(crate) fn delete(&mut self, key: Key, rev: RevisionID) -> RevisionID {
        self.put(key, Value::tombstone(), rev, None)
    }

    /// Get the latest non-expired, non-tombstone value for a key.
    pub(crate) fn get(&self, key: &Key) -> Option<&Value> {
        let entries = self.entries.get(key)?;
        let latest = entries.last()?;

        // Check expiration
        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return None;
            }
        }

        // Check tombstone
        if latest.value.is_tombstone() {
            return None;
        }

        Some(&latest.value)
    }

    /// Three-state lookup: Found / Tombstone / NotFound.
    ///
    /// Unlike `get()`, this distinguishes "key is tombstoned" from "key was
    /// never written". `Namespace::get()` uses this to avoid falling through
    /// to stale SSTable data when the memtable holds a tombstone.
    pub(crate) fn lookup(&self, key: &Key) -> MemLookup<'_> {
        let Some(entries) = self.entries.get(key) else {
            return MemLookup::NotFound;
        };
        let Some(latest) = entries.last() else {
            return MemLookup::NotFound;
        };

        // Expired → treat as tombstone (do not fall through)
        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return MemLookup::Tombstone;
            }
        }

        if latest.value.is_tombstone() {
            return MemLookup::Tombstone;
        }

        MemLookup::Found(&latest.value)
    }

    /// Like `lookup` but also returns the revision for found values.
    pub(crate) fn lookup_with_revision(&self, key: &Key) -> MemLookupRev<'_> {
        let Some(entries) = self.entries.get(key) else {
            return MemLookupRev::NotFound;
        };
        let Some(latest) = entries.last() else {
            return MemLookupRev::NotFound;
        };

        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return MemLookupRev::Tombstone;
            }
        }

        if latest.value.is_tombstone() {
            return MemLookupRev::Tombstone;
        }

        MemLookupRev::Found(&latest.value, latest.revision)
    }

    /// Check if a key exists (non-expired, non-tombstone).
    pub(crate) fn exists(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    /// Returns the approximate memory usage in bytes.
    pub(crate) fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    /// Returns true if the memtable is in ordered mode (Int keys only).
    pub(crate) fn is_ordered(&self) -> bool {
        self.ordered_mode
    }

    /// Returns true if the approximate size meets or exceeds the limit.
    #[cfg(test)]
    pub(crate) fn is_full(&self, limit: usize) -> bool {
        self.approximate_size >= limit
    }

    /// Total number of keys in the table (including tombstoned/expired).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the table contains no keys.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Forward scan keys starting from `prefix`.
    ///
    /// - **Ordered mode** (Int keys): range scan from `prefix` in ascending order.
    /// - **Unordered mode** (Str keys): prefix matching on string representation.
    ///
    /// Tombstoned and expired keys are excluded.
    #[cfg(test)]
    pub(crate) fn scan(&self, prefix: &Key, limit: usize, offset: usize) -> Vec<Key> {
        if self.ordered_mode {
            self.entries
                .range(prefix..)
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
                .skip(offset)
                .take(limit)
                .collect()
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .filter(|(k, entries)| {
                    k.to_string().starts_with(&prefix_str) && self.is_live(entries)
                })
                .map(|(k, _)| k.clone())
                .skip(offset)
                .take(limit)
                .collect()
        }
    }

    /// Reverse scan keys starting from `prefix`.
    ///
    /// - **Ordered mode**: range scan from `prefix` in descending order.
    /// - **Unordered mode**: prefix matching, reverse iteration order.
    ///
    /// Tombstoned and expired keys are excluded.
    #[cfg(test)]
    pub(crate) fn rscan(&self, prefix: &Key, limit: usize, offset: usize) -> Vec<Key> {
        if self.ordered_mode {
            self.entries
                .range(..=prefix.clone())
                .rev()
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
                .skip(offset)
                .take(limit)
                .collect()
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .rev()
                .filter(|(k, entries)| {
                    k.to_string().starts_with(&prefix_str) && self.is_live(entries)
                })
                .map(|(k, _)| k.clone())
                .skip(offset)
                .take(limit)
                .collect()
        }
    }

    /// Return all raw `(Key, Value)` pairs matching a prefix, including
    /// tombstones and expired entries (surfaced as tombstones).
    ///
    /// No limit/offset — returns everything. Used by the merged scan to
    /// overlay MemTable entries on top of SSTable results.
    pub(crate) fn scan_all_raw(&self, prefix: &Key) -> Vec<(Key, Value)> {
        if self.ordered_mode {
            // In ordered mode, range(Key::Str("")..) misses all Int keys
            // because Key::Int < Key::Str. An empty Str prefix means
            // "scan everything", so iterate all entries.
            if *prefix == Key::Str(String::new()) {
                self.entries
                    .iter()
                    .map(|(k, entries)| (k.clone(), Self::latest_or_tombstone(entries)))
                    .collect()
            } else {
                self.entries
                    .range(prefix..)
                    .map(|(k, entries)| (k.clone(), Self::latest_or_tombstone(entries)))
                    .collect()
            }
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .filter(|(k, _)| k.to_string().starts_with(&prefix_str))
                .map(|(k, entries)| (k.clone(), Self::latest_or_tombstone(entries)))
                .collect()
        }
    }

    /// Return all raw `(Key, Value)` pairs matching a prefix in reverse,
    /// including tombstones and expired entries (surfaced as tombstones).
    ///
    /// For ordered mode: returns keys <= prefix. For unordered mode: prefix
    /// matching (same as forward). Used by merged rscan.
    pub(crate) fn rscan_all_raw(&self, prefix: &Key) -> Vec<(Key, Value)> {
        if self.ordered_mode {
            self.entries
                .range(..=prefix.clone())
                .map(|(k, entries)| (k.clone(), Self::latest_or_tombstone(entries)))
                .collect()
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .filter(|(k, _)| k.to_string().starts_with(&prefix_str))
                .map(|(k, entries)| (k.clone(), Self::latest_or_tombstone(entries)))
                .collect()
        }
    }

    /// Forward scan keys starting from `prefix`, returning raw `(Key, Value)` pairs.
    ///
    /// Like `scan()` but includes tombstones (needed to shadow SSTable entries
    /// during merge). Expired entries are still skipped.
    #[cfg(test)]
    pub(crate) fn scan_raw(&self, prefix: &Key, limit: usize, offset: usize) -> Vec<(Key, Value)> {
        if self.ordered_mode {
            self.entries
                .range(prefix..)
                .filter(|(_, entries)| self.is_not_expired(entries))
                .map(|(k, entries)| (k.clone(), entries.last().unwrap().value.clone()))
                .skip(offset)
                .take(limit)
                .collect()
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .filter(|(k, entries)| {
                    k.to_string().starts_with(&prefix_str) && self.is_not_expired(entries)
                })
                .map(|(k, entries)| (k.clone(), entries.last().unwrap().value.clone()))
                .skip(offset)
                .take(limit)
                .collect()
        }
    }

    /// Reverse scan keys starting from `prefix`, returning raw `(Key, Value)` pairs.
    ///
    /// Like `rscan()` but includes tombstones. Expired entries are skipped.
    #[cfg(test)]
    pub(crate) fn rscan_raw(&self, prefix: &Key, limit: usize, offset: usize) -> Vec<(Key, Value)> {
        if self.ordered_mode {
            self.entries
                .range(..=prefix.clone())
                .rev()
                .filter(|(_, entries)| self.is_not_expired(entries))
                .map(|(k, entries)| (k.clone(), entries.last().unwrap().value.clone()))
                .skip(offset)
                .take(limit)
                .collect()
        } else {
            let prefix_str = prefix.to_string();
            self.entries
                .iter()
                .rev()
                .filter(|(k, entries)| {
                    k.to_string().starts_with(&prefix_str) && self.is_not_expired(entries)
                })
                .map(|(k, entries)| (k.clone(), entries.last().unwrap().value.clone()))
                .skip(offset)
                .take(limit)
                .collect()
        }
    }

    /// Count live (non-tombstone, non-expired) keys.
    pub(crate) fn count(&self) -> u64 {
        self.entries
            .iter()
            .filter(|(_, entries)| self.is_live(entries))
            .count() as u64
    }

    /// Returns the total number of revisions for a key, or None if the key
    /// was never written.
    pub(crate) fn rev_count(&self, key: &Key) -> Option<u64> {
        self.entries.get(key).map(|entries| entries.len() as u64)
    }

    /// Returns the value at a specific revision index (0 = oldest).
    /// Returns None if key doesn't exist or index is out of bounds.
    pub(crate) fn rev_get(&self, key: &Key, index: u64) -> Option<&Value> {
        let entries = self.entries.get(key)?;
        entries.get(index as usize).map(|e| &e.value)
    }

    /// Returns the value and remaining TTL at a specific revision index.
    ///
    /// Returns `(value, expired, remaining_ttl)`:
    /// - `expired`: true if the revision had a TTL that has elapsed
    /// - `remaining_ttl`: `Some(duration)` if TTL is set and not expired, `None` otherwise
    pub(crate) fn rev_get_with_ttl(
        &self,
        key: &Key,
        index: u64,
    ) -> Option<(&Value, bool, Option<Duration>)> {
        let entries = self.entries.get(key)?;
        let entry = entries.get(index as usize)?;
        let (expired, remaining) = match entry.expires_at {
            Some(expires_at) => {
                let now = Instant::now();
                if now > expires_at {
                    (true, None)
                } else {
                    (false, Some(expires_at - now))
                }
            }
            None => (false, None),
        };
        Some((&entry.value, expired, remaining))
    }

    /// Returns the remaining TTL for a key.
    ///
    /// - `None` — key not found or expired
    /// - `Some(None)` — key exists but has no TTL
    /// - `Some(Some(duration))` — remaining duration
    pub(crate) fn ttl(&self, key: &Key) -> Option<Option<Duration>> {
        let entries = self.entries.get(key)?;
        let latest = entries.last()?;

        // Tombstoned keys are invisible
        if latest.value.is_tombstone() {
            return None;
        }

        match latest.expires_at {
            Some(expires_at) => {
                let now = Instant::now();
                if now > expires_at {
                    None // expired
                } else {
                    Some(Some(expires_at - now))
                }
            }
            None => Some(None), // no TTL
        }
    }

    /// Collect live keys in the range `[start, end)` or `[start, end]`.
    ///
    /// Uses BTreeMap ordering (works for both Int and Str keys).
    /// Tombstoned and expired keys are excluded.
    pub(crate) fn keys_in_range(&self, start: &Key, end: &Key, inclusive: bool) -> Vec<Key> {
        if inclusive {
            self.entries
                .range(start.clone()..=end.clone())
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            self.entries
                .range(start.clone()..end.clone())
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
                .collect()
        }
    }

    /// Collect live keys whose string representation starts with `prefix`.
    ///
    /// Works regardless of ordered/unordered mode — always uses string
    /// prefix matching for consistent behavior.
    pub(crate) fn keys_with_prefix(&self, prefix: &str) -> Vec<Key> {
        self.entries
            .iter()
            .filter(|(k, entries)| k.to_string().starts_with(prefix) && self.is_live(entries))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Drain the latest value for each key in sorted order.
    ///
    /// Returns a `Vec<(Key, Value, RevisionID)>` containing the most recent
    /// non-expired value (plus its revision) for every key, **including
    /// tombstones** (needed for correctness when flushing to SSTable — a
    /// tombstone must shadow older SSTables).
    ///
    /// Expired entries are flushed as tombstones so they remain visible to
    /// "show deleted" scans after the memtable is drained. Compaction will
    /// eventually garbage-collect them.
    pub(crate) fn drain_latest(&mut self) -> Vec<(Key, Value, RevisionID)> {
        let entries = std::mem::take(&mut self.entries);
        self.last_rev.clear();
        self.approximate_size = 0;

        let mut result = Vec::with_capacity(entries.len());
        for (key, revisions) in entries {
            if let Some(latest) = revisions.last() {
                let rev = latest.revision;
                if let Some(expires_at) = latest.expires_at {
                    if Instant::now() > expires_at {
                        // Expired → flush as tombstone so "show deleted" works
                        result.push((key, Value::tombstone(), rev));
                        continue;
                    }
                }
                result.push((key, latest.value.clone(), rev));
            }
        }
        result
    }

    /// Return the latest value, or a tombstone if the entry is expired.
    fn latest_or_tombstone(entries: &[MemEntry]) -> Value {
        let latest = entries.last().expect("non-empty entries");
        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return Value::tombstone();
            }
        }
        latest.value.clone()
    }

    /// Check if the latest entry for a key is not expired (may be a tombstone).
    #[cfg(test)]
    fn is_not_expired(&self, entries: &[MemEntry]) -> bool {
        let Some(latest) = entries.last() else {
            return false;
        };
        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return false;
            }
        }
        true
    }

    /// Check if the latest entry for a key is live (non-tombstone, non-expired).
    fn is_live(&self, entries: &[MemEntry]) -> bool {
        let Some(latest) = entries.last() else {
            return false;
        };
        if latest.value.is_tombstone() {
            return false;
        }
        if let Some(expires_at) = latest.expires_at {
            if Instant::now() > expires_at {
                return false;
            }
        }
        true
    }

    /// Estimate the memory footprint of a single entry.
    fn entry_size(key: &Key, value: &Value) -> usize {
        use super::value::ValuePointer;

        let key_size = match key {
            Key::Int(_) => 8,
            Key::Str(s) => s.len(),
        };
        let value_size = match value {
            Value::Pointer(_) => ValuePointer::encoded_size(),
            _ => value.len(),
        };
        // key + value bytes + RevisionID (16) + Option<Instant> (16) + overhead
        key_size + value_size + 16 + 16 + 32
    }

    /// Widen all Int keys to Str and rebuild internal maps.
    fn upgrade_to_unordered(&mut self) {
        self.ordered_mode = false;

        let old_entries = std::mem::take(&mut self.entries);
        let old_last_rev = std::mem::take(&mut self.last_rev);

        for (key, entries) in old_entries {
            let widened = key.widen();
            self.entries.insert(widened, entries);
        }

        for (key, rev) in old_last_rev {
            let widened = key.widen();
            self.last_rev.insert(widened, rev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let mt = MemTable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.approximate_size(), 0);
    }

    #[test]
    fn put_and_get() {
        let mut mt = MemTable::new();
        let rev = RevisionID::from(1u128);
        mt.put(Key::Int(1), Value::from("hello"), rev, None);

        let val = mt.get(&Key::Int(1)).unwrap();
        assert_eq!(val, &Value::from("hello"));
    }

    #[test]
    fn put_overwrites() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("v1"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::Int(1),
            Value::from("v2"),
            RevisionID::from(2u128),
            None,
        );

        let val = mt.get(&Key::Int(1)).unwrap();
        assert_eq!(val, &Value::from("v2"));
    }

    #[test]
    fn get_missing_key() {
        let mt = MemTable::new();
        assert!(mt.get(&Key::Int(1)).is_none());
    }

    #[test]
    fn delete_makes_key_invisible() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("hello"),
            RevisionID::from(1u128),
            None,
        );
        mt.delete(Key::Int(1), RevisionID::from(2u128));

        assert!(mt.get(&Key::Int(1)).is_none());
        assert!(!mt.exists(&Key::Int(1)));
    }

    #[test]
    fn exists_after_put() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("hello"),
            RevisionID::from(1u128),
            None,
        );
        assert!(mt.exists(&Key::Int(1)));
    }

    #[test]
    fn exists_missing_key() {
        let mt = MemTable::new();
        assert!(!mt.exists(&Key::Int(1)));
    }

    #[test]
    fn len_tracks_distinct_keys() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(1), Value::from("c"), RevisionID::from(3u128), None); // overwrite

        assert_eq!(mt.len(), 2);
        assert!(!mt.is_empty());
    }

    #[test]
    fn approximate_size_grows() {
        let mut mt = MemTable::new();
        assert_eq!(mt.approximate_size(), 0);

        mt.put(
            Key::Int(1),
            Value::from("hello"),
            RevisionID::from(1u128),
            None,
        );
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn is_full_check() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("hello"),
            RevisionID::from(1u128),
            None,
        );

        assert!(mt.is_full(1)); // very low limit
        assert!(!mt.is_full(1_000_000)); // very high limit
    }

    #[test]
    fn per_key_monotonicity() {
        let mut mt = MemTable::new();
        let r1 = mt.put(
            Key::Int(1),
            Value::from("a"),
            RevisionID::from(100u128),
            None,
        );
        // Same rev should get bumped
        let r2 = mt.put(
            Key::Int(1),
            Value::from("b"),
            RevisionID::from(50u128),
            None,
        );

        assert_eq!(r1, RevisionID::from(100u128));
        assert!(r2 > r1);
    }

    #[test]
    fn upgrade_widens_int_keys() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(42),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        // Insert a Str key triggers upgrade
        mt.put(
            Key::from("hello"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );

        // Original Int(42) is now Str("42")
        assert!(mt.get(&Key::Int(42)).is_none());
        assert_eq!(mt.get(&Key::from("42")).unwrap(), &Value::from("a"));
        assert_eq!(mt.get(&Key::from("hello")).unwrap(), &Value::from("b"));
    }

    // --- Scan ---

    #[test]
    fn scan_all_raw_ordered_empty_prefix_includes_int_keys() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        assert!(mt.is_ordered());

        // Empty Str prefix = "scan everything" — must include Int keys
        let entries = mt.scan_all_raw(&Key::Str(String::new()));
        assert_eq!(entries.len(), 2, "Int keys must be visible: {entries:?}");
        assert_eq!(entries[0].0, Key::Int(1));
        assert_eq!(entries[1].0, Key::Int(2));
    }

    #[test]
    fn scan_ordered_mode() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(2u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(3u128), None);

        let keys = mt.scan(&Key::Int(1), 10, 0);
        assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
    }

    #[test]
    fn scan_ordered_with_limit() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let keys = mt.scan(&Key::Int(1), 2, 0);
        assert_eq!(keys, vec![Key::Int(1), Key::Int(2)]);
    }

    #[test]
    fn scan_excludes_tombstones() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.delete(Key::Int(2), RevisionID::from(3u128));

        let keys = mt.scan(&Key::Int(1), 10, 0);
        assert_eq!(keys, vec![Key::Int(1)]);
    }

    #[test]
    fn rscan_ordered_mode() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let keys = mt.rscan(&Key::Int(3), 10, 0);
        assert_eq!(keys, vec![Key::Int(3), Key::Int(2), Key::Int(1)]);
    }

    #[test]
    fn scan_unordered_prefix_matching() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("user:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("user:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("post:1"),
            Value::from("c"),
            RevisionID::from(3u128),
            None,
        );

        let keys = mt.scan(&Key::from("user:"), 10, 0);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::from("user:1")));
        assert!(keys.contains(&Key::from("user:2")));
    }

    #[test]
    fn scan_with_offset() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);
        mt.put(Key::Int(4), Value::from("d"), RevisionID::from(4u128), None);
        mt.put(Key::Int(5), Value::from("e"), RevisionID::from(5u128), None);

        // Skip 2, take 2
        let keys = mt.scan(&Key::Int(1), 2, 2);
        assert_eq!(keys, vec![Key::Int(3), Key::Int(4)]);

        // Offset beyond results
        let keys = mt.scan(&Key::Int(1), 10, 10);
        assert!(keys.is_empty());
    }

    // --- Count ---

    #[test]
    fn count_live_keys() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.delete(Key::Int(2), RevisionID::from(3u128));

        assert_eq!(mt.count(), 1);
    }

    #[test]
    fn count_empty() {
        let mt = MemTable::new();
        assert_eq!(mt.count(), 0);
    }

    // --- Revision history ---

    #[test]
    fn rev_count_tracks_history() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("v1"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::Int(1),
            Value::from("v2"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::Int(1),
            Value::from("v3"),
            RevisionID::from(3u128),
            None,
        );

        assert_eq!(mt.rev_count(&Key::Int(1)), Some(3));
    }

    #[test]
    fn rev_count_missing_key() {
        let mt = MemTable::new();
        assert_eq!(mt.rev_count(&Key::Int(1)), None);
    }

    #[test]
    fn rev_get_by_index() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("v1"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::Int(1),
            Value::from("v2"),
            RevisionID::from(2u128),
            None,
        );

        assert_eq!(mt.rev_get(&Key::Int(1), 0).unwrap(), &Value::from("v1"));
        assert_eq!(mt.rev_get(&Key::Int(1), 1).unwrap(), &Value::from("v2"));
        assert!(mt.rev_get(&Key::Int(1), 2).is_none());
    }

    #[test]
    fn rev_get_missing_key() {
        let mt = MemTable::new();
        assert!(mt.rev_get(&Key::Int(1), 0).is_none());
    }

    // --- TTL ---

    #[test]
    fn ttl_no_expiration() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);

        assert_eq!(mt.ttl(&Key::Int(1)), Some(None));
    }

    #[test]
    fn ttl_with_expiration() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("a"),
            RevisionID::from(1u128),
            Some(Duration::from_secs(60)),
        );

        let remaining = mt.ttl(&Key::Int(1)).unwrap().unwrap();
        assert!(remaining.as_secs() > 50);
    }

    #[test]
    fn ttl_missing_key() {
        let mt = MemTable::new();
        assert_eq!(mt.ttl(&Key::Int(1)), None);
    }

    #[test]
    fn ttl_tombstoned_key() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.delete(Key::Int(1), RevisionID::from(2u128));

        assert_eq!(mt.ttl(&Key::Int(1)), None);
    }

    // --- drain_latest ---

    #[test]
    fn drain_latest_returns_sorted_entries() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(1u128), None);
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(2u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(3u128), None);

        let drained = mt.drain_latest();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].0, Key::Int(1));
        assert_eq!(drained[0].1, Value::from("a"));
        assert_eq!(drained[1].0, Key::Int(2));
        assert_eq!(drained[1].1, Value::from("b"));
        assert_eq!(drained[2].0, Key::Int(3));
        assert_eq!(drained[2].1, Value::from("c"));
    }

    #[test]
    fn drain_latest_takes_most_recent_revision() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("v1"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::Int(1),
            Value::from("v2"),
            RevisionID::from(2u128),
            None,
        );

        let drained = mt.drain_latest();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1, Value::from("v2"));
    }

    #[test]
    fn drain_latest_includes_tombstones() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.delete(Key::Int(1), RevisionID::from(2u128));

        let drained = mt.drain_latest();
        assert_eq!(drained.len(), 1);
        assert!(drained[0].1.is_tombstone());
    }

    #[test]
    fn drain_latest_converts_expired_to_tombstone() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("a"),
            RevisionID::from(1u128),
            Some(Duration::from_millis(1)),
        );
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);

        std::thread::sleep(Duration::from_millis(10));

        let drained = mt.drain_latest();
        // Expired entry is flushed as tombstone (not skipped) so
        // "show deleted" scans work after flush.
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].0, Key::Int(1));
        assert!(
            drained[0].1.is_tombstone(),
            "expired entry should be a tombstone"
        );
        assert_eq!(drained[1].0, Key::Int(2));
        assert!(
            !drained[1].1.is_tombstone(),
            "live entry should not be a tombstone"
        );
    }

    #[test]
    fn drain_latest_clears_memtable() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);

        let _ = mt.drain_latest();
        assert!(mt.is_empty());
        assert_eq!(mt.approximate_size(), 0);
        assert_eq!(mt.count(), 0);
    }

    #[test]
    fn drain_latest_preserves_ordered_mode() {
        let mut mt = MemTable::new();
        // Insert Str key to trigger unordered mode
        mt.put(
            Key::from("hello"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        assert!(!mt.ordered_mode);

        let _ = mt.drain_latest();
        // ordered_mode should be preserved after drain
        assert!(!mt.ordered_mode);
    }

    #[test]
    fn drain_latest_empty_memtable() {
        let mut mt = MemTable::new();
        let drained = mt.drain_latest();
        assert!(drained.is_empty());
    }

    // --- Unordered rscan ---

    #[test]
    fn rscan_unordered_prefix_matching() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("user:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("user:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("post:1"),
            Value::from("c"),
            RevisionID::from(3u128),
            None,
        );

        let keys = mt.rscan(&Key::from("user:"), 10, 0);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::from("user:1")));
        assert!(keys.contains(&Key::from("user:2")));
    }

    #[test]
    fn rscan_unordered_with_offset() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("k:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("k:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("k:3"),
            Value::from("c"),
            RevisionID::from(3u128),
            None,
        );

        // Reversed BTree order + skip 1, take 1
        let keys = mt.rscan(&Key::from("k:"), 1, 1);
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn rscan_unordered_excludes_tombstones() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("x:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("x:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.delete(Key::from("x:2"), RevisionID::from(3u128));

        let keys = mt.rscan(&Key::from("x:"), 10, 0);
        assert_eq!(keys, vec![Key::from("x:1")]);
    }

    // --- scan_raw ---

    #[test]
    fn scan_raw_ordered() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.delete(Key::Int(3), RevisionID::from(3u128));

        let pairs = mt.scan_raw(&Key::Int(1), 10, 0);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (Key::Int(1), Value::from("a")));
        assert!(pairs[2].1.is_tombstone());
    }

    #[test]
    fn scan_raw_unordered_prefix() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("ns:a"),
            Value::from("1"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("ns:b"),
            Value::from("2"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("other:c"),
            Value::from("3"),
            RevisionID::from(3u128),
            None,
        );

        let pairs = mt.scan_raw(&Key::from("ns:"), 10, 0);
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn scan_raw_with_limit_offset() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let pairs = mt.scan_raw(&Key::Int(1), 1, 1);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, Key::Int(2));
    }

    // --- rscan_raw ---

    #[test]
    fn rscan_raw_ordered() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let pairs = mt.rscan_raw(&Key::Int(3), 10, 0);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, Key::Int(3));
        assert_eq!(pairs[2].0, Key::Int(1));
    }

    #[test]
    fn rscan_raw_unordered_prefix() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("r:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("r:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("other:1"),
            Value::from("c"),
            RevisionID::from(3u128),
            None,
        );

        let pairs = mt.rscan_raw(&Key::from("r:"), 10, 0);
        assert_eq!(pairs.len(), 2);
    }

    // --- rscan_all_raw ---

    #[test]
    fn rscan_all_raw_unordered() {
        let mut mt = MemTable::new();
        mt.put(
            Key::from("p:1"),
            Value::from("a"),
            RevisionID::from(1u128),
            None,
        );
        mt.put(
            Key::from("p:2"),
            Value::from("b"),
            RevisionID::from(2u128),
            None,
        );
        mt.put(
            Key::from("q:1"),
            Value::from("c"),
            RevisionID::from(3u128),
            None,
        );

        let pairs = mt.rscan_all_raw(&Key::from("p:"));
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|(k, _)| k.to_string().starts_with("p:")));
    }

    // --- scan_raw skips expired ---

    #[test]
    fn scan_raw_skips_expired() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("a"),
            RevisionID::from(1u128),
            Some(Duration::from_millis(1)),
        );
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);

        std::thread::sleep(Duration::from_millis(10));

        let pairs = mt.scan_raw(&Key::Int(1), 10, 0);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, Key::Int(2));
    }

    #[test]
    fn expired_key_invisible() {
        let mut mt = MemTable::new();
        mt.put(
            Key::Int(1),
            Value::from("a"),
            RevisionID::from(1u128),
            Some(Duration::from_millis(1)),
        );

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        assert!(mt.get(&Key::Int(1)).is_none());
        assert!(!mt.exists(&Key::Int(1)));
        assert_eq!(mt.ttl(&Key::Int(1)), None);
        assert_eq!(mt.count(), 0);
    }
}
