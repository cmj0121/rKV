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

    /// Check if a key exists (non-expired, non-tombstone).
    pub(crate) fn exists(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    /// Returns the approximate memory usage in bytes.
    #[allow(dead_code)]
    pub(crate) fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    /// Returns true if the approximate size meets or exceeds the limit.
    #[allow(dead_code)]
    pub(crate) fn is_full(&self, limit: usize) -> bool {
        self.approximate_size >= limit
    }

    /// Total number of keys in the table (including tombstoned/expired).
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the table contains no keys.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Forward scan keys starting from `prefix`.
    ///
    /// - **Ordered mode** (Int keys): range scan from `prefix` in ascending order.
    /// - **Unordered mode** (Str keys): prefix matching on string representation.
    ///
    /// Tombstoned and expired keys are excluded.
    pub(crate) fn scan(&self, prefix: &Key, limit: usize) -> Vec<Key> {
        if self.ordered_mode {
            self.entries
                .range(prefix..)
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
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
    pub(crate) fn rscan(&self, prefix: &Key, limit: usize) -> Vec<Key> {
        if self.ordered_mode {
            // Collect keys >= prefix, then reverse
            // For rscan we want keys <= prefix in descending order
            // Actually per the plan: rscan returns keys in descending order
            // We scan from prefix downward
            self.entries
                .range(..=prefix.clone())
                .rev()
                .filter(|(_, entries)| self.is_live(entries))
                .map(|(k, _)| k.clone())
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
        let key_size = match key {
            Key::Int(_) => 8,
            Key::Str(s) => s.len(),
        };
        // key + value bytes + RevisionID (16) + Option<Instant> (16) + overhead
        key_size + value.len() + 16 + 16 + 32
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
    fn scan_ordered_mode() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(2u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(3u128), None);

        let keys = mt.scan(&Key::Int(1), 10);
        assert_eq!(keys, vec![Key::Int(1), Key::Int(2), Key::Int(3)]);
    }

    #[test]
    fn scan_ordered_with_limit() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let keys = mt.scan(&Key::Int(1), 2);
        assert_eq!(keys, vec![Key::Int(1), Key::Int(2)]);
    }

    #[test]
    fn scan_excludes_tombstones() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.delete(Key::Int(2), RevisionID::from(3u128));

        let keys = mt.scan(&Key::Int(1), 10);
        assert_eq!(keys, vec![Key::Int(1)]);
    }

    #[test]
    fn rscan_ordered_mode() {
        let mut mt = MemTable::new();
        mt.put(Key::Int(1), Value::from("a"), RevisionID::from(1u128), None);
        mt.put(Key::Int(2), Value::from("b"), RevisionID::from(2u128), None);
        mt.put(Key::Int(3), Value::from("c"), RevisionID::from(3u128), None);

        let keys = mt.rscan(&Key::Int(3), 10);
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

        let keys = mt.scan(&Key::from("user:"), 10);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Key::from("user:1")));
        assert!(keys.contains(&Key::from("user:2")));
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
