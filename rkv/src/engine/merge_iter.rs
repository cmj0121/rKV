#![allow(dead_code)] // consumed by later commits in this branch
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::error::Result;
use super::key::Key;
use super::value::Value;

// ---------------------------------------------------------------------------
// MergeSource trait
// ---------------------------------------------------------------------------

/// A source of sorted `(Key, Value)` pairs for the merge iterator.
pub(crate) trait MergeSource {
    /// Return the next entry, or `None` if exhausted.
    fn next_entry(&mut self) -> Result<Option<(Key, Value)>>;
}

// ---------------------------------------------------------------------------
// VecSource — wraps a pre-sorted Vec (for memtable snapshots)
// ---------------------------------------------------------------------------

pub(crate) struct VecSource {
    entries: std::vec::IntoIter<(Key, Value)>,
}

impl VecSource {
    pub(crate) fn new(entries: Vec<(Key, Value)>) -> Self {
        Self {
            entries: entries.into_iter(),
        }
    }
}

impl MergeSource for VecSource {
    fn next_entry(&mut self) -> Result<Option<(Key, Value)>> {
        Ok(self.entries.next())
    }
}

// ---------------------------------------------------------------------------
// HeapItem — wrapper for BinaryHeap (min-heap via Reverse ordering)
// ---------------------------------------------------------------------------

struct HeapItem {
    key: Key,
    value: Value,
    /// Higher priority wins during dedup. Memtable > L0 newest > L0 oldest > L1 > ...
    priority: u32,
    /// Index into the sources array.
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. We want min-key ordering, so reverse key comparison.
        // For same keys, higher priority should come first (so we DON'T reverse priority).
        other
            .key
            .cmp(&self.key)
            .then(self.priority.cmp(&other.priority))
    }
}

// ---------------------------------------------------------------------------
// MergeIterator — min-heap merge with dedup
// ---------------------------------------------------------------------------

/// A lazy merge iterator that streams deduplicated `(Key, Value)` pairs from
/// multiple sorted sources using a min-heap.
///
/// When multiple sources contain the same key, the source with the highest
/// priority wins. Tombstones are emitted (caller decides whether to filter).
pub(crate) struct MergeIterator {
    sources: Vec<Box<dyn MergeSource>>,
    priorities: Vec<u32>,
    heap: BinaryHeap<HeapItem>,
    initialized: bool,
}

impl MergeIterator {
    /// Create a new merge iterator from sources with assigned priorities.
    ///
    /// Each `(source, priority)` pair contributes entries. Higher priority
    /// values win during deduplication.
    pub(crate) fn new(sources: Vec<(Box<dyn MergeSource>, u32)>) -> Self {
        let (sources, priorities): (Vec<_>, Vec<_>) = sources.into_iter().unzip();
        Self {
            sources,
            priorities,
            heap: BinaryHeap::new(),
            initialized: false,
        }
    }

    /// Seed the heap with the first entry from each source.
    fn initialize(&mut self) -> Result<()> {
        for (idx, source) in self.sources.iter_mut().enumerate() {
            if let Some((key, value)) = source.next_entry()? {
                self.heap.push(HeapItem {
                    key,
                    value,
                    priority: self.priorities[idx],
                    source_idx: idx,
                });
            }
        }
        self.initialized = true;
        Ok(())
    }

    /// Refill a source: pop its next entry onto the heap.
    fn refill(&mut self, source_idx: usize) -> Result<()> {
        if let Some((key, value)) = self.sources[source_idx].next_entry()? {
            self.heap.push(HeapItem {
                key,
                value,
                priority: self.priorities[source_idx],
                source_idx,
            });
        }
        Ok(())
    }

    /// Return the next deduplicated `(Key, Value)` pair, or `None` if exhausted.
    pub(crate) fn next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.initialized {
            self.initialize()?;
        }

        let winner = match self.heap.pop() {
            Some(item) => item,
            None => return Ok(None),
        };

        // Refill the winner's source
        self.refill(winner.source_idx)?;

        let mut best_key = winner.key;
        let mut best_value = winner.value;
        let mut best_priority = winner.priority;

        // Drain all items with the same key (they're at the top due to ordering)
        while let Some(top) = self.heap.peek() {
            if top.key != best_key {
                break;
            }
            let dup = self.heap.pop().unwrap();
            self.refill(dup.source_idx)?;
            if dup.priority > best_priority {
                best_key = dup.key;
                best_value = dup.value;
                best_priority = dup.priority;
            }
        }

        Ok(Some((best_key, best_value)))
    }
}

// ---------------------------------------------------------------------------
// RScanAdapter — collects forward merge then reverses
// ---------------------------------------------------------------------------

/// Collects all entries from a MergeIterator, then yields them in reverse order.
pub(crate) struct RScanAdapter {
    entries: Vec<(Key, Value)>,
    pos: usize,
}

impl RScanAdapter {
    /// Create by draining the merge iterator.
    pub(crate) fn from_merge_iter(mut iter: MergeIterator) -> Result<Self> {
        let mut entries = Vec::new();
        while let Some(entry) = iter.next()? {
            entries.push(entry);
        }
        entries.reverse();
        Ok(Self { entries, pos: 0 })
    }

    pub(crate) fn next(&mut self) -> Option<(Key, Value)> {
        if self.pos < self.entries.len() {
            let idx = self.pos;
            self.pos += 1;
            Some(std::mem::replace(
                &mut self.entries[idx],
                (Key::Int(0), Value::Null),
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_source_basic() {
        let entries = vec![
            (Key::Int(1), Value::from("a")),
            (Key::Int(2), Value::from("b")),
            (Key::Int(3), Value::from("c")),
        ];
        let mut src = VecSource::new(entries);
        assert!(src.next_entry().unwrap().is_some());
        assert!(src.next_entry().unwrap().is_some());
        assert!(src.next_entry().unwrap().is_some());
        assert!(src.next_entry().unwrap().is_none());
    }

    #[test]
    fn merge_two_sorted_sources() {
        let s1 = VecSource::new(vec![
            (Key::Int(1), Value::from("a")),
            (Key::Int(3), Value::from("c")),
            (Key::Int(5), Value::from("e")),
        ]);
        let s2 = VecSource::new(vec![
            (Key::Int(2), Value::from("b")),
            (Key::Int(4), Value::from("d")),
        ]);

        let mut iter = MergeIterator::new(vec![(Box::new(s1), 1), (Box::new(s2), 0)]);

        let mut keys = Vec::new();
        while let Some((k, _v)) = iter.next().unwrap() {
            keys.push(k);
        }
        assert_eq!(
            keys,
            vec![
                Key::Int(1),
                Key::Int(2),
                Key::Int(3),
                Key::Int(4),
                Key::Int(5)
            ]
        );
    }

    #[test]
    fn merge_dedup_priority() {
        // Two sources with same keys — higher priority wins.
        let old = VecSource::new(vec![
            (Key::Int(1), Value::from("old")),
            (Key::Int(2), Value::from("old2")),
        ]);
        let new = VecSource::new(vec![
            (Key::Int(1), Value::from("new")),
            (Key::Int(3), Value::from("new3")),
        ]);

        let mut iter = MergeIterator::new(vec![
            (Box::new(old), 0), // low priority
            (Box::new(new), 1), // high priority
        ]);

        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(k1, Key::Int(1));
        assert_eq!(v1, Value::from("new")); // high priority wins

        let (k2, v2) = iter.next().unwrap().unwrap();
        assert_eq!(k2, Key::Int(2));
        assert_eq!(v2, Value::from("old2"));

        let (k3, _v3) = iter.next().unwrap().unwrap();
        assert_eq!(k3, Key::Int(3));

        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn merge_empty_sources() {
        let mut iter = MergeIterator::new(vec![
            (Box::new(VecSource::new(vec![])), 0),
            (Box::new(VecSource::new(vec![])), 1),
        ]);
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn merge_single_source() {
        let s = VecSource::new(vec![
            (Key::Int(10), Value::from("x")),
            (Key::Int(20), Value::from("y")),
        ]);
        let mut iter = MergeIterator::new(vec![(Box::new(s), 0)]);
        assert!(iter.next().unwrap().is_some());
        assert!(iter.next().unwrap().is_some());
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn merge_tombstone_shadows() {
        // SSTable has a live value, memtable has a tombstone for the same key
        let sst = VecSource::new(vec![(Key::Int(1), Value::from("live"))]);
        let mem = VecSource::new(vec![(Key::Int(1), Value::tombstone())]);

        let mut iter = MergeIterator::new(vec![
            (Box::new(sst), 0), // low priority
            (Box::new(mem), 1), // high priority (memtable)
        ]);

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, Key::Int(1));
        assert!(v.is_tombstone()); // tombstone wins
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn merge_three_sources_overlap() {
        let s1 = VecSource::new(vec![
            (Key::Int(1), Value::from("s1")),
            (Key::Int(2), Value::from("s1")),
        ]);
        let s2 = VecSource::new(vec![
            (Key::Int(1), Value::from("s2")),
            (Key::Int(3), Value::from("s2")),
        ]);
        let s3 = VecSource::new(vec![
            (Key::Int(1), Value::from("s3")),
            (Key::Int(2), Value::from("s3")),
            (Key::Int(4), Value::from("s3")),
        ]);

        let mut iter = MergeIterator::new(vec![
            (Box::new(s1), 0),
            (Box::new(s2), 1),
            (Box::new(s3), 2), // highest priority
        ]);

        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(k1, Key::Int(1));
        assert_eq!(v1, Value::from("s3")); // s3 has highest priority

        let (k2, v2) = iter.next().unwrap().unwrap();
        assert_eq!(k2, Key::Int(2));
        assert_eq!(v2, Value::from("s3")); // s3 has highest priority

        let (k3, _) = iter.next().unwrap().unwrap();
        assert_eq!(k3, Key::Int(3));

        let (k4, _) = iter.next().unwrap().unwrap();
        assert_eq!(k4, Key::Int(4));

        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn rscan_adapter_reverses() {
        let s = VecSource::new(vec![
            (Key::Int(1), Value::from("a")),
            (Key::Int(2), Value::from("b")),
            (Key::Int(3), Value::from("c")),
        ]);
        let iter = MergeIterator::new(vec![(Box::new(s), 0)]);
        let mut rscan = RScanAdapter::from_merge_iter(iter).unwrap();

        let (k1, _) = rscan.next().unwrap();
        assert_eq!(k1, Key::Int(3));
        let (k2, _) = rscan.next().unwrap();
        assert_eq!(k2, Key::Int(2));
        let (k3, _) = rscan.next().unwrap();
        assert_eq!(k3, Key::Int(1));
        assert!(rscan.next().is_none());
    }
}
