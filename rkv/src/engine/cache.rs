use std::collections::HashMap;

/// Cache key: (SSTable ID, block index within the SSTable).
type CacheKey = (u64, u32);

/// Raw entry parsed from a data block: (key_bytes, value_tag, value_data).
pub(crate) type RawEntry = (Vec<u8>, u8, Vec<u8>);

/// Node in the slab-backed doubly-linked LRU list.
struct LruNode {
    key: CacheKey,
    entries: Vec<RawEntry>,
    size: usize,
    prev: usize,
    next: usize,
}

/// Sentinel index used for head/tail when the list is empty.
const SENTINEL: usize = usize::MAX;

/// LRU block cache for decompressed SSTable blocks.
///
/// Stores parsed block entries keyed by `(sst_id, block_index)`. Uses a
/// slab-allocated doubly-linked list for O(1) promotion and eviction, plus
/// a `HashMap` for O(1) lookups.
///
/// When `capacity` is 0, all operations are no-ops (cache disabled).
pub(crate) struct BlockCache {
    map: HashMap<CacheKey, usize>,
    nodes: Vec<LruNode>,
    free: Vec<usize>,
    head: usize, // MRU end
    tail: usize, // LRU end (evict from here)
    current_size: usize,
    capacity: usize,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    /// Create a new block cache with the given byte capacity.
    ///
    /// A capacity of 0 disables caching entirely.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: SENTINEL,
            tail: SENTINEL,
            current_size: 0,
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    /// Look up a cached block and promote it to MRU position.
    ///
    /// Returns a clone of the cached entries, or `None` on miss.
    pub(crate) fn get(&mut self, sst_id: u64, block_index: u32) -> Option<Vec<RawEntry>> {
        if self.capacity == 0 {
            return None;
        }

        if let Some(&idx) = self.map.get(&(sst_id, block_index)) {
            self.hits += 1;
            self.detach(idx);
            self.push_front(idx);
            Some(self.nodes[idx].entries.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    #[allow(dead_code)]
    pub(crate) fn hits(&self) -> u64 {
        self.hits
    }

    #[allow(dead_code)]
    pub(crate) fn misses(&self) -> u64 {
        self.misses
    }

    /// Insert a block into the cache, evicting LRU entries if needed.
    ///
    /// Blocks larger than the total capacity are silently skipped to
    /// prevent thrashing.
    pub(crate) fn insert(
        &mut self,
        sst_id: u64,
        block_index: u32,
        entries: Vec<RawEntry>,
        size: usize,
    ) {
        if self.capacity == 0 || size > self.capacity {
            return;
        }

        let key = (sst_id, block_index);

        // If already present, update in place and promote
        if let Some(&idx) = self.map.get(&key) {
            self.current_size -= self.nodes[idx].size;
            self.nodes[idx].entries = entries;
            self.nodes[idx].size = size;
            self.current_size += size;
            self.detach(idx);
            self.push_front(idx);
            self.evict_to_capacity();
            return;
        }

        // Evict until there's room
        while self.current_size + size > self.capacity && self.tail != SENTINEL {
            self.evict_tail();
        }

        // Allocate a node (reuse free slot or push new)
        let node = LruNode {
            key,
            entries,
            size,
            prev: SENTINEL,
            next: SENTINEL,
        };

        let idx = if let Some(free_idx) = self.free.pop() {
            self.nodes[free_idx] = node;
            free_idx
        } else {
            self.nodes.push(node);
            self.nodes.len() - 1
        };

        self.map.insert(key, idx);
        self.current_size += size;
        self.push_front(idx);
    }

    /// Remove all cached blocks for a given SSTable.
    ///
    /// Used during compaction to invalidate stale SSTable data.
    pub(crate) fn evict_sst(&mut self, sst_id: u64) {
        let keys_to_remove: Vec<CacheKey> =
            self.map.keys().filter(|k| k.0 == sst_id).copied().collect();

        for key in keys_to_remove {
            if let Some(idx) = self.map.remove(&key) {
                self.current_size -= self.nodes[idx].size;
                self.detach(idx);
                self.free.push(idx);
            }
        }
    }

    /// Number of cached blocks.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the cache is empty.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    // --- Linked list operations ---

    /// Remove a node from wherever it sits in the list.
    fn detach(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if prev != SENTINEL {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }

        if next != SENTINEL {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }

        self.nodes[idx].prev = SENTINEL;
        self.nodes[idx].next = SENTINEL;
    }

    /// Insert a node at the head (MRU end).
    fn push_front(&mut self, idx: usize) {
        self.nodes[idx].prev = SENTINEL;
        self.nodes[idx].next = self.head;

        if self.head != SENTINEL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;

        if self.tail == SENTINEL {
            self.tail = idx;
        }
    }

    /// Evict the tail (LRU) node.
    fn evict_tail(&mut self) {
        if self.tail == SENTINEL {
            return;
        }
        let idx = self.tail;
        let key = self.nodes[idx].key;
        self.current_size -= self.nodes[idx].size;
        self.map.remove(&key);
        self.detach(idx);
        self.free.push(idx);
    }

    /// Keep evicting until current_size <= capacity.
    fn evict_to_capacity(&mut self) {
        while self.current_size > self.capacity && self.tail != SENTINEL {
            self.evict_tail();
        }
    }
}

/// Estimate the in-memory size of a block's raw entries.
///
/// Per entry: key_bytes.len() + 1 (tag) + value_data.len() + 48 (Vec overhead).
/// Plus 64 bytes for the Vec<RawEntry> itself and node overhead.
pub(crate) fn estimate_block_size(entries: &[RawEntry]) -> usize {
    let mut size = 64; // Vec + node overhead
    for (key_bytes, _, value_data) in entries {
        size += key_bytes.len() + 1 + value_data.len() + 48;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(n: usize) -> Vec<RawEntry> {
        (0..n)
            .map(|i| (vec![i as u8], 0x00, vec![i as u8; 10]))
            .collect()
    }

    #[test]
    fn new_cache_is_empty() {
        let cache = BlockCache::new(1024);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn disabled_cache_skips_all() {
        let mut cache = BlockCache::new(0);
        let entries = make_entries(1);
        cache.insert(1, 0, entries.clone(), 100);
        assert!(cache.is_empty());
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn insert_and_get() {
        let mut cache = BlockCache::new(4096);
        let entries = make_entries(3);
        let size = estimate_block_size(&entries);
        cache.insert(1, 0, entries.clone(), size);

        assert_eq!(cache.len(), 1);
        let got = cache.get(1, 0).unwrap();
        assert_eq!(got, entries);
    }

    #[test]
    fn miss_returns_none() {
        let mut cache = BlockCache::new(4096);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn evicts_lru_when_full() {
        let mut cache = BlockCache::new(200);
        let e1 = make_entries(1);
        let e2 = make_entries(1);
        let e3 = make_entries(1);

        // Each entry ~60 bytes (64 overhead + 1 + 1 + 10 + 48 = 124),
        // use explicit sizes for predictability.
        cache.insert(1, 0, e1, 80);
        cache.insert(2, 0, e2, 80);
        // Both fit (160 <= 200)
        assert_eq!(cache.len(), 2);

        cache.insert(3, 0, e3, 80);
        // Evicts LRU (sst 1, block 0) to fit (240 > 200)
        assert_eq!(cache.len(), 2);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(2, 0).is_some());
        assert!(cache.get(3, 0).is_some());
    }

    #[test]
    fn get_promotes_to_mru() {
        let mut cache = BlockCache::new(200);
        let e1 = make_entries(1);
        let e2 = make_entries(1);
        let e3 = make_entries(1);

        cache.insert(1, 0, e1, 80);
        cache.insert(2, 0, e2, 80);

        // Access sst 1 to promote it
        cache.get(1, 0);

        // Insert sst 3, should evict sst 2 (now LRU)
        cache.insert(3, 0, e3, 80);
        assert!(cache.get(2, 0).is_none());
        assert!(cache.get(1, 0).is_some());
        assert!(cache.get(3, 0).is_some());
    }

    #[test]
    fn evict_sst_removes_all_blocks() {
        let mut cache = BlockCache::new(4096);
        cache.insert(1, 0, make_entries(1), 100);
        cache.insert(1, 1, make_entries(1), 100);
        cache.insert(1, 2, make_entries(1), 100);
        cache.insert(2, 0, make_entries(1), 100);

        assert_eq!(cache.len(), 4);
        cache.evict_sst(1);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 1).is_none());
        assert!(cache.get(1, 2).is_none());
        assert!(cache.get(2, 0).is_some());
    }

    #[test]
    fn skip_oversized_block() {
        let mut cache = BlockCache::new(100);
        let entries = make_entries(1);
        cache.insert(1, 0, entries, 200); // larger than capacity
        assert!(cache.is_empty());
    }

    #[test]
    fn update_existing_entry() {
        let mut cache = BlockCache::new(4096);
        let e1 = make_entries(2);
        let e2 = make_entries(3);

        cache.insert(1, 0, e1, 100);
        cache.insert(1, 0, e2.clone(), 150);

        assert_eq!(cache.len(), 1);
        let got = cache.get(1, 0).unwrap();
        assert_eq!(got, e2);
    }

    #[test]
    fn estimate_block_size_works() {
        let entries = make_entries(2);
        let size = estimate_block_size(&entries);
        // 64 + 2 * (1 + 1 + 10 + 48) = 64 + 120 = 184
        assert_eq!(size, 184);
    }

    #[test]
    fn hit_miss_counters() {
        let mut cache = BlockCache::new(4096);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 0);

        // Miss on empty cache
        cache.get(1, 0);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 1);

        // Insert and hit
        cache.insert(1, 0, make_entries(1), 100);
        cache.get(1, 0);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);

        // Miss on different key
        cache.get(2, 0);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 2);

        // Another hit
        cache.get(1, 0);
        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 2);
    }

    #[test]
    fn disabled_cache_no_hit_miss_tracking() {
        let mut cache = BlockCache::new(0);
        cache.get(1, 0);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 0);
    }
}
