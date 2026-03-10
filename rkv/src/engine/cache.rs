use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Cache key: (SSTable ID, block index within the SSTable).
type CacheKey = (u64, u32);

/// Node in the slab-backed doubly-linked LRU list.
struct LruNode {
    key: CacheKey,
    data: Arc<Vec<u8>>,
    size: usize,
    prev: usize,
    next: usize,
}

/// Sentinel index used for head/tail when the list is empty.
const SENTINEL: usize = usize::MAX;

/// LRU block cache for decompressed SSTable blocks.
///
/// Stores raw decompressed block bytes keyed by `(sst_id, block_index)`.
/// Uses a slab-allocated doubly-linked list for O(1) promotion and eviction,
/// plus a `HashMap` for O(1) lookups. Callers search the cached bytes
/// directly via restart-point binary search — no per-entry allocation.
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
    /// Returns a shared reference to the cached decompressed bytes, or `None` on miss.
    pub(crate) fn get(&mut self, sst_id: u64, block_index: u32) -> Option<Arc<Vec<u8>>> {
        if self.capacity == 0 {
            return None;
        }

        if let Some(&idx) = self.map.get(&(sst_id, block_index)) {
            self.hits += 1;
            self.detach(idx);
            self.push_front(idx);
            Some(Arc::clone(&self.nodes[idx].data))
        } else {
            self.misses += 1;
            None
        }
    }

    pub(crate) fn hits(&self) -> u64 {
        self.hits
    }

    pub(crate) fn misses(&self) -> u64 {
        self.misses
    }

    /// Insert a block into the cache, evicting LRU entries if needed.
    ///
    /// Convenience wrapper that wraps data in `Arc`.
    #[cfg(test)]
    pub(crate) fn insert(&mut self, sst_id: u64, block_index: u32, data: Vec<u8>, size: usize) {
        self.insert_arc(sst_id, block_index, Arc::new(data), size);
    }

    /// Insert a pre-wrapped `Arc` block into the cache, evicting LRU
    /// entries if needed.
    ///
    /// Blocks larger than the total capacity are silently skipped to
    /// prevent thrashing.
    pub(crate) fn insert_arc(
        &mut self,
        sst_id: u64,
        block_index: u32,
        data: Arc<Vec<u8>>,
        size: usize,
    ) {
        if self.capacity == 0 || size > self.capacity {
            return;
        }

        let key = (sst_id, block_index);

        // If already present, update in place and promote
        if let Some(&idx) = self.map.get(&key) {
            self.current_size -= self.nodes[idx].size;
            self.nodes[idx].data = Arc::clone(&data);
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
            data,
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
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the cache is empty.
    #[cfg(test)]
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

/// Number of shards for the sharded block cache.
///
/// 16 shards provides good concurrency (up to 16 readers hitting
/// different shards simultaneously) without excessive overhead.
const NUM_SHARDS: usize = 16;

/// Sharded block cache that distributes entries across multiple
/// independent `BlockCache` shards, each with its own `Mutex`.
///
/// This reduces mutex contention under concurrent reads: two readers
/// hitting different shards never block each other.
pub(crate) struct ShardedBlockCache {
    shards: Vec<Mutex<BlockCache>>,
}

impl ShardedBlockCache {
    /// Create a new sharded cache. Total capacity is distributed
    /// evenly across `NUM_SHARDS` shards.
    pub(crate) fn new(total_capacity: usize) -> Self {
        let per_shard = if total_capacity == 0 {
            0
        } else {
            total_capacity.div_ceil(NUM_SHARDS)
        };
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(BlockCache::new(per_shard)))
            .collect();
        Self { shards }
    }

    /// Hash a cache key to a shard index.
    fn shard_index(sst_id: u64, block_index: u32) -> usize {
        // FxHash-style: multiply by a large odd constant, take high bits
        let h = (sst_id as usize)
            .wrapping_mul(0x517cc1b727220a95)
            .wrapping_add(block_index as usize);
        h % NUM_SHARDS
    }

    /// Look up a cached block and promote it to MRU position within
    /// its shard.
    pub(crate) fn get(&self, sst_id: u64, block_index: u32) -> Option<Arc<Vec<u8>>> {
        let idx = Self::shard_index(sst_id, block_index);
        let mut shard = self.shards[idx].lock().unwrap_or_else(|e| e.into_inner());
        shard.get(sst_id, block_index)
    }

    /// Insert a pre-wrapped `Arc` block into the appropriate shard.
    pub(crate) fn insert_arc(
        &self,
        sst_id: u64,
        block_index: u32,
        data: Arc<Vec<u8>>,
        size: usize,
    ) {
        let idx = Self::shard_index(sst_id, block_index);
        let mut shard = self.shards[idx].lock().unwrap_or_else(|e| e.into_inner());
        shard.insert_arc(sst_id, block_index, data, size);
    }

    /// Remove all cached blocks for a given SSTable across all shards.
    pub(crate) fn evict_sst(&self, sst_id: u64) {
        for shard in &self.shards {
            let mut s = shard.lock().unwrap_or_else(|e| e.into_inner());
            s.evict_sst(sst_id);
        }
    }

    /// Total cache hits across all shards.
    pub(crate) fn hits(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).hits())
            .sum()
    }

    /// Total cache misses across all shards.
    pub(crate) fn misses(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).misses())
            .sum()
    }
}

/// Estimate the in-memory size of decompressed block bytes.
///
/// The cached data is a `Vec<u8>` of the raw decompressed block.
/// Add 64 bytes for Arc/Vec/node overhead.
pub(crate) fn estimate_block_size(data: &[u8]) -> usize {
    data.len() + 64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block_data(n: usize) -> Vec<u8> {
        vec![0u8; 64 * n]
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
        let data = make_block_data(1);
        cache.insert(1, 0, data, 100);
        assert!(cache.is_empty());
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn insert_and_get() {
        let mut cache = BlockCache::new(4096);
        let data = make_block_data(3);
        let size = estimate_block_size(&data);
        cache.insert(1, 0, data.clone(), size);

        assert_eq!(cache.len(), 1);
        let got = cache.get(1, 0).unwrap();
        assert_eq!(*got, data);
    }

    #[test]
    fn miss_returns_none() {
        let mut cache = BlockCache::new(4096);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn evicts_lru_when_full() {
        let mut cache = BlockCache::new(200);

        cache.insert(1, 0, make_block_data(1), 80);
        cache.insert(2, 0, make_block_data(1), 80);
        // Both fit (160 <= 200)
        assert_eq!(cache.len(), 2);

        cache.insert(3, 0, make_block_data(1), 80);
        // Evicts LRU (sst 1, block 0) to fit (240 > 200)
        assert_eq!(cache.len(), 2);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(2, 0).is_some());
        assert!(cache.get(3, 0).is_some());
    }

    #[test]
    fn get_promotes_to_mru() {
        let mut cache = BlockCache::new(200);

        cache.insert(1, 0, make_block_data(1), 80);
        cache.insert(2, 0, make_block_data(1), 80);

        // Access sst 1 to promote it
        cache.get(1, 0);

        // Insert sst 3, should evict sst 2 (now LRU)
        cache.insert(3, 0, make_block_data(1), 80);
        assert!(cache.get(2, 0).is_none());
        assert!(cache.get(1, 0).is_some());
        assert!(cache.get(3, 0).is_some());
    }

    #[test]
    fn evict_sst_removes_all_blocks() {
        let mut cache = BlockCache::new(4096);
        cache.insert(1, 0, make_block_data(1), 100);
        cache.insert(1, 1, make_block_data(1), 100);
        cache.insert(1, 2, make_block_data(1), 100);
        cache.insert(2, 0, make_block_data(1), 100);

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
        cache.insert(1, 0, make_block_data(1), 200); // larger than capacity
        assert!(cache.is_empty());
    }

    #[test]
    fn update_existing_entry() {
        let mut cache = BlockCache::new(4096);
        let d1 = make_block_data(2);
        let d2 = make_block_data(3);

        cache.insert(1, 0, d1, 100);
        cache.insert(1, 0, d2.clone(), 150);

        assert_eq!(cache.len(), 1);
        let got = cache.get(1, 0).unwrap();
        assert_eq!(*got, d2);
    }

    #[test]
    fn estimate_block_size_works() {
        let data = make_block_data(2);
        let size = estimate_block_size(&data);
        // 128 bytes of data + 64 overhead = 192
        assert_eq!(size, 192);
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
        cache.insert(1, 0, make_block_data(1), 100);
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

    // --- ShardedBlockCache ---

    #[test]
    fn sharded_insert_and_get() {
        let cache = ShardedBlockCache::new(65536);
        let data = Arc::new(make_block_data(3));
        let size = estimate_block_size(&data);
        cache.insert_arc(1, 0, Arc::clone(&data), size);

        let got = cache.get(1, 0).unwrap();
        assert_eq!(*got, *data);
    }

    #[test]
    fn sharded_miss_returns_none() {
        let cache = ShardedBlockCache::new(65536);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn sharded_evict_sst_removes_across_shards() {
        let cache = ShardedBlockCache::new(1_000_000);
        for bi in 0..32 {
            cache.insert_arc(1, bi, Arc::new(make_block_data(1)), 100);
        }
        cache.insert_arc(2, 0, Arc::new(make_block_data(1)), 100);

        cache.evict_sst(1);

        for bi in 0..32 {
            assert!(
                cache.get(1, bi).is_none(),
                "sst 1 block {bi} should be evicted"
            );
        }
        assert!(cache.get(2, 0).is_some());
    }

    #[test]
    fn sharded_hit_miss_aggregation() {
        let cache = ShardedBlockCache::new(65536);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 0);

        // Miss
        cache.get(1, 0);
        assert_eq!(cache.misses(), 1);

        // Insert and hit
        cache.insert_arc(1, 0, Arc::new(make_block_data(1)), 100);
        cache.get(1, 0);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn sharded_zero_capacity_disabled() {
        let cache = ShardedBlockCache::new(0);
        cache.insert_arc(1, 0, Arc::new(make_block_data(1)), 100);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn sharded_distributes_across_shards() {
        let mut seen = std::collections::HashSet::new();
        for sst_id in 0..8u64 {
            for bi in 0..4u32 {
                seen.insert(ShardedBlockCache::shard_index(sst_id, bi));
            }
        }
        assert!(
            seen.len() >= 8,
            "expected >=8 shards used, got {}",
            seen.len()
        );
    }
}
