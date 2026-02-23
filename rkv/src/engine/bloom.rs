/// Per-SSTable Bloom filter for probabilistic key membership testing.
///
/// A Bloom filter allows skipping SSTable levels that definitely do not
/// contain a key, avoiding unnecessary disk I/O on read misses.
///
/// **Stub implementation**: `may_contain()` always returns `true` (safe —
/// no false negatives, just no optimization). Actual bit-array logic will
/// be added when the persistence layer lands.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct BloomFilter {
    /// Bit array (empty in stub).
    bits: Vec<u8>,
    /// Number of hash functions (computed from bits_per_key).
    num_hashes: u32,
    /// Number of keys inserted.
    num_keys: u32,
    /// Configured bits per key.
    bits_per_key: usize,
}
