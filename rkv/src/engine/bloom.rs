use super::error::{Error, Result};

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

#[allow(dead_code)]
impl BloomFilter {
    /// Create a new Bloom filter with the given bits-per-key setting.
    ///
    /// The `bits_per_key` parameter controls the space–accuracy trade-off:
    /// 10 bits/key ≈ 1% false-positive rate at standard hash count.
    /// Set to 0 to effectively disable the filter.
    pub(crate) fn new(bits_per_key: usize) -> Self {
        // Optimal number of hash functions: k = ln(2) * bits_per_key ≈ 0.69 * bits_per_key
        // Clamped to [1, 30] per LevelDB/RocksDB convention.
        let num_hashes = if bits_per_key == 0 {
            1
        } else {
            let k = (bits_per_key as f64 * std::f64::consts::LN_2) as u32;
            k.clamp(1, 30)
        };

        Self {
            bits: Vec::new(),
            num_hashes,
            num_keys: 0,
            bits_per_key,
        }
    }

    /// Insert a key into the filter.
    ///
    /// Stub: increments the key count only.
    pub(crate) fn insert(&mut self, _key: &[u8]) {
        self.num_keys += 1;
    }

    /// Test whether the filter may contain the given key.
    ///
    /// Stub: always returns `true` (safe — no false negatives).
    pub(crate) fn may_contain(&self, _key: &[u8]) -> bool {
        true
    }

    /// Reset the filter to empty.
    pub(crate) fn clear(&mut self) {
        self.bits.clear();
        self.num_keys = 0;
    }

    /// Number of keys inserted.
    pub(crate) fn len(&self) -> usize {
        self.num_keys as usize
    }

    /// Returns `true` if no keys have been inserted.
    pub(crate) fn is_empty(&self) -> bool {
        self.num_keys == 0
    }

    /// Returns the configured bits-per-key.
    pub(crate) fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    /// Estimated false-positive rate.
    ///
    /// Stub: returns `1.0` (worst case — every query is a "maybe").
    pub(crate) fn estimated_fpr(&self) -> f64 {
        1.0
    }

    /// Serialize the filter to bytes.
    ///
    /// Stub: returns an empty vec.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Deserialize a filter from bytes.
    ///
    /// Stub: returns `NotImplemented`.
    pub(crate) fn from_bytes(_data: &[u8]) -> Result<Self> {
        Err(Error::NotImplemented("BloomFilter::from_bytes".into()))
    }
}
