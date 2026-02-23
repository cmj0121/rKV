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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Construction ---

    #[test]
    fn new_default_bits_per_key() {
        let bf = BloomFilter::new(10);
        assert_eq!(bf.bits_per_key(), 10);
        assert!(bf.is_empty());
        assert_eq!(bf.len(), 0);
    }

    #[test]
    fn new_custom_bits_per_key() {
        let bf = BloomFilter::new(20);
        assert_eq!(bf.bits_per_key(), 20);
    }

    #[test]
    fn new_zero_bits_per_key() {
        let bf = BloomFilter::new(0);
        assert_eq!(bf.bits_per_key(), 0);
        assert!(bf.is_empty());
    }

    // --- Insert / query ---

    #[test]
    fn may_contain_always_true() {
        let bf = BloomFilter::new(10);
        assert!(bf.may_contain(b"anything"));
        assert!(bf.may_contain(b""));
        assert!(bf.may_contain(b"nonexistent"));
    }

    #[test]
    fn insert_increments_count() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key1");
        assert_eq!(bf.len(), 1);
        bf.insert(b"key2");
        assert_eq!(bf.len(), 2);
    }

    #[test]
    fn may_contain_after_insert() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"hello");
        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world")); // stub: always true
    }

    // --- len / is_empty ---

    #[test]
    fn len_empty() {
        let bf = BloomFilter::new(10);
        assert_eq!(bf.len(), 0);
        assert!(bf.is_empty());
    }

    #[test]
    fn len_after_inserts() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"a");
        bf.insert(b"b");
        bf.insert(b"c");
        assert_eq!(bf.len(), 3);
        assert!(!bf.is_empty());
    }

    // --- clear ---

    #[test]
    fn clear_resets_to_empty() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key1");
        bf.insert(b"key2");
        assert_eq!(bf.len(), 2);

        bf.clear();
        assert_eq!(bf.len(), 0);
        assert!(bf.is_empty());
    }

    // --- bits_per_key ---

    #[test]
    fn bits_per_key_returns_configured_value() {
        assert_eq!(BloomFilter::new(10).bits_per_key(), 10);
        assert_eq!(BloomFilter::new(15).bits_per_key(), 15);
        assert_eq!(BloomFilter::new(0).bits_per_key(), 0);
    }

    // --- estimated_fpr ---

    #[test]
    fn estimated_fpr_returns_one() {
        let bf = BloomFilter::new(10);
        assert!((bf.estimated_fpr() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn estimated_fpr_after_insert() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key");
        assert!((bf.estimated_fpr() - 1.0).abs() < f64::EPSILON);
    }

    // --- Serialization stubs ---

    #[test]
    fn to_bytes_returns_empty_vec() {
        let bf = BloomFilter::new(10);
        assert!(bf.to_bytes().is_empty());
    }

    #[test]
    fn from_bytes_returns_not_implemented() {
        let result = BloomFilter::from_bytes(&[1, 2, 3]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::NotImplemented(ref msg) if msg.contains("BloomFilter")),
            "expected NotImplemented, got: {err}",
        );
    }

    // --- Clone ---

    #[test]
    fn clone_preserves_state() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key1");
        bf.insert(b"key2");

        let cloned = bf.clone();
        assert_eq!(cloned.len(), bf.len());
        assert_eq!(cloned.bits_per_key(), bf.bits_per_key());
    }

    // --- Debug ---

    #[test]
    fn debug_format() {
        let bf = BloomFilter::new(10);
        let debug = format!("{bf:?}");
        assert!(debug.contains("BloomFilter"));
    }
}
