use super::error::Result;

/// LevelDB-compatible hash function for bloom filter probe generation.
fn bloom_hash(data: &[u8]) -> u32 {
    let m: u32 = 0xc6a4_a793;
    let mut h: u32 = 0xbc9f_1d34_u32 ^ (data.len() as u32).wrapping_mul(m);

    let mut i = 0;
    while i + 4 <= data.len() {
        // SAFETY: i + 4 <= data.len() ensured by while condition
        h = h.wrapping_add(u32::from_le_bytes([
            data[i],
            data[i + 1],
            data[i + 2],
            data[i + 3],
        ]));
        h = h.wrapping_mul(m);
        h ^= h >> 16;
        i += 4;
    }

    let remaining = data.len() - i;
    if remaining >= 3 {
        h = h.wrapping_add((data[i + 2] as u32) << 16);
    }
    if remaining >= 2 {
        h = h.wrapping_add((data[i + 1] as u32) << 8);
    }
    if remaining >= 1 {
        h = h.wrapping_add(data[i] as u32);
        h = h.wrapping_mul(m);
        h ^= h >> 24;
    }
    h
}

/// Per-SSTable Bloom filter for probabilistic key membership testing.
///
/// A Bloom filter allows skipping SSTables that definitely do not contain
/// a key, avoiding unnecessary disk I/O on read misses.
///
/// **Build phase**: call `insert()` for each key, then `to_bytes()` to
/// serialize the filter. **Query phase**: call `from_bytes()` to
/// deserialize, then `may_contain()` to test membership.
#[derive(Clone, Debug)]
pub(crate) struct BloomFilter {
    /// Bit array (populated after `build()` or `from_bytes()`).
    bits: Vec<u8>,
    /// Number of hash probes per key.
    num_hashes: u32,
    /// Number of keys inserted.
    num_keys: u32,
    /// Configured bits per key.
    bits_per_key: usize,
    /// Collected key hashes during build phase.
    key_hashes: Vec<u32>,
}

impl BloomFilter {
    /// Create a new Bloom filter with the given bits-per-key setting.
    ///
    /// The `bits_per_key` parameter controls the space-accuracy trade-off:
    /// 10 bits/key ~ 1% false-positive rate. Set to 0 to disable.
    pub(crate) fn new(bits_per_key: usize) -> Self {
        // Optimal k = ln(2) * bits_per_key ~ 0.69 * bits_per_key
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
            key_hashes: Vec::new(),
        }
    }

    /// Insert a key into the filter (build phase).
    ///
    /// Collects the key hash for later bit-array construction in `to_bytes()`.
    pub(crate) fn insert(&mut self, key: &[u8]) {
        self.key_hashes.push(bloom_hash(key));
        self.num_keys += 1;
    }

    /// Test whether the filter may contain the given key.
    ///
    /// Returns `true` if the key might be present (possible false positive),
    /// `false` if the key is definitely absent (no false negatives).
    /// Returns `true` if the bit array is empty (no filter available).
    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true; // no filter built yet — conservative
        }
        let num_bits = (self.bits.len() as u32) * 8;
        let h = bloom_hash(key);
        let delta = h.rotate_left(15);
        let mut current = h;

        // Use bitmask instead of modulo when num_bits is a power of two.
        if num_bits.is_power_of_two() {
            let mask = num_bits - 1;
            for _ in 0..self.num_hashes {
                let bit_pos = current & mask;
                if self.bits[(bit_pos >> 3) as usize] & (1 << (bit_pos & 7)) == 0 {
                    return false;
                }
                current = current.wrapping_add(delta);
            }
        } else {
            for _ in 0..self.num_hashes {
                let bit_pos = current % num_bits;
                if self.bits[(bit_pos / 8) as usize] & (1 << (bit_pos % 8)) == 0 {
                    return false;
                }
                current = current.wrapping_add(delta);
            }
        }
        true
    }

    /// Reset the filter to empty.
    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.bits.clear();
        self.key_hashes.clear();
        self.num_keys = 0;
    }

    /// Number of keys inserted.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.num_keys as usize
    }

    /// Returns `true` if no keys have been inserted.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.num_keys == 0
    }

    /// Returns the configured bits-per-key.
    #[cfg(test)]
    pub(crate) fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    /// Estimated false-positive rate based on filter parameters.
    ///
    /// Uses the formula: FPR ~ (1 - e^(-k*n/m))^k
    #[cfg(test)]
    pub(crate) fn estimated_fpr(&self) -> f64 {
        if self.bits.is_empty() || self.num_keys == 0 {
            return 1.0;
        }
        let m = (self.bits.len() as f64) * 8.0;
        let n = self.num_keys as f64;
        let k = self.num_hashes as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Build the bit array and serialize the filter to bytes.
    ///
    /// Builds from collected key hashes, then returns
    /// `[num_hashes: u8][bit_array...]`. Returns empty vec if no keys.
    ///
    /// The bit array size is rounded up to a power of two so that
    /// `may_contain` can use bitmask operations instead of modulo.
    pub(crate) fn build(&mut self) -> Vec<u8> {
        if self.key_hashes.is_empty() || self.bits_per_key == 0 {
            return Vec::new();
        }

        // Compute bit array size (minimum 64 bits), rounded to power of two
        let raw_bits = std::cmp::max(self.key_hashes.len() * self.bits_per_key, 64);
        let num_bits = (raw_bits as u32).next_power_of_two();
        let num_bytes = (num_bits / 8) as usize;

        self.bits = vec![0u8; num_bytes];

        let mask = num_bits - 1;
        for &h in &self.key_hashes {
            let delta = h.rotate_left(15);
            let mut current = h;
            for _ in 0..self.num_hashes {
                let bit_pos = current & mask;
                self.bits[(bit_pos >> 3) as usize] |= 1 << (bit_pos & 7);
                current = current.wrapping_add(delta);
            }
        }

        let mut out = Vec::with_capacity(1 + self.bits.len());
        out.push(self.num_hashes as u8);
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a filter from bytes.
    ///
    /// Expects `[num_hashes: u8][bit_array...]` as produced by `to_bytes()`.
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            // Empty data means no filter — return a pass-through filter
            return Ok(Self::new(0));
        }
        let num_hashes = data[0] as u32;
        let bits = data[1..].to_vec();
        Ok(Self {
            bits,
            num_hashes,
            num_keys: 0,
            bits_per_key: 0,
            key_hashes: Vec::new(),
        })
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
    fn inserted_keys_always_found() {
        let mut bf = BloomFilter::new(10);
        for i in 0..100 {
            bf.insert(format!("key{i}").as_bytes());
        }
        let _ = bf.build(); // build the bit array

        // No false negatives: every inserted key must be found
        for i in 0..100 {
            assert!(
                bf.may_contain(format!("key{i}").as_bytes()),
                "false negative for key{i}"
            );
        }
    }

    #[test]
    fn non_inserted_keys_mostly_rejected() {
        let mut bf = BloomFilter::new(10);
        for i in 0..1000 {
            bf.insert(format!("key{i}").as_bytes());
        }
        let _ = bf.build();

        // Check false positive rate on 10000 non-inserted keys
        let mut false_positives = 0;
        for i in 1000..11000 {
            if bf.may_contain(format!("key{i}").as_bytes()) {
                false_positives += 1;
            }
        }
        // At 10 bits/key, expected FPR ~ 1%. Allow up to 3%.
        let fpr = false_positives as f64 / 10000.0;
        assert!(
            fpr < 0.03,
            "false positive rate too high: {fpr:.4} ({false_positives}/10000)"
        );
    }

    #[test]
    fn may_contain_returns_true_when_no_filter_built() {
        let bf = BloomFilter::new(10);
        assert!(bf.may_contain(b"anything"));
    }

    #[test]
    fn insert_increments_count() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key1");
        assert_eq!(bf.len(), 1);
        bf.insert(b"key2");
        assert_eq!(bf.len(), 2);
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
        let _ = bf.build();
        assert_eq!(bf.len(), 2);

        bf.clear();
        assert_eq!(bf.len(), 0);
        assert!(bf.is_empty());
        assert!(bf.bits.is_empty());
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
    fn estimated_fpr_returns_one_before_build() {
        let bf = BloomFilter::new(10);
        assert!((bf.estimated_fpr() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn estimated_fpr_reasonable_after_build() {
        let mut bf = BloomFilter::new(10);
        for i in 0..100 {
            bf.insert(format!("key{i}").as_bytes());
        }
        let _ = bf.build();
        let fpr = bf.estimated_fpr();
        // At 10 bits/key, theoretical FPR ~ 0.8%
        assert!(fpr < 0.02, "estimated FPR too high: {fpr}");
        assert!(fpr > 0.0, "estimated FPR should be > 0");
    }

    // --- Serialization ---

    #[test]
    fn to_bytes_empty_filter() {
        let mut bf = BloomFilter::new(10);
        assert!(bf.build().is_empty());
    }

    #[test]
    fn to_bytes_disabled_filter() {
        let mut bf = BloomFilter::new(0);
        bf.insert(b"key");
        assert!(bf.build().is_empty());
    }

    #[test]
    fn roundtrip_serialization() {
        let mut bf = BloomFilter::new(10);
        for i in 0..50 {
            bf.insert(format!("key{i}").as_bytes());
        }
        let data = bf.build();
        assert!(!data.is_empty());

        let bf2 = BloomFilter::from_bytes(&data).unwrap();
        // Deserialized filter must agree on all inserted keys
        for i in 0..50 {
            assert!(
                bf2.may_contain(format!("key{i}").as_bytes()),
                "false negative after deserialization for key{i}"
            );
        }
    }

    #[test]
    fn from_bytes_empty_returns_passthrough() {
        let bf = BloomFilter::from_bytes(&[]).unwrap();
        assert!(bf.may_contain(b"anything"));
    }

    // --- Hash function ---

    #[test]
    fn bloom_hash_deterministic() {
        assert_eq!(bloom_hash(b"hello"), bloom_hash(b"hello"));
    }

    #[test]
    fn bloom_hash_different_for_different_keys() {
        assert_ne!(bloom_hash(b"hello"), bloom_hash(b"world"));
    }

    #[test]
    fn bloom_hash_empty_key() {
        // Should not panic
        let _ = bloom_hash(b"");
    }

    // --- Clone ---

    #[test]
    fn clone_preserves_state() {
        let mut bf = BloomFilter::new(10);
        bf.insert(b"key1");
        bf.insert(b"key2");
        let _ = bf.build();

        let cloned = bf.clone();
        assert_eq!(cloned.len(), bf.len());
        assert_eq!(cloned.bits_per_key(), bf.bits_per_key());
        assert!(cloned.may_contain(b"key1"));
        assert!(cloned.may_contain(b"key2"));
    }

    // --- Debug ---

    #[test]
    fn debug_format() {
        let bf = BloomFilter::new(10);
        let debug = format!("{bf:?}");
        assert!(debug.contains("BloomFilter"));
    }
}
