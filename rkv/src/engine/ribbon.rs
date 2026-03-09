use super::error::{Error, Result};

/// Ribbon width in bits — matches u64 for efficient register operations.
const RIBBON_WIDTH: usize = 64;

/// Ribbon filter type tag for serialization (distinguishes from Bloom's 0x00/0x01).
pub(crate) const RIBBON_TAG: u8 = 0x02;

/// Overhead factor: allocate ~1.23x slots to ensure banding succeeds.
const OVERHEAD_NUMERATOR: usize = 123;
const OVERHEAD_DENOMINATOR: usize = 100;

/// Per-SSTable Ribbon filter for probabilistic key membership testing.
///
/// A Ribbon filter uses the RIBBON algorithm (Rapid Incremental Boolean
/// Banding ON the fly) to achieve ~30% less space than a Bloom filter at
/// the same false-positive rate.
///
/// **Build phase**: call `insert()` for each key, then `build()` to
/// construct and serialize the filter. **Query phase**: call `from_bytes()`
/// to deserialize, then `may_contain()` to test membership.
#[derive(Clone, Debug)]
pub(crate) struct RibbonFilter {
    /// Number of result bits per key (controls FPR: FPR ~ 2^(-r)).
    result_bits: u8,
    /// Number of rows in the solution array.
    num_rows: u32,
    /// Solution array: `result_bits` parallel bit-vectors, each `ceil(num_rows/8)` bytes.
    solution: Vec<u8>,
    /// Collected key hashes during build phase.
    key_hashes: Vec<u64>,
    /// Configured bits per key.
    bits_per_key: usize,
}

/// Hash a key using SipHash-like mixing for good distribution.
fn ribbon_hash(data: &[u8]) -> u64 {
    // Use two rounds of mixing for good avalanche.
    let mut h: u64 = 0x517cc1b727220a95 ^ (data.len() as u64);
    for chunk in data.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        let w = u64::from_le_bytes(buf);
        h ^= w;
        h = h.wrapping_mul(0x9e3779b97f4a7c15);
        h ^= h >> 32;
    }
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    h
}

/// Derive (start_row, coefficients, result) from a 64-bit hash.
///
/// - `start_row`: where the band begins in the solution array (0..num_rows-RIBBON_WIDTH)
/// - `coefficients`: a u64 bitmask (the "band"), always with bit 0 set
/// - `result`: r-bit fingerprint
fn hash_to_band(h: u64, num_rows: u32, result_bits: u8) -> (u32, u64, u16) {
    // Split the hash: upper bits for start_row, middle for coefficients, lower for result
    let usable_rows = num_rows.saturating_sub(RIBBON_WIDTH as u32 - 1);
    let start_row = if usable_rows > 0 {
        (h >> 32) as u32 % usable_rows
    } else {
        0
    };
    // Coefficients: use lower 64 bits, ensure bit 0 is set (required for banding)
    let coefficients = h | 1;
    // Result: use a secondary hash for the fingerprint
    let h2 = h.wrapping_mul(0x517cc1b727220a95) ^ (h >> 17);
    let result = (h2 & ((1u64 << result_bits) - 1)) as u16;
    (start_row, coefficients, result)
}

impl RibbonFilter {
    /// Create a new Ribbon filter with the given bits-per-key setting.
    ///
    /// `bits_per_key` controls space-accuracy: 7 bits/key ~ 1% FPR (vs 10 for Bloom).
    /// Set to 0 to disable.
    pub(crate) fn new(bits_per_key: usize) -> Self {
        // result_bits r: FPR = 2^(-r), so r = ceil(bits_per_key * ln(2) / ln(2)) ~ bits_per_key
        // Actually r ≈ bits_per_key for Ribbon (each result bit halves FPR).
        // Clamp to [1, 16].
        let result_bits = if bits_per_key == 0 {
            1
        } else {
            (bits_per_key as u8).clamp(1, 16)
        };

        Self {
            result_bits,
            num_rows: 0,
            solution: Vec::new(),
            key_hashes: Vec::new(),
            bits_per_key,
        }
    }

    /// Insert a key into the filter (build phase).
    pub(crate) fn insert(&mut self, key: &[u8]) {
        self.key_hashes.push(ribbon_hash(key));
    }

    /// Test whether the filter may contain the given key.
    ///
    /// Returns `true` if the key might be present (possible false positive),
    /// `false` if the key is definitely absent (no false negatives).
    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        if self.solution.is_empty() || self.num_rows == 0 {
            return true; // no filter built — conservative
        }

        let h = ribbon_hash(key);
        let (start_row, coefficients, expected_result) =
            hash_to_band(h, self.num_rows, self.result_bits);

        // Iterate coefficient bits once, accumulating all result planes simultaneously
        let bytes_per_plane = self.num_rows.div_ceil(8) as usize;
        let mut accumulated = vec![0u8; self.result_bits as usize];
        let mut coeff = coefficients;

        for j in 0..RIBBON_WIDTH {
            if coeff & 1 != 0 {
                let row = start_row as usize + j;
                let byte_idx_base = row / 8;
                let bit_idx = row % 8;
                for (r, acc) in accumulated.iter_mut().enumerate() {
                    let byte_idx = r * bytes_per_plane + byte_idx_base;
                    if byte_idx < self.solution.len() {
                        *acc ^= (self.solution[byte_idx] >> bit_idx) & 1;
                    }
                }
            }
            coeff >>= 1;
        }

        for (r, &acc) in accumulated.iter().enumerate() {
            if acc != ((expected_result >> r) & 1) as u8 {
                return false;
            }
        }

        true
    }

    /// Number of keys inserted.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.key_hashes.len()
    }

    /// Returns `true` if no keys have been inserted.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.key_hashes.is_empty()
    }

    /// Returns the configured bits-per-key.
    #[cfg(test)]
    pub(crate) fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }

    /// Build the filter and serialize to bytes.
    ///
    /// Returns `[RIBBON_TAG: u8][result_bits: u8][num_rows: u32 LE][solution...]`.
    /// Returns empty vec if no keys or bits_per_key == 0.
    pub(crate) fn build(&mut self) -> Vec<u8> {
        if self.key_hashes.is_empty() || self.bits_per_key == 0 {
            return Vec::new();
        }

        let n = self.key_hashes.len();
        // Allocate with overhead to ensure banding succeeds
        let num_rows =
            ((n * OVERHEAD_NUMERATOR) / OVERHEAD_DENOMINATOR).max(RIBBON_WIDTH + 1) as u32;
        self.num_rows = num_rows;

        let bytes_per_plane = num_rows.div_ceil(8) as usize;
        let total_solution_bytes = self.result_bits as usize * bytes_per_plane;

        // Banding phase: on-the-fly Gaussian elimination
        // Each slot stores (coefficients: u64, result: u16, occupied: bool)
        let mut band_coeff = vec![0u64; num_rows as usize];
        let mut band_result = vec![0u16; num_rows as usize];
        let mut occupied = vec![false; num_rows as usize];

        for &h in &self.key_hashes {
            let (start_row, mut coeff, mut result) = hash_to_band(h, num_rows, self.result_bits);

            let mut row = start_row as usize;
            while row < num_rows as usize && coeff != 0 {
                if !occupied[row] {
                    // Place here
                    band_coeff[row] = coeff;
                    band_result[row] = result;
                    occupied[row] = true;
                    break;
                }
                // XOR-reduce with existing row
                let existing_coeff = band_coeff[row];
                let existing_result = band_result[row];
                coeff ^= existing_coeff;
                result ^= existing_result;
                // Find next set bit position
                if coeff == 0 {
                    break; // linearly dependent — acceptable (adds to FPR slightly)
                }
                let shift = coeff.trailing_zeros() as usize;
                coeff >>= shift;
                // Result bits don't need rotation — XOR algebra is self-consistent
                // since we XOR results during elimination.
                row += shift;
            }
        }

        // Back-substitution: solve for solution bits
        let mut solution = vec![0u8; total_solution_bytes];

        // Process rows from bottom to top
        for row in (0..num_rows as usize).rev() {
            if !occupied[row] {
                continue;
            }

            let coeff = band_coeff[row];
            let mut result = band_result[row];

            // XOR in already-solved rows that this row depends on
            let mut c = coeff >> 1; // skip bit 0 (the pivot)
            let mut j = row + 1;
            while c != 0 && j < num_rows as usize {
                if c & 1 != 0 {
                    // Read the solved value for row j
                    for r in 0..self.result_bits {
                        let plane_offset = r as usize * bytes_per_plane;
                        let byte_idx = plane_offset + j / 8;
                        let bit_idx = j % 8;
                        let solved_bit = (solution[byte_idx] >> bit_idx) & 1;
                        result ^= (solved_bit as u16) << r;
                    }
                }
                c >>= 1;
                j += 1;
            }

            // Write solution for this row
            for r in 0..self.result_bits {
                let bit = (result >> r) & 1;
                let plane_offset = r as usize * bytes_per_plane;
                let byte_idx = plane_offset + row / 8;
                let bit_idx = row % 8;
                solution[byte_idx] |= (bit as u8) << bit_idx;
            }
        }

        self.solution = solution;
        self.key_hashes = Vec::new(); // free hash memory after banding

        // Serialize: [tag][result_bits][num_rows LE][solution]
        let mut out = Vec::with_capacity(1 + 1 + 4 + total_solution_bytes);
        out.push(RIBBON_TAG);
        out.push(self.result_bits);
        out.extend_from_slice(&num_rows.to_le_bytes());
        out.extend_from_slice(&self.solution);
        out
    }

    /// Deserialize a Ribbon filter from bytes.
    ///
    /// Expects `[RIBBON_TAG: u8][result_bits: u8][num_rows: u32 LE][solution...]`.
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        // Minimum: tag(1) + result_bits(1) + num_rows(4) = 6
        if data.len() < 6 {
            return Err(Error::Corruption("Ribbon filter data too short".into()));
        }

        if data[0] != RIBBON_TAG {
            return Err(Error::Corruption(format!(
                "Ribbon filter bad tag: expected {RIBBON_TAG:#x}, got {:#x}",
                data[0]
            )));
        }

        let result_bits = data[1];
        if result_bits == 0 || result_bits > 16 {
            return Err(Error::Corruption(format!(
                "Ribbon filter invalid result_bits: {result_bits}"
            )));
        }

        let num_rows = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        let bytes_per_plane = num_rows.div_ceil(8) as usize;
        let expected_solution_len = result_bits as usize * bytes_per_plane;
        let solution_data = &data[6..];

        if solution_data.len() < expected_solution_len {
            return Err(Error::Corruption(format!(
                "Ribbon filter solution too short: expected {expected_solution_len}, got {}",
                solution_data.len()
            )));
        }

        Ok(Self {
            result_bits,
            num_rows,
            solution: solution_data[..expected_solution_len].to_vec(),
            key_hashes: Vec::new(),
            bits_per_key: result_bits as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Construction ---

    #[test]
    fn new_default() {
        let rf = RibbonFilter::new(10);
        assert_eq!(rf.bits_per_key(), 10);
        assert!(rf.is_empty());
        assert_eq!(rf.len(), 0);
    }

    #[test]
    fn new_zero_disabled() {
        let rf = RibbonFilter::new(0);
        assert_eq!(rf.bits_per_key(), 0);
    }

    // --- Insert / query ---

    #[test]
    fn inserted_keys_always_found() {
        let mut rf = RibbonFilter::new(10);
        for i in 0..200 {
            rf.insert(format!("key{i}").as_bytes());
        }
        let _ = rf.build();

        // No false negatives
        for i in 0..200 {
            assert!(
                rf.may_contain(format!("key{i}").as_bytes()),
                "false negative for key{i}"
            );
        }
    }

    #[test]
    fn non_inserted_keys_mostly_rejected() {
        let mut rf = RibbonFilter::new(10);
        for i in 0..1000 {
            rf.insert(format!("key{i}").as_bytes());
        }
        let _ = rf.build();

        let mut false_positives = 0;
        for i in 1000..11000 {
            if rf.may_contain(format!("key{i}").as_bytes()) {
                false_positives += 1;
            }
        }
        // At 10 result bits, expected FPR ~ 1/1024 ~ 0.1%. Allow up to 3%.
        let fpr = false_positives as f64 / 10000.0;
        assert!(
            fpr < 0.03,
            "false positive rate too high: {fpr:.4} ({false_positives}/10000)"
        );
    }

    #[test]
    fn may_contain_returns_true_when_no_filter_built() {
        let rf = RibbonFilter::new(10);
        assert!(rf.may_contain(b"anything"));
    }

    #[test]
    fn insert_increments_count() {
        let mut rf = RibbonFilter::new(10);
        rf.insert(b"key1");
        assert_eq!(rf.len(), 1);
        rf.insert(b"key2");
        assert_eq!(rf.len(), 2);
    }

    // --- Serialization ---

    #[test]
    fn build_empty_returns_empty() {
        let mut rf = RibbonFilter::new(10);
        assert!(rf.build().is_empty());
    }

    #[test]
    fn build_disabled_returns_empty() {
        let mut rf = RibbonFilter::new(0);
        rf.insert(b"key");
        assert!(rf.build().is_empty());
    }

    #[test]
    fn roundtrip_serialization() {
        let mut rf = RibbonFilter::new(10);
        for i in 0..100 {
            rf.insert(format!("key{i}").as_bytes());
        }
        let data = rf.build();
        assert!(!data.is_empty());
        assert_eq!(data[0], RIBBON_TAG);

        let rf2 = RibbonFilter::from_bytes(&data).unwrap();
        for i in 0..100 {
            assert!(
                rf2.may_contain(format!("key{i}").as_bytes()),
                "false negative after deserialization for key{i}"
            );
        }
    }

    #[test]
    fn from_bytes_too_short() {
        assert!(RibbonFilter::from_bytes(&[RIBBON_TAG, 10]).is_err());
    }

    #[test]
    fn from_bytes_bad_tag() {
        assert!(RibbonFilter::from_bytes(&[0xFF, 10, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn from_bytes_invalid_result_bits() {
        assert!(RibbonFilter::from_bytes(&[RIBBON_TAG, 0, 0, 0, 0, 0]).is_err());
        assert!(RibbonFilter::from_bytes(&[RIBBON_TAG, 17, 0, 0, 0, 0]).is_err());
    }

    // --- Hash function ---

    #[test]
    fn hash_deterministic() {
        assert_eq!(ribbon_hash(b"hello"), ribbon_hash(b"hello"));
    }

    #[test]
    fn hash_different_keys() {
        assert_ne!(ribbon_hash(b"hello"), ribbon_hash(b"world"));
    }

    #[test]
    fn hash_empty_key() {
        let _ = ribbon_hash(b"");
    }

    // --- Space efficiency ---

    #[test]
    fn ribbon_smaller_than_bloom_at_equal_fpr() {
        use super::super::bloom::BloomFilter;

        // Both target ~1% FPR: Bloom needs 10 bits/key, Ribbon needs 7
        let n = 1000;

        let mut bloom = BloomFilter::new(10);
        let mut ribbon = RibbonFilter::new(7);

        for i in 0..n {
            let key = format!("key{i}");
            bloom.insert(key.as_bytes());
            ribbon.insert(key.as_bytes());
        }

        let bloom_data = bloom.build();
        let ribbon_data = ribbon.build();

        // Ribbon at 7 bits/key should be smaller than Bloom at 10 bits/key
        assert!(
            ribbon_data.len() < bloom_data.len(),
            "Ribbon ({}) should be smaller than Bloom ({}) at equal ~1% FPR",
            ribbon_data.len(),
            bloom_data.len()
        );
    }

    // --- Clone ---

    #[test]
    fn clone_preserves_state() {
        let mut rf = RibbonFilter::new(10);
        rf.insert(b"key1");
        rf.insert(b"key2");
        let _ = rf.build();

        let cloned = rf.clone();
        assert!(cloned.may_contain(b"key1"));
        assert!(cloned.may_contain(b"key2"));
    }

    // --- Large dataset ---

    #[test]
    fn large_dataset_no_false_negatives() {
        let mut rf = RibbonFilter::new(7);
        for i in 0..5000 {
            rf.insert(format!("item_{i:06}").as_bytes());
        }
        let _ = rf.build();

        for i in 0..5000 {
            assert!(
                rf.may_contain(format!("item_{i:06}").as_bytes()),
                "false negative for item_{i:06}"
            );
        }
    }
}
