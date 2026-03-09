use super::bloom::BloomFilter;
use super::error::Result;
use super::ribbon::{RibbonFilter, RIBBON_TAG};

/// Unified filter interface wrapping either a Bloom or Ribbon filter.
///
/// Auto-detects the filter type on deserialization via the first byte tag.
#[derive(Clone, Debug)]
pub(crate) enum KeyFilter {
    Bloom(BloomFilter),
    Ribbon(RibbonFilter),
}

impl KeyFilter {
    /// Create a new Bloom filter with the given bits-per-key.
    pub(crate) fn bloom(bits_per_key: usize) -> Self {
        KeyFilter::Bloom(BloomFilter::new(bits_per_key))
    }

    /// Create a new Ribbon filter with the given bits-per-key.
    pub(crate) fn ribbon(bits_per_key: usize) -> Self {
        KeyFilter::Ribbon(RibbonFilter::new(bits_per_key))
    }

    /// Insert a key into the filter (build phase).
    pub(crate) fn insert(&mut self, key: &[u8]) {
        match self {
            KeyFilter::Bloom(f) => f.insert(key),
            KeyFilter::Ribbon(f) => f.insert(key),
        }
    }

    /// Test whether the filter may contain the given key.
    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        match self {
            KeyFilter::Bloom(f) => f.may_contain(key),
            KeyFilter::Ribbon(f) => f.may_contain(key),
        }
    }

    /// Build the filter and serialize to bytes.
    pub(crate) fn build(&mut self) -> Vec<u8> {
        match self {
            KeyFilter::Bloom(f) => f.build(),
            KeyFilter::Ribbon(f) => f.build(),
        }
    }

    /// Deserialize a filter from bytes. Auto-detects Bloom vs Ribbon by tag byte.
    ///
    /// - Tag `0x02` → Ribbon filter
    /// - Anything else → Bloom filter (backward compatible)
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(KeyFilter::Bloom(BloomFilter::from_bytes(data)?));
        }
        if data[0] == RIBBON_TAG {
            Ok(KeyFilter::Ribbon(RibbonFilter::from_bytes(data)?))
        } else {
            Ok(KeyFilter::Bloom(BloomFilter::from_bytes(data)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_roundtrip_via_key_filter() {
        let mut kf = KeyFilter::bloom(10);
        for i in 0..50 {
            kf.insert(format!("key{i}").as_bytes());
        }
        let data = kf.build();
        assert!(!data.is_empty());

        let kf2 = KeyFilter::from_bytes(&data).unwrap();
        assert!(matches!(kf2, KeyFilter::Bloom(_)));
        for i in 0..50 {
            assert!(kf2.may_contain(format!("key{i}").as_bytes()));
        }
    }

    #[test]
    fn ribbon_roundtrip_via_key_filter() {
        let mut kf = KeyFilter::ribbon(10);
        for i in 0..50 {
            kf.insert(format!("key{i}").as_bytes());
        }
        let data = kf.build();
        assert!(!data.is_empty());
        assert_eq!(data[0], RIBBON_TAG);

        let kf2 = KeyFilter::from_bytes(&data).unwrap();
        assert!(matches!(kf2, KeyFilter::Ribbon(_)));
        for i in 0..50 {
            assert!(kf2.may_contain(format!("key{i}").as_bytes()));
        }
    }

    #[test]
    fn empty_data_returns_bloom() {
        let kf = KeyFilter::from_bytes(&[]).unwrap();
        assert!(matches!(kf, KeyFilter::Bloom(_)));
        assert!(kf.may_contain(b"anything"));
    }

    #[test]
    fn auto_detect_bloom_vs_ribbon() {
        // Bloom data starts with num_hashes byte (never 0x02 for reasonable configs)
        let mut bloom = KeyFilter::bloom(10);
        bloom.insert(b"test");
        let bloom_data = bloom.build();

        let mut ribbon = KeyFilter::ribbon(10);
        ribbon.insert(b"test");
        let ribbon_data = ribbon.build();

        assert!(matches!(
            KeyFilter::from_bytes(&bloom_data).unwrap(),
            KeyFilter::Bloom(_)
        ));
        assert!(matches!(
            KeyFilter::from_bytes(&ribbon_data).unwrap(),
            KeyFilter::Ribbon(_)
        ));
    }
}
