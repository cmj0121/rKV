use super::error::{Error, Result};

/// Algorithm tag for checksum computation.
///
/// Currently only CRC32C is defined. The tag is stored alongside the
/// checksum value so readers can identify the algorithm without external
/// metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChecksumAlgo {
    /// CRC32C (Castagnoli) — hardware-accelerated on modern x86/ARM.
    Crc32c = 0x01,
}

/// A checksum value paired with its algorithm tag.
///
/// Every WAL entry and SSTable block carries a `Checksum`. On write the
/// engine computes the checksum over the raw data; on read the engine
/// recomputes and compares to detect corruption.
///
/// **Stub implementation**: `compute()` returns a zeroed checksum and
/// `verify()` always succeeds. Actual CRC32C logic will be wired in
/// when the persistence layer lands.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct Checksum {
    algo: ChecksumAlgo,
    value: u32,
}

#[allow(dead_code)]
impl Checksum {
    /// Compute a checksum over `data`.
    ///
    /// Stub: returns a zeroed CRC32C checksum.
    pub(crate) fn compute(_data: &[u8]) -> Self {
        Self {
            algo: ChecksumAlgo::Crc32c,
            value: 0,
        }
    }

    /// Verify this checksum against `data`.
    ///
    /// Stub: always succeeds.
    pub(crate) fn verify(&self, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Return the algorithm used for this checksum.
    pub(crate) fn algo(&self) -> ChecksumAlgo {
        self.algo
    }

    /// Return the raw checksum value.
    pub(crate) fn value(&self) -> u32 {
        self.value
    }

    /// The encoded size of a `Checksum` in bytes (1 algo tag + 4 value).
    pub(crate) const fn encoded_size() -> usize {
        5
    }

    /// Serialize the checksum to bytes: `[algo_tag, value_be(4)]`.
    ///
    /// The format is a 1-byte algorithm tag followed by the 4-byte
    /// big-endian checksum value.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::encoded_size());
        buf.push(self.algo as u8);
        buf.extend_from_slice(&self.value.to_be_bytes());
        buf
    }

    /// Deserialize a checksum from bytes.
    ///
    /// Stub: returns `NotImplemented`.
    pub(crate) fn from_bytes(_data: &[u8]) -> Result<Self> {
        Err(Error::NotImplemented("Checksum::from_bytes".into()))
    }

    /// Construct a checksum directly from an algorithm and value.
    pub(crate) fn from_raw(algo: ChecksumAlgo, value: u32) -> Self {
        Self { algo, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Construction ---

    #[test]
    fn compute_returns_zeroed_crc32c() {
        let cs = Checksum::compute(b"hello");
        assert_eq!(cs.algo(), ChecksumAlgo::Crc32c);
        assert_eq!(cs.value(), 0);
    }

    #[test]
    fn compute_empty_data() {
        let cs = Checksum::compute(b"");
        assert_eq!(cs.algo(), ChecksumAlgo::Crc32c);
        assert_eq!(cs.value(), 0);
    }

    #[test]
    fn from_raw_stores_values() {
        let cs = Checksum::from_raw(ChecksumAlgo::Crc32c, 0xDEAD_BEEF);
        assert_eq!(cs.algo(), ChecksumAlgo::Crc32c);
        assert_eq!(cs.value(), 0xDEAD_BEEF);
    }

    // --- Verify ---

    #[test]
    fn verify_always_succeeds() {
        let cs = Checksum::compute(b"data");
        assert!(cs.verify(b"data").is_ok());
        assert!(cs.verify(b"different").is_ok()); // stub: always ok
    }

    // --- Accessors ---

    #[test]
    fn algo_returns_crc32c() {
        let cs = Checksum::compute(b"x");
        assert_eq!(cs.algo(), ChecksumAlgo::Crc32c);
    }

    #[test]
    fn value_returns_stored_value() {
        let cs = Checksum::from_raw(ChecksumAlgo::Crc32c, 42);
        assert_eq!(cs.value(), 42);
    }

    // --- Encoded size ---

    #[test]
    fn encoded_size_is_five() {
        assert_eq!(Checksum::encoded_size(), 5);
    }

    // --- Serialization ---

    #[test]
    fn to_bytes_format() {
        let cs = Checksum::from_raw(ChecksumAlgo::Crc32c, 0x01020304);
        let bytes = cs.to_bytes();
        assert_eq!(bytes.len(), 5);
        assert_eq!(bytes[0], 0x01); // algo tag
        assert_eq!(&bytes[1..], &[0x01, 0x02, 0x03, 0x04]); // big-endian value
    }

    #[test]
    fn to_bytes_zeroed() {
        let cs = Checksum::compute(b"anything");
        let bytes = cs.to_bytes();
        assert_eq!(bytes, vec![0x01, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn to_bytes_length_matches_encoded_size() {
        let cs = Checksum::from_raw(ChecksumAlgo::Crc32c, u32::MAX);
        assert_eq!(cs.to_bytes().len(), Checksum::encoded_size());
    }

    #[test]
    fn from_bytes_returns_not_implemented() {
        let result = Checksum::from_bytes(&[0x01, 0x00, 0x00, 0x00, 0x00]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::NotImplemented(ref msg) if msg.contains("Checksum")),
            "expected NotImplemented, got: {err}",
        );
    }

    // --- Equality ---

    #[test]
    fn equality_same_values() {
        let a = Checksum::from_raw(ChecksumAlgo::Crc32c, 100);
        let b = Checksum::from_raw(ChecksumAlgo::Crc32c, 100);
        assert_eq!(a, b);
    }

    #[test]
    fn inequality_different_values() {
        let a = Checksum::from_raw(ChecksumAlgo::Crc32c, 100);
        let b = Checksum::from_raw(ChecksumAlgo::Crc32c, 200);
        assert_ne!(a, b);
    }

    // --- Clone ---

    #[test]
    fn clone_preserves_state() {
        let cs = Checksum::from_raw(ChecksumAlgo::Crc32c, 0xCAFE);
        let cloned = cs.clone();
        assert_eq!(cs, cloned);
    }

    // --- Debug ---

    #[test]
    fn debug_format() {
        let cs = Checksum::compute(b"x");
        let debug = format!("{cs:?}");
        assert!(debug.contains("Checksum"));
    }

    // --- ChecksumAlgo ---

    #[test]
    fn checksum_algo_copy() {
        let algo = ChecksumAlgo::Crc32c;
        let copy = algo;
        assert_eq!(algo, copy);
    }

    #[test]
    fn checksum_algo_discriminant() {
        assert_eq!(ChecksumAlgo::Crc32c as u8, 0x01);
    }
}
