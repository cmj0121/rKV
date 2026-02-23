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
