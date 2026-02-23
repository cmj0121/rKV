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
    /// Return the algorithm used for this checksum.
    pub(crate) fn algo(&self) -> ChecksumAlgo {
        self.algo
    }

    /// Return the raw checksum value.
    pub(crate) fn value(&self) -> u32 {
        self.value
    }
}
