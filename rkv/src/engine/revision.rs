use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use fast32::base32::CROCKFORD_LOWER;

/// A monotonically increasing revision identifier.
///
/// Every mutation produces a new `RevisionID`. Displayed as a Crockford
/// Base32 encoded string.
///
/// Internal layout (ULID-like, 128 bits):
///
/// ```text
///  MSB                                                              LSB
///  [  timestamp 48  |  cluster 16  |  process 16  |  sequence 48  ]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RevisionID(u128);

impl RevisionID {
    /// The zero revision (no revision).
    pub const ZERO: RevisionID = RevisionID(0);

    /// Returns the inner `u128` value.
    pub fn as_u128(self) -> u128 {
        self.0
    }

    /// Returns the timestamp component (ms since Unix epoch, bits 127–80).
    pub fn timestamp_ms(&self) -> u64 {
        (self.0 >> 80) as u64 & 0xFFFF_FFFF_FFFF
    }

    /// Returns the cluster ID component (bits 79–64).
    pub fn cluster_id(&self) -> u16 {
        (self.0 >> 64) as u16
    }

    /// Returns the process ID component (bits 63–48).
    pub fn process_id(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    /// Returns the sequence component (bits 47–0).
    pub fn sequence(&self) -> u64 {
        self.0 as u64 & 0xFFFF_FFFF_FFFF
    }
}

impl fmt::Display for RevisionID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&CROCKFORD_LOWER.encode_u128(self.0))
    }
}

impl From<u128> for RevisionID {
    fn from(v: u128) -> Self {
        RevisionID(v)
    }
}

impl From<RevisionID> for u128 {
    fn from(r: RevisionID) -> Self {
        r.0
    }
}

/// Generates ULID-like RevisionIDs with a 48-16-16-48 bit layout.
pub(crate) struct RevisionGen {
    cluster_id: u16,
    process_id: u16,
}

impl RevisionGen {
    pub(crate) fn new(cluster_id: Option<u16>) -> Self {
        Self {
            cluster_id: cluster_id.unwrap_or_else(|| fastrand::u16(..)),
            process_id: std::process::id() as u16,
        }
    }

    pub(crate) fn cluster_id(&self) -> u16 {
        self.cluster_id
    }

    pub(crate) fn generate(&self) -> RevisionID {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let seq: u64 = fastrand::u64(..);
        let raw: u128 = ((ts as u128 & 0xFFFF_FFFF_FFFF) << 80)
            | ((self.cluster_id as u128) << 64)
            | ((self.process_id as u128) << 48)
            | (seq as u128 & 0xFFFF_FFFF_FFFF);
        RevisionID::from(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_zero() {
        assert_eq!(RevisionID::ZERO.to_string(), "0");
    }

    #[test]
    fn display_nonzero() {
        assert_eq!(RevisionID::from(255).to_string(), "7z");
    }

    #[test]
    fn display_max() {
        assert_eq!(
            RevisionID::from(u128::MAX).to_string(),
            "7zzzzzzzzzzzzzzzzzzzzzzzzz"
        );
    }

    #[test]
    fn as_u128() {
        assert_eq!(RevisionID::from(42).as_u128(), 42);
    }

    #[test]
    fn into_u128() {
        let r = RevisionID::from(42);
        let v: u128 = r.into();
        assert_eq!(v, 42);
    }

    #[test]
    fn ordering() {
        assert!(RevisionID::from(1) < RevisionID::from(2));
        assert!(RevisionID::from(100) > RevisionID::ZERO);
    }

    #[test]
    fn equality() {
        assert_eq!(RevisionID::from(42), RevisionID::from(42));
        assert_ne!(RevisionID::from(1), RevisionID::from(2));
    }

    #[test]
    fn copy_semantics() {
        let r = RevisionID::from(42);
        let r2 = r;
        assert_eq!(r, r2);
    }

    // --- Field accessors ---

    #[test]
    fn field_accessors_roundtrip() {
        let ts: u64 = 0x0001_2345_6789; // 48-bit timestamp
        let cluster: u16 = 0xABCD;
        let process: u16 = 0x1234;
        let seq: u64 = 0x0000_FEDC_BA98; // 48-bit sequence

        let raw: u128 = ((ts as u128) << 80)
            | ((cluster as u128) << 64)
            | ((process as u128) << 48)
            | (seq as u128);
        let rev = RevisionID::from(raw);

        assert_eq!(rev.timestamp_ms(), ts);
        assert_eq!(rev.cluster_id(), cluster);
        assert_eq!(rev.process_id(), process);
        assert_eq!(rev.sequence(), seq);
    }

    #[test]
    fn field_accessors_zero() {
        let rev = RevisionID::ZERO;
        assert_eq!(rev.timestamp_ms(), 0);
        assert_eq!(rev.cluster_id(), 0);
        assert_eq!(rev.process_id(), 0);
        assert_eq!(rev.sequence(), 0);
    }

    // --- RevisionGen ---

    #[test]
    fn generate_nonzero() {
        let gen = RevisionGen::new(Some(1));
        let rev = gen.generate();
        assert_ne!(rev, RevisionID::ZERO);
    }

    #[test]
    fn generate_embeds_cluster_id() {
        let gen = RevisionGen::new(Some(0x00FF));
        let rev = gen.generate();
        assert_eq!(rev.cluster_id(), 0x00FF);
    }

    #[test]
    fn generate_embeds_process_id() {
        let gen = RevisionGen::new(Some(1));
        let rev = gen.generate();
        assert_eq!(rev.process_id(), std::process::id() as u16);
    }

    #[test]
    fn generate_has_nonzero_timestamp() {
        let gen = RevisionGen::new(None);
        let rev = gen.generate();
        assert!(rev.timestamp_ms() > 0);
    }

    #[test]
    fn generate_two_are_different() {
        let gen = RevisionGen::new(None);
        let r1 = gen.generate();
        let r2 = gen.generate();
        // Extremely unlikely to get the same random sequence
        assert_ne!(r1, r2);
    }

    #[test]
    fn generate_random_cluster_when_none() {
        // Just verify it doesn't panic and produces a valid revision
        let gen = RevisionGen::new(None);
        let rev = gen.generate();
        assert_ne!(rev, RevisionID::ZERO);
    }
}
