use std::fmt;

use fast32::base32::CROCKFORD_LOWER;

/// A monotonically increasing revision identifier.
///
/// Every mutation produces a new `RevisionID`. Displayed as a Crockford
/// Base32 encoded string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RevisionID(u128);

impl RevisionID {
    /// The zero revision (no revision).
    pub const ZERO: RevisionID = RevisionID(0);

    /// Returns the inner `u128` value.
    pub fn as_u128(self) -> u128 {
        self.0
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
}
