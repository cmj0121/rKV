use std::fmt;

use fast32::base32::CROCKFORD_LOWER;

/// A monotonically increasing revision identifier.
///
/// Every mutation produces a new `Revision`. Displayed as a Crockford
/// Base32 encoded string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u128);

impl Revision {
    /// The zero revision (no revision).
    pub const ZERO: Revision = Revision(0);

    /// Returns the inner `u128` value.
    pub fn as_u128(self) -> u128 {
        self.0
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&CROCKFORD_LOWER.encode_u128(self.0))
    }
}

impl From<u128> for Revision {
    fn from(v: u128) -> Self {
        Revision(v)
    }
}

impl From<Revision> for u128 {
    fn from(r: Revision) -> Self {
        r.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_zero() {
        assert_eq!(Revision::ZERO.to_string(), "0");
    }

    #[test]
    fn display_nonzero() {
        assert_eq!(Revision::from(255).to_string(), "7z");
    }

    #[test]
    fn display_max() {
        assert_eq!(
            Revision::from(u128::MAX).to_string(),
            "7zzzzzzzzzzzzzzzzzzzzzzzzz"
        );
    }

    #[test]
    fn as_u128() {
        assert_eq!(Revision::from(42).as_u128(), 42);
    }

    #[test]
    fn into_u128() {
        let r = Revision::from(42);
        let v: u128 = r.into();
        assert_eq!(v, 42);
    }

    #[test]
    fn ordering() {
        assert!(Revision::from(1) < Revision::from(2));
        assert!(Revision::from(100) > Revision::ZERO);
    }

    #[test]
    fn equality() {
        assert_eq!(Revision::from(42), Revision::from(42));
        assert_ne!(Revision::from(1), Revision::from(2));
    }

    #[test]
    fn copy_semantics() {
        let r = Revision::from(42);
        let r2 = r;
        assert_eq!(r, r2);
    }
}
