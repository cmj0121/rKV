use std::cmp::Ordering;
use std::fmt;

use super::error::{Error, Result};

const TAG_INT: u8 = 0x01;
const TAG_STR: u8 = 0x02;

const MAX_STR_LEN: usize = 255;

/// Sign-bit flip constant for byte-sortable i64 encoding.
const SIGN_FLIP: u64 = 0x8000_0000_0000_0000;

/// A typed key for the key-value store.
///
/// Two variants: `Int < Str`. Booleans are syntax sugar via `From<bool>`:
/// `true` → `Int(1)`, `false` → `Int(0)`.
///
/// Serialization preserves ordering under byte comparison (`memcmp`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Key {
    Int(i64),
    Str(String),
}

impl Key {
    /// Create a string key with validation.
    ///
    /// Returns an error if `s` exceeds 255 characters or contains interior null bytes.
    pub fn new_str(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.len() > MAX_STR_LEN {
            return Err(Error::InvalidKey(format!(
                "string length {} exceeds maximum {MAX_STR_LEN}",
                s.len()
            )));
        }
        if s.contains('\0') {
            return Err(Error::InvalidKey(
                "string key must not contain interior null bytes".into(),
            ));
        }
        Ok(Key::Str(s))
    }

    /// Serialize the key to bytes. The encoding preserves ordering under `memcmp`.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Key::Int(v) => {
                let flipped = (*v as u64) ^ SIGN_FLIP;
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_INT);
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }
            Key::Str(s) => {
                let mut buf = Vec::with_capacity(1 + s.len() + 1);
                buf.push(TAG_STR);
                buf.extend_from_slice(s.as_bytes());
                buf.push(0x00); // null terminator
                buf
            }
        }
    }

    /// Deserialize a key from bytes produced by [`to_bytes`].
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::InvalidKey("empty key data".into()));
        }
        match data[0] {
            TAG_INT => {
                if data.len() != 9 {
                    return Err(Error::InvalidKey(format!(
                        "int key expects 9 bytes, got {}",
                        data.len()
                    )));
                }
                let flipped = u64::from_be_bytes(data[1..9].try_into().unwrap());
                let v = (flipped ^ SIGN_FLIP) as i64;
                Ok(Key::Int(v))
            }
            TAG_STR => {
                if data.len() < 2 || data[data.len() - 1] != 0x00 {
                    return Err(Error::InvalidKey(
                        "string key must end with null terminator".into(),
                    ));
                }
                let s = std::str::from_utf8(&data[1..data.len() - 1])
                    .map_err(|e| Error::InvalidKey(format!("invalid utf-8 in string key: {e}")))?;
                Key::new_str(s)
            }
            tag => Err(Error::InvalidKey(format!("unknown key tag: 0x{tag:02x}"))),
        }
    }

    /// Serialize the key for prefix matching.
    ///
    /// Like `to_bytes()`, but omits the trailing null terminator for Str keys
    /// so that `other.to_bytes().starts_with(prefix.to_prefix_bytes())` works
    /// correctly.
    pub fn to_prefix_bytes(&self) -> Vec<u8> {
        match self {
            Key::Int(v) => {
                let flipped = (*v as u64) ^ SIGN_FLIP;
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_INT);
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }
            Key::Str(s) => {
                let mut buf = Vec::with_capacity(1 + s.len());
                buf.push(TAG_STR);
                buf.extend_from_slice(s.as_bytes());
                // No null terminator — this is a prefix
                buf
            }
        }
    }

    /// Widen this key to Str. `Int(v)` becomes `Str(v.to_string())`, Str unchanged.
    pub fn widen(&self) -> Key {
        match self {
            Key::Int(v) => Key::Str(v.to_string()),
            Key::Str(_) => self.clone(),
        }
    }

    /// Try to narrow this key. Str that parses as i64 becomes Int, otherwise unchanged.
    pub fn try_narrow(&self) -> Key {
        match self {
            Key::Str(s) => s
                .parse::<i64>()
                .map(Key::Int)
                .unwrap_or_else(|_| self.clone()),
            Key::Int(_) => self.clone(),
        }
    }

    fn variant_order(&self) -> u8 {
        match self {
            Key::Int(_) => 0,
            Key::Str(_) => 1,
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Int(a), Key::Int(b)) => a.cmp(b),
            (Key::Str(a), Key::Str(b)) => a.cmp(b),
            _ => self.variant_order().cmp(&other.variant_order()),
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Int(v) => write!(f, "{v}"),
            Key::Str(s) => write!(f, "{s}"),
        }
    }
}

impl From<bool> for Key {
    fn from(b: bool) -> Self {
        Key::Int(if b { 1 } else { 0 })
    }
}

impl From<i64> for Key {
    fn from(v: i64) -> Self {
        Key::Int(v)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key::Str(s.to_owned())
    }
}

impl From<String> for Key {
    fn from(s: String) -> Self {
        Key::Str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Ordering ---

    #[test]
    fn int_ordering() {
        assert!(Key::Int(-100) < Key::Int(0));
        assert!(Key::Int(0) < Key::Int(100));
        assert!(Key::Int(i64::MIN) < Key::Int(i64::MAX));
    }

    #[test]
    fn str_ordering() {
        assert!(Key::Str("aaa".into()) < Key::Str("bbb".into()));
        assert!(Key::Str("a".into()) < Key::Str("aa".into()));
    }

    #[test]
    fn cross_variant_ordering() {
        assert!(Key::Int(i64::MAX) < Key::Str(String::new()));
    }

    // --- Serialization round-trip ---

    #[test]
    fn roundtrip_int() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let key = Key::Int(v);
            let bytes = key.to_bytes();
            assert_eq!(Key::from_bytes(&bytes).unwrap(), key);
        }
    }

    #[test]
    fn roundtrip_str() {
        for s in ["", "hello", "a".repeat(255).as_str()] {
            let key = Key::Str(s.to_owned());
            let bytes = key.to_bytes();
            assert_eq!(Key::from_bytes(&bytes).unwrap(), key);
        }
    }

    // --- Byte-sort preservation ---

    #[test]
    fn byte_sort_matches_key_ord() {
        let keys = [
            Key::Int(i64::MIN),
            Key::Int(-1),
            Key::Int(0),
            Key::Int(1),
            Key::Int(i64::MAX),
            Key::Str(String::new()),
            Key::Str("aaa".into()),
            Key::Str("bbb".into()),
        ];

        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                let bytes_i = keys[i].to_bytes();
                let bytes_j = keys[j].to_bytes();
                assert_eq!(
                    bytes_i.cmp(&bytes_j),
                    keys[i].cmp(&keys[j]),
                    "byte ordering mismatch for {:?} vs {:?}",
                    keys[i],
                    keys[j]
                );
            }
        }
    }

    // --- Validation errors ---

    #[test]
    fn str_too_long() {
        let long = "x".repeat(256);
        assert!(Key::new_str(long).is_err());
    }

    #[test]
    fn str_interior_null() {
        assert!(Key::new_str("hello\0world").is_err());
    }

    #[test]
    fn str_max_length_ok() {
        let max = "x".repeat(255);
        assert!(Key::new_str(max).is_ok());
    }

    #[test]
    fn from_bytes_empty() {
        assert!(Key::from_bytes(&[]).is_err());
    }

    #[test]
    fn from_bytes_unknown_tag() {
        assert!(Key::from_bytes(&[0xFF]).is_err());
    }

    #[test]
    fn from_bytes_tag_zero_unknown() {
        assert!(Key::from_bytes(&[0x00, 0x01]).is_err());
    }

    #[test]
    fn from_bytes_int_wrong_length() {
        assert!(Key::from_bytes(&[TAG_INT, 0x00]).is_err());
    }

    #[test]
    fn from_bytes_str_no_terminator() {
        assert!(Key::from_bytes(&[TAG_STR, b'a']).is_err());
    }

    // --- Display ---

    #[test]
    fn display_int() {
        assert_eq!(Key::Int(42).to_string(), "42");
        assert_eq!(Key::Int(-1).to_string(), "-1");
    }

    #[test]
    fn display_str() {
        assert_eq!(Key::Str("hello".into()).to_string(), "hello");
    }

    // --- From impls ---

    #[test]
    fn from_bool() {
        assert_eq!(Key::from(true), Key::Int(1));
        assert_eq!(Key::from(false), Key::Int(0));
    }

    #[test]
    fn from_i64() {
        assert_eq!(Key::from(42_i64), Key::Int(42));
    }

    #[test]
    fn from_str() {
        assert_eq!(Key::from("hello"), Key::Str("hello".into()));
    }

    #[test]
    fn from_string() {
        assert_eq!(Key::from(String::from("hello")), Key::Str("hello".into()));
    }

    // --- widen / try_narrow ---

    #[test]
    fn widen_int() {
        assert_eq!(Key::Int(42).widen(), Key::Str("42".into()));
        assert_eq!(Key::Int(-1).widen(), Key::Str("-1".into()));
    }

    #[test]
    fn widen_str_unchanged() {
        let key = Key::Str("hello".into());
        assert_eq!(key.widen(), key);
    }

    #[test]
    fn try_narrow_numeric_str() {
        assert_eq!(Key::Str("42".into()).try_narrow(), Key::Int(42));
        assert_eq!(Key::Str("-1".into()).try_narrow(), Key::Int(-1));
    }

    #[test]
    fn try_narrow_non_numeric_str() {
        let key = Key::Str("hello".into());
        assert_eq!(key.try_narrow(), key);
    }

    #[test]
    fn try_narrow_int_unchanged() {
        assert_eq!(Key::Int(42).try_narrow(), Key::Int(42));
    }
}
