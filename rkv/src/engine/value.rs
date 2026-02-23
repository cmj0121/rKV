use std::fmt;

/// The payload associated with a key.
///
/// Two public variants: `Data` holds arbitrary bytes; `Null` means the key
/// exists but carries no payload. An empty `Data` (zero bytes) is a valid,
/// distinct state from `Null`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// Arbitrary-length byte payload.
    Data(Vec<u8>),
    /// Key exists but carries no payload.
    Null,
    /// Internal deletion marker. **Do not construct externally.**
    #[doc(hidden)]
    Tombstone,
}

impl Value {
    /// Create a tombstone value (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn tombstone() -> Self {
        Value::Tombstone
    }

    /// Check whether this value is a tombstone (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
    }

    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns `true` if this value is `Data`.
    pub fn is_data(&self) -> bool {
        matches!(self, Value::Data(_))
    }

    /// Returns the byte payload, or `None` for `Null` and tombstones.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Data(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Consumes the value and returns the byte payload, or `None` for `Null`
    /// and tombstones.
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Value::Data(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Returns the byte length of the payload. `Null` and tombstones return 0.
    pub fn len(&self) -> usize {
        match self {
            Value::Data(bytes) => bytes.len(),
            _ => 0,
        }
    }

    /// Returns `true` if the value carries no bytes.
    ///
    /// `Null` and tombstones are always empty. `Data` is empty when it has
    /// zero bytes (but is still distinct from `Null`).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Data(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => write!(f, "{s}"),
                Err(_) => {
                    for byte in bytes {
                        write!(f, "{byte:02x}")?;
                    }
                    Ok(())
                }
            },
            Value::Null => write!(f, "(null)"),
            Value::Tombstone => write!(f, "(tombstone)"),
        }
    }
}

// --- From conversions (public) ---

impl From<&[u8]> for Value {
    fn from(bytes: &[u8]) -> Self {
        Value::Data(bytes.to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        Value::Data(bytes)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Data(s.as_bytes().to_vec())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Data(s.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Construction ---

    #[test]
    fn data_from_bytes() {
        let v = Value::from(b"hello".as_slice());
        assert_eq!(v, Value::Data(b"hello".to_vec()));
    }

    #[test]
    fn data_from_vec() {
        let v = Value::from(vec![1, 2, 3]);
        assert_eq!(v, Value::Data(vec![1, 2, 3]));
    }

    #[test]
    fn data_from_str() {
        let v = Value::from("hello");
        assert_eq!(v, Value::Data(b"hello".to_vec()));
    }

    #[test]
    fn data_from_string() {
        let v = Value::from(String::from("hello"));
        assert_eq!(v, Value::Data(b"hello".to_vec()));
    }

    #[test]
    fn data_empty() {
        let v = Value::from(b"".as_slice());
        assert_eq!(v, Value::Data(vec![]));
    }

    // --- Null vs empty Data ---

    #[test]
    fn null_is_not_empty_data() {
        assert_ne!(Value::Null, Value::Data(vec![]));
    }

    // --- Accessors ---

    #[test]
    fn is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Data(vec![]).is_null());
    }

    #[test]
    fn is_data() {
        assert!(Value::Data(vec![1]).is_data());
        assert!(!Value::Null.is_data());
    }

    #[test]
    fn as_bytes_data() {
        let v = Value::Data(vec![1, 2, 3]);
        assert_eq!(v.as_bytes(), Some([1, 2, 3].as_slice()));
    }

    #[test]
    fn as_bytes_null() {
        assert_eq!(Value::Null.as_bytes(), None);
    }

    #[test]
    fn into_bytes_data() {
        let v = Value::Data(vec![1, 2, 3]);
        assert_eq!(v.into_bytes(), Some(vec![1, 2, 3]));
    }

    #[test]
    fn into_bytes_null() {
        assert_eq!(Value::Null.into_bytes(), None);
    }

    #[test]
    fn len_data() {
        assert_eq!(Value::Data(vec![1, 2, 3]).len(), 3);
        assert_eq!(Value::Data(vec![]).len(), 0);
    }

    #[test]
    fn len_null() {
        assert_eq!(Value::Null.len(), 0);
    }

    // --- Tombstone (internal) ---

    #[test]
    fn tombstone_creation() {
        let v = Value::tombstone();
        assert!(v.is_tombstone());
        assert!(!v.is_null());
        assert!(!v.is_data());
    }

    #[test]
    fn tombstone_as_bytes_none() {
        assert_eq!(Value::tombstone().as_bytes(), None);
    }

    #[test]
    fn tombstone_len_zero() {
        assert_eq!(Value::tombstone().len(), 0);
    }

    // --- Display ---

    #[test]
    fn display_utf8_data() {
        let v = Value::from("hello");
        assert_eq!(v.to_string(), "hello");
    }

    #[test]
    fn display_binary_data() {
        let v = Value::Data(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(v.to_string(), "deadbeef");
    }

    #[test]
    fn display_null() {
        assert_eq!(Value::Null.to_string(), "(null)");
    }

    #[test]
    fn display_tombstone() {
        assert_eq!(Value::tombstone().to_string(), "(tombstone)");
    }

    // --- Equality ---

    #[test]
    fn data_equality() {
        assert_eq!(Value::from("abc"), Value::from("abc"));
        assert_ne!(Value::from("abc"), Value::from("xyz"));
    }

    #[test]
    fn null_equality() {
        assert_eq!(Value::Null, Value::Null);
    }

    #[test]
    fn tombstone_equality() {
        assert_eq!(Value::tombstone(), Value::tombstone());
    }

    #[test]
    fn all_variants_distinct() {
        let data = Value::Data(vec![]);
        let null = Value::Null;
        let tomb = Value::tombstone();
        assert_ne!(data, null);
        assert_ne!(data, tomb);
        assert_ne!(null, tomb);
    }

    // --- Clone ---

    #[test]
    fn clone_data() {
        let v = Value::from("hello");
        assert_eq!(v.clone(), v);
    }

    #[test]
    fn clone_null() {
        assert_eq!(Value::Null.clone(), Value::Null);
    }
}
