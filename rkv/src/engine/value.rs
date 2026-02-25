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
    /// Internal pointer to a bin object. **Do not construct externally.**
    #[doc(hidden)]
    #[allow(private_interfaces)]
    Pointer(ValuePointer),
}

impl Value {
    /// Create a tombstone value (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn tombstone() -> Self {
        Value::Tombstone
    }

    /// Serialize the variant to a 1-byte tag for on-disk encoding.
    pub(crate) fn to_tag(&self) -> u8 {
        match self {
            Value::Data(_) => 0x00,
            Value::Null => 0x01,
            Value::Tombstone => 0x02,
            Value::Pointer(_) => 0x03,
        }
    }

    /// Reconstruct a Value from a tag byte and optional data payload.
    pub(crate) fn from_tag(tag: u8, data: &[u8]) -> super::error::Result<Self> {
        match tag {
            0x00 => Ok(Value::Data(data.to_vec())),
            0x01 => Ok(Value::Null),
            0x02 => Ok(Value::Tombstone),
            0x03 => Ok(Value::Pointer(ValuePointer::from_bytes(data)?)),
            _ => Err(super::error::Error::Corruption(format!(
                "unknown value tag: 0x{tag:02x}"
            ))),
        }
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

    /// Returns `true` if this value is a `Pointer` (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn is_pointer(&self) -> bool {
        matches!(self, Value::Pointer(_))
    }

    /// Returns the inner `ValuePointer`, or `None` if not a `Pointer`.
    #[allow(dead_code)]
    pub(crate) fn as_pointer(&self) -> Option<&ValuePointer> {
        match self {
            Value::Pointer(vp) => Some(vp),
            _ => None,
        }
    }

    /// Returns the byte payload, or `None` for `Null`, tombstones, and pointers.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Data(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Consumes the value and returns the byte payload, or `None` for `Null`,
    /// tombstones, and pointers.
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Value::Data(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Returns the byte length of the payload.
    ///
    /// For `Data`, returns the data length. For `Pointer`, returns the original
    /// uncompressed size. `Null` and tombstones return 0.
    pub fn len(&self) -> usize {
        match self {
            Value::Data(bytes) => bytes.len(),
            Value::Pointer(vp) => vp.size as usize,
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
            Value::Pointer(vp) => {
                let hex = vp.hex_hash();
                write!(f, "(object:{}...)", &hex[..8])
            }
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

/// Internal pointer to a bin object stored in the object store.
///
/// When a value exceeds the configured `object_size`, the LSM-tree
/// entry stores a `ValuePointer` instead of the raw bytes. The pointer
/// holds the BLAKE3 content hash (which doubles as the object filename)
/// and the original uncompressed size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValuePointer {
    /// BLAKE3 content hash — also the object filename.
    pub(crate) hash: [u8; 32],
    /// Original (uncompressed) size of the value in bytes.
    pub(crate) size: u32,
}

impl ValuePointer {
    /// Create a new value pointer.
    pub(crate) fn new(hash: [u8; 32], size: u32) -> Self {
        Self { hash, size }
    }

    /// The encoded size of a `ValuePointer` in bytes (32 + 4 = 36).
    pub(crate) const fn encoded_size() -> usize {
        36
    }

    /// Return the hex-encoded hash string (used as the object filename).
    pub(crate) fn hex_hash(&self) -> String {
        self.hash.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Return the fan-out directory prefix (first 2 hex chars).
    pub(crate) fn fan_out_prefix(&self) -> String {
        format!("{:02x}", self.hash[0])
    }

    /// Serialize to 36 bytes: `[hash: 32][size: 4 BE]`.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::encoded_size());
        buf.extend_from_slice(&self.hash);
        buf.extend_from_slice(&self.size.to_be_bytes());
        buf
    }

    /// Deserialize from a byte slice (must be exactly 36 bytes).
    pub(crate) fn from_bytes(data: &[u8]) -> super::error::Result<Self> {
        if data.len() != Self::encoded_size() {
            return Err(super::error::Error::Corruption(format!(
                "ValuePointer expected {} bytes, got {}",
                Self::encoded_size(),
                data.len()
            )));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[..32]);
        let size = u32::from_be_bytes(data[32..36].try_into().unwrap());
        Ok(Self { hash, size })
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
        let ptr = Value::Pointer(ValuePointer::new([0xABu8; 32], 100));
        assert_ne!(data, null);
        assert_ne!(data, tomb);
        assert_ne!(data, ptr);
        assert_ne!(null, tomb);
        assert_ne!(null, ptr);
        assert_ne!(tomb, ptr);
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

    // --- Tag serialization ---

    #[test]
    fn to_tag_data() {
        assert_eq!(Value::Data(vec![1, 2]).to_tag(), 0x00);
    }

    #[test]
    fn to_tag_null() {
        assert_eq!(Value::Null.to_tag(), 0x01);
    }

    #[test]
    fn to_tag_tombstone() {
        assert_eq!(Value::tombstone().to_tag(), 0x02);
    }

    #[test]
    fn from_tag_data() {
        let v = Value::from_tag(0x00, b"hello").unwrap();
        assert_eq!(v, Value::Data(b"hello".to_vec()));
    }

    #[test]
    fn from_tag_data_empty() {
        let v = Value::from_tag(0x00, b"").unwrap();
        assert_eq!(v, Value::Data(vec![]));
    }

    #[test]
    fn from_tag_null() {
        let v = Value::from_tag(0x01, b"").unwrap();
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn from_tag_tombstone() {
        let v = Value::from_tag(0x02, b"").unwrap();
        assert_eq!(v, Value::tombstone());
    }

    #[test]
    fn from_tag_unknown() {
        let err = Value::from_tag(0xFF, b"").unwrap_err();
        assert!(matches!(err, super::super::error::Error::Corruption(_)));
    }

    // --- ValuePointer ---

    #[test]
    fn value_pointer_new() {
        let hash = [0xABu8; 32];
        let vp = ValuePointer::new(hash, 512);
        assert_eq!(vp.hash, hash);
        assert_eq!(vp.size, 512);
    }

    #[test]
    fn value_pointer_encoded_size() {
        assert_eq!(ValuePointer::encoded_size(), 36);
    }

    #[test]
    fn value_pointer_equality() {
        let h1 = [0x01u8; 32];
        let h2 = [0x02u8; 32];
        let a = ValuePointer::new(h1, 50);
        let b = ValuePointer::new(h1, 50);
        let c = ValuePointer::new(h2, 50);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn value_pointer_hex_hash() {
        let mut hash = [0u8; 32];
        hash[0] = 0xAB;
        hash[1] = 0xCD;
        let vp = ValuePointer::new(hash, 100);
        let hex = vp.hex_hash();
        assert_eq!(hex.len(), 64);
        assert!(hex.starts_with("abcd"));
    }

    #[test]
    fn value_pointer_fan_out_prefix() {
        let mut hash = [0u8; 32];
        hash[0] = 0xFF;
        let vp = ValuePointer::new(hash, 100);
        assert_eq!(vp.fan_out_prefix(), "ff");
    }

    #[test]
    fn value_pointer_to_bytes_roundtrip() {
        let hash = [0xABu8; 32];
        let vp = ValuePointer::new(hash, 12345);
        let bytes = vp.to_bytes();
        assert_eq!(bytes.len(), 36);
        let vp2 = ValuePointer::from_bytes(&bytes).unwrap();
        assert_eq!(vp, vp2);
    }

    #[test]
    fn value_pointer_from_bytes_wrong_size() {
        let err = ValuePointer::from_bytes(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, super::super::error::Error::Corruption(_)));
    }

    // --- Pointer variant ---

    #[test]
    fn to_tag_pointer() {
        let vp = ValuePointer::new([0u8; 32], 100);
        assert_eq!(Value::Pointer(vp).to_tag(), 0x03);
    }

    #[test]
    fn from_tag_pointer() {
        let vp = ValuePointer::new([0xABu8; 32], 512);
        let bytes = vp.to_bytes();
        let v = Value::from_tag(0x03, &bytes).unwrap();
        assert_eq!(v, Value::Pointer(ValuePointer::new([0xABu8; 32], 512)));
    }

    #[test]
    fn pointer_len_returns_original_size() {
        let vp = ValuePointer::new([0u8; 32], 4096);
        let v = Value::Pointer(vp);
        assert_eq!(v.len(), 4096);
    }

    #[test]
    fn pointer_as_bytes_returns_none() {
        let vp = ValuePointer::new([0u8; 32], 100);
        assert_eq!(Value::Pointer(vp).as_bytes(), None);
    }

    #[test]
    fn pointer_into_bytes_returns_none() {
        let vp = ValuePointer::new([0u8; 32], 100);
        assert_eq!(Value::Pointer(vp).into_bytes(), None);
    }

    #[test]
    fn pointer_is_not_empty() {
        let vp = ValuePointer::new([0u8; 32], 100);
        assert!(!Value::Pointer(vp).is_empty());
    }

    #[test]
    fn display_pointer() {
        let vp = ValuePointer::new([0xABu8; 32], 100);
        let s = Value::Pointer(vp).to_string();
        assert_eq!(s, "(object:abababab...)");
    }

    #[test]
    fn is_pointer_check() {
        let vp = ValuePointer::new([0u8; 32], 100);
        assert!(Value::Pointer(vp).is_pointer());
        assert!(!Value::Data(vec![]).is_pointer());
    }

    #[test]
    fn as_pointer_check() {
        let vp = ValuePointer::new([0xABu8; 32], 100);
        let v = Value::Pointer(vp.clone());
        assert_eq!(v.as_pointer(), Some(&vp));
        assert_eq!(Value::Data(vec![]).as_pointer(), None);
    }
}
