use std::fmt;

/// All Knot errors.
#[derive(Debug)]
pub enum Error {
    /// Namespace does not exist.
    NamespaceNotFound(String),
    /// Data table does not exist.
    TableNotFound(String),
    /// Link table does not exist.
    LinkTableNotFound(String),
    /// Node key does not exist in table.
    KeyNotFound(String),
    /// Link entry does not exist.
    LinkNotFound { from: String, to: String },
    /// Table already exists.
    TableExists(String),
    /// Link table already exists.
    LinkTableExists(String),
    /// Index already exists.
    IndexExists(String),
    /// Used bidirectional syntax on a directional link table.
    NotBidirectional(String),
    /// Key is empty, contains control characters, or exceeds 511 bytes.
    InvalidKey(String),
    /// Name contains dots, control characters, or exceeds 511 bytes.
    InvalidName(String),
    /// Property name or value is invalid.
    InvalidProperty(String),
    /// Malformed query condition or filter expression.
    InvalidFilter(String),
    /// Source or target node does not exist when creating a link.
    EndpointNotFound(String),
    /// rKV error propagated.
    StorageError(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamespaceNotFound(s) => write!(f, "namespace not found: {s}"),
            Self::TableNotFound(s) => write!(f, "table not found: {s}"),
            Self::LinkTableNotFound(s) => write!(f, "link table not found: {s}"),
            Self::KeyNotFound(s) => write!(f, "key not found: {s}"),
            Self::LinkNotFound { from, to } => {
                write!(f, "link not found: {from} -> {to}")
            }
            Self::TableExists(s) => write!(f, "table already exists: {s}"),
            Self::LinkTableExists(s) => write!(f, "link table already exists: {s}"),
            Self::IndexExists(s) => write!(f, "index already exists: {s}"),
            Self::NotBidirectional(s) => {
                write!(f, "link table is not bidirectional: {s}")
            }
            Self::InvalidKey(s) => write!(f, "invalid key: {s}"),
            Self::InvalidName(s) => write!(f, "invalid name: {s}"),
            Self::InvalidProperty(s) => write!(f, "invalid property: {s}"),
            Self::InvalidFilter(s) => write!(f, "invalid filter: {s}"),
            Self::EndpointNotFound(s) => write!(f, "endpoint not found: {s}"),
            Self::StorageError(s) => write!(f, "storage error: {s}"),
        }
    }
}

impl std::error::Error for Error {}

/// Maximum key/name size in bytes (null-terminated at 512).
pub const MAX_KEY_SIZE: usize = 511;

/// Convert an rKV error into a Knot StorageError.
pub fn storage(e: rkv::Error) -> Error {
    Error::StorageError(e.to_string())
}

/// Validate a name (table, namespace, link, property). No dots, no control
/// characters, non-empty, max 511 bytes.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidName("name must not be empty".into()));
    }
    if name.len() > MAX_KEY_SIZE {
        return Err(Error::InvalidName(format!(
            "name exceeds {} bytes: {}",
            MAX_KEY_SIZE,
            name.len()
        )));
    }
    if name.contains('.') {
        return Err(Error::InvalidName("name must not contain dots".into()));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidName(
            "name must not contain control characters".into(),
        ));
    }
    Ok(())
}

/// Validate a primary key. No control characters, non-empty, max 511 bytes.
/// Dots ARE allowed in keys.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key must not be empty".into()));
    }
    if key.len() > MAX_KEY_SIZE {
        return Err(Error::InvalidKey(format!(
            "key exceeds {} bytes: {}",
            MAX_KEY_SIZE,
            key.len()
        )));
    }
    if key.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidKey(
            "key must not contain control characters".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_names() {
        assert!(validate_name("person").is_ok());
        assert!(validate_name("my-table").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn invalid_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("has.dot").is_err());
        assert!(validate_name("has\nnewline").is_err());
        let long = "a".repeat(512);
        assert!(validate_name(&long).is_err());
    }

    #[test]
    fn valid_keys() {
        assert!(validate_key("alice").is_ok());
        assert!(validate_key("has.dot.ok").is_ok());
        assert!(validate_key("a").is_ok());
    }

    #[test]
    fn invalid_keys() {
        assert!(validate_key("").is_err());
        assert!(validate_key("has\x00null").is_err());
        let long = "a".repeat(512);
        assert!(validate_key(&long).is_err());
    }
}
