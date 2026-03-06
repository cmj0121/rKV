use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("key not found")]
    KeyNotFound,

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("encryption required: {0}")]
    EncryptionRequired(String),

    #[error("namespace is not encrypted: {0}")]
    NotEncrypted(String),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("read-only replica: writes are rejected")]
    ReadOnlyReplica,

    #[error("namespace '{0}' is owned by shard group {1}")]
    NotMyShard(String, u16),

    #[error("cluster error: {0}")]
    ClusterError(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Safely convert a byte slice to a fixed-size array, returning `Corruption`
/// on length mismatch. Use this instead of `.try_into().unwrap()` when parsing
/// binary data from disk or network.
#[inline]
pub(crate) fn bytes_to_array<const N: usize>(data: &[u8], context: &str) -> Result<[u8; N]> {
    data.try_into().map_err(|_| {
        Error::Corruption(format!("{context}: expected {N} bytes, got {}", data.len()))
    })
}
