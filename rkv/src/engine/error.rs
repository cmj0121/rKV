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
