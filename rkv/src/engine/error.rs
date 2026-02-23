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

    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub type Result<T> = std::result::Result<T, Error>;
