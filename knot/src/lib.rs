//! Knot — schema-free, graph-based, temporal database built on rKV.

mod engine;

pub use engine::error::{Error, Result};
pub use engine::Knot;
