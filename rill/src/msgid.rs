//! ULID-style message ID generator.
//!
//! Layout (128 bits): `[timestamp 48 | random 16 | sequence 64]`
//!
//! - Timestamp: ms since Unix epoch — sorts by time across milliseconds
//! - Random: 16-bit random value — uniqueness across instances
//! - Sequence: monotonic counter — preserves insertion order within same ms
//!
//! Encoded as 26-char zero-padded Crockford Base32 — lexicographically sortable.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fast32::base32::CROCKFORD_LOWER;

/// Generates ULID-style message IDs with guaranteed monotonic ordering.
///
/// Thread-safe: uses atomic sequence counter. IDs within the same
/// millisecond are ordered by their sequence number.
pub struct MsgIdGen {
    random_bits: u16,
    cached_ts: AtomicU64,
    sequence: AtomicU64,
}

impl MsgIdGen {
    pub fn new() -> Self {
        let ts = now_ms();
        Self {
            random_bits: fastrand::u16(..),
            cached_ts: AtomicU64::new(ts),
            sequence: AtomicU64::new(0),
        }
    }

    /// Generate a new ULID-style message ID.
    ///
    /// IDs are guaranteed to be monotonically increasing (sortable by
    /// insertion order) and unique within a single `MsgIdGen` instance.
    pub fn generate(&self) -> String {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let ts = self.refresh_ts(seq);

        let raw: u128 = ((ts as u128 & 0xFFFF_FFFF_FFFF) << 80)
            | ((self.random_bits as u128) << 64)
            | (seq as u128);

        // Zero-pad to 26 chars for consistent length and correct lexicographic sort
        format!("{:0>26}", CROCKFORD_LOWER.encode_u128(raw))
    }

    /// Generate a single ULID without keeping a generator around.
    pub fn one() -> String {
        Self::new().generate()
    }

    fn refresh_ts(&self, seq: u64) -> u64 {
        // Refresh timestamp periodically (every 4096 ops)
        if seq.is_multiple_of(4096) {
            let now = now_ms();
            self.cached_ts.store(now, Ordering::Relaxed);
            now
        } else {
            self.cached_ts.load(Ordering::Relaxed)
        }
    }
}

impl Default for MsgIdGen {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_26_char_string() {
        let gen = MsgIdGen::new();
        let id = gen.generate();
        assert_eq!(id.len(), 26, "ID was: {id}");
    }

    #[test]
    fn ids_are_unique() {
        let gen = MsgIdGen::new();
        let ids: Vec<String> = (0..100).map(|_| gen.generate()).collect();
        let mut deduped = ids.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(ids.len(), deduped.len());
    }

    #[test]
    fn ids_are_monotonically_increasing() {
        let gen = MsgIdGen::new();
        let ids: Vec<String> = (0..100).map(|_| gen.generate()).collect();
        for window in ids.windows(2) {
            assert!(
                window[0] < window[1],
                "not monotonic: {:?} >= {:?}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn ids_sort_by_time() {
        // Manually construct IDs with different timestamps to verify sorting
        let ts1: u128 = 1000;
        let ts2: u128 = 2000;
        let raw1 = (ts1 << 80) | 42;
        let raw2 = (ts2 << 80) | 42;
        let id1 = format!("{:0>26}", CROCKFORD_LOWER.encode_u128(raw1));
        let id2 = format!("{:0>26}", CROCKFORD_LOWER.encode_u128(raw2));
        assert!(id1 < id2, "id1={id1} should be < id2={id2}");
    }

    #[test]
    fn zero_pads_short_values() {
        // A very small u128 should still produce a 26-char string
        let encoded = format!("{:0>26}", CROCKFORD_LOWER.encode_u128(1));
        assert_eq!(encoded.len(), 26);
        assert!(encoded.starts_with('0'));
    }

    #[test]
    fn concurrent_ids_are_unique() {
        use std::sync::Arc;
        let gen = Arc::new(MsgIdGen::new());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let g = Arc::clone(&gen);
            handles.push(std::thread::spawn(move || {
                (0..1000).map(|_| g.generate()).collect::<Vec<_>>()
            }));
        }
        let mut all_ids: Vec<String> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let total = all_ids.len();
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(
            total,
            all_ids.len(),
            "duplicate IDs found under concurrency"
        );
    }
}
