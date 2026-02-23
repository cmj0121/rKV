/// Report produced by `DB::repair()` describing what was found and fixed.
///
/// After an offline repair, callers inspect the report to determine whether
/// the database is clean, whether any data was lost, and what warnings were
/// generated during recovery.
#[derive(Clone, Debug, Default)]
pub struct RecoveryReport {
    /// WAL records scanned during repair.
    pub wal_records_scanned: u64,
    /// WAL records skipped due to corruption.
    pub wal_records_skipped: u64,
    /// SSTable blocks scanned during repair.
    pub sstable_blocks_scanned: u64,
    /// SSTable blocks found to be corrupted.
    pub sstable_blocks_corrupted: u64,
    /// Bin objects scanned during repair.
    pub objects_scanned: u64,
    /// Bin objects found to be corrupted.
    pub objects_corrupted: u64,
    /// Keys successfully recovered from redundant sources.
    pub keys_recovered: u64,
    /// Keys permanently lost (no redundant copy available).
    pub keys_lost: u64,
    /// Human-readable warnings generated during repair.
    pub warnings: Vec<String>,
}
