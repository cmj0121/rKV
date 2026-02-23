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

impl RecoveryReport {
    /// Returns `true` if no corruption was detected.
    pub fn is_clean(&self) -> bool {
        self.wal_records_skipped == 0
            && self.sstable_blocks_corrupted == 0
            && self.objects_corrupted == 0
    }

    /// Total number of corrupted entries across all sources.
    pub fn total_corrupted(&self) -> u64 {
        self.wal_records_skipped + self.sstable_blocks_corrupted + self.objects_corrupted
    }

    /// Returns `true` if any keys were permanently lost.
    pub fn has_data_loss(&self) -> bool {
        self.keys_lost > 0
    }
}
