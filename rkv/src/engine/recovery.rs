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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Default ---

    #[test]
    fn default_is_clean() {
        let report = RecoveryReport::default();
        assert!(report.is_clean());
        assert_eq!(report.total_corrupted(), 0);
        assert!(!report.has_data_loss());
    }

    #[test]
    fn default_counters_are_zero() {
        let report = RecoveryReport::default();
        assert_eq!(report.wal_records_scanned, 0);
        assert_eq!(report.wal_records_skipped, 0);
        assert_eq!(report.sstable_blocks_scanned, 0);
        assert_eq!(report.sstable_blocks_corrupted, 0);
        assert_eq!(report.objects_scanned, 0);
        assert_eq!(report.objects_corrupted, 0);
        assert_eq!(report.keys_recovered, 0);
        assert_eq!(report.keys_lost, 0);
        assert!(report.warnings.is_empty());
    }

    // --- is_clean ---

    #[test]
    fn is_clean_with_wal_corruption() {
        let report = RecoveryReport {
            wal_records_skipped: 1,
            ..Default::default()
        };
        assert!(!report.is_clean());
    }

    #[test]
    fn is_clean_with_sstable_corruption() {
        let report = RecoveryReport {
            sstable_blocks_corrupted: 1,
            ..Default::default()
        };
        assert!(!report.is_clean());
    }

    #[test]
    fn is_clean_with_object_corruption() {
        let report = RecoveryReport {
            objects_corrupted: 1,
            ..Default::default()
        };
        assert!(!report.is_clean());
    }

    #[test]
    fn is_clean_ignores_scanned_counters() {
        let report = RecoveryReport {
            wal_records_scanned: 100,
            sstable_blocks_scanned: 200,
            objects_scanned: 50,
            ..Default::default()
        };
        assert!(report.is_clean());
    }

    // --- total_corrupted ---

    #[test]
    fn total_corrupted_sums_all_corruption() {
        let report = RecoveryReport {
            wal_records_skipped: 3,
            sstable_blocks_corrupted: 5,
            objects_corrupted: 2,
            ..Default::default()
        };
        assert_eq!(report.total_corrupted(), 10);
    }

    #[test]
    fn total_corrupted_excludes_recovery_counts() {
        let report = RecoveryReport {
            keys_recovered: 100,
            keys_lost: 50,
            ..Default::default()
        };
        assert_eq!(report.total_corrupted(), 0);
    }

    // --- has_data_loss ---

    #[test]
    fn has_data_loss_false_when_zero() {
        let report = RecoveryReport::default();
        assert!(!report.has_data_loss());
    }

    #[test]
    fn has_data_loss_true_when_keys_lost() {
        let report = RecoveryReport {
            keys_lost: 1,
            ..Default::default()
        };
        assert!(report.has_data_loss());
    }

    #[test]
    fn has_data_loss_independent_of_recovered() {
        let report = RecoveryReport {
            keys_recovered: 100,
            ..Default::default()
        };
        assert!(!report.has_data_loss());
    }

    // --- Warnings ---

    #[test]
    fn warnings_can_be_added() {
        let report = RecoveryReport {
            warnings: vec!["truncated WAL tail".into()],
            ..Default::default()
        };
        assert_eq!(report.warnings.len(), 1);
        assert!(report.is_clean()); // warnings don't affect clean status
    }

    // --- Clone ---

    #[test]
    fn clone_preserves_state() {
        let report = RecoveryReport {
            wal_records_scanned: 42,
            keys_lost: 3,
            warnings: vec!["test".into()],
            ..Default::default()
        };

        let cloned = report.clone();
        assert_eq!(cloned.wal_records_scanned, 42);
        assert_eq!(cloned.keys_lost, 3);
        assert_eq!(cloned.warnings, vec!["test"]);
    }

    // --- Debug ---

    #[test]
    fn debug_format() {
        let report = RecoveryReport::default();
        let debug = format!("{report:?}");
        assert!(debug.contains("RecoveryReport"));
    }
}
