use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct LevelStat {
    pub file_count: u64,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    // Storage
    pub total_keys: u64,
    pub data_size_bytes: u64,
    pub namespace_count: u64,
    // LSM internals
    pub level_count: usize,
    pub sstable_count: u64,
    pub write_buffer_bytes: u64,
    pub pending_compactions: u64,
    /// Per-level breakdown: `level_stats[i]` = stats for level `i`.
    pub level_stats: Vec<LevelStat>,
    // Operation counters
    pub op_puts: u64,
    pub op_gets: u64,
    pub op_deletes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    // Uptime
    pub uptime: Duration,
    // Replication
    pub role: String,
}
