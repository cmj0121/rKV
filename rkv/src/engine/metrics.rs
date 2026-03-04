use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// --- Histogram ---

/// Fixed-bucket latency histogram using atomic counters.
///
/// Bucket boundaries are pre-defined for latency tracking (in seconds).
/// Thread-safe: all operations use `Ordering::Relaxed` atomics.
const BUCKET_BOUNDS: [f64; 14] = [
    0.000_01, // 10 µs
    0.000_05, // 50 µs
    0.000_1,  // 100 µs
    0.000_5,  // 500 µs
    0.001,    // 1 ms
    0.005,    // 5 ms
    0.01,     // 10 ms
    0.05,     // 50 ms
    0.1,      // 100 ms
    0.5,      // 500 ms
    1.0,      // 1 s
    5.0,      // 5 s
    10.0,     // 10 s
    60.0,     // 60 s
];

pub(crate) struct Histogram {
    /// Cumulative count per bucket (bucket[i] = observations <= BUCKET_BOUNDS[i]).
    buckets: [AtomicU64; 14],
    /// Total observations.
    count: AtomicU64,
    /// Sum of all observed values, stored as nanoseconds (u64).
    sum_nanos: AtomicU64,
}

impl Histogram {
    pub(crate) fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    /// Record a duration observation.
    pub(crate) fn observe(&self, seconds: f64) {
        let nanos = (seconds * 1e9) as u64;
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            if seconds <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Falls above all buckets — counted only in count/sum (+Inf bucket)
    }

    /// Snapshot of histogram state for rendering.
    pub(crate) fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [(0.0, 0u64); 14];
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            buckets[i] = (*bound, self.buckets[i].load(Ordering::Relaxed));
        }
        HistogramSnapshot {
            buckets,
            count: self.count.load(Ordering::Relaxed),
            sum_nanos: self.sum_nanos.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct HistogramSnapshot {
    pub(crate) buckets: [(f64, u64); 14],
    pub(crate) count: u64,
    pub(crate) sum_nanos: u64,
}

impl HistogramSnapshot {
    pub(crate) fn sum_seconds(&self) -> f64 {
        self.sum_nanos as f64 / 1e9
    }
}

// --- Timer guard ---

/// RAII timer that records elapsed time to a histogram on drop.
pub(crate) struct Timer<'a> {
    histogram: &'a Histogram,
    start: Instant,
}

impl<'a> Timer<'a> {
    pub(crate) fn start(histogram: &'a Histogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.histogram.observe(elapsed.as_secs_f64());
    }
}

// --- Metrics ---

/// Central metrics registry for the database engine.
///
/// All fields are atomic — safe to share via `Arc<Metrics>` across threads.
pub(crate) struct Metrics {
    // Operation latency histograms
    pub(crate) op_put: Histogram,
    pub(crate) op_get: Histogram,
    pub(crate) op_delete: Histogram,
    pub(crate) op_scan: Histogram,

    // Maintenance latency histograms
    pub(crate) flush: Histogram,
    pub(crate) compaction: Histogram,

    // Maintenance counters
    pub(crate) flush_total: AtomicU64,
    pub(crate) compaction_total: AtomicU64,
    pub(crate) bytes_flushed: AtomicU64,
    pub(crate) bytes_compacted: AtomicU64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            op_put: Histogram::new(),
            op_get: Histogram::new(),
            op_delete: Histogram::new(),
            op_scan: Histogram::new(),
            flush: Histogram::new(),
            compaction: Histogram::new(),
            flush_total: AtomicU64::new(0),
            compaction_total: AtomicU64::new(0),
            bytes_flushed: AtomicU64::new(0),
            bytes_compacted: AtomicU64::new(0),
        }
    }
}

// --- EventListener ---

/// Event data for a completed flush operation.
pub struct FlushEvent {
    /// Namespace that was flushed.
    pub namespace: String,
    /// Number of entries written to the SSTable.
    pub entries: u64,
    /// Size of the SSTable file in bytes.
    pub bytes: u64,
    /// Duration of the flush operation.
    pub duration: std::time::Duration,
}

/// Event data for a completed compaction operation.
pub struct CompactionEvent {
    /// Namespace that was compacted.
    pub namespace: String,
    /// Source level in the LSM tree.
    pub source_level: usize,
    /// Target level in the LSM tree.
    pub target_level: usize,
    /// Size of the output SSTable in bytes.
    pub bytes: u64,
    /// Duration of the compaction operation.
    pub duration: std::time::Duration,
}

/// Callback interface for database lifecycle events.
///
/// Implement this trait to receive notifications about flush and compaction
/// operations. All methods have default no-op implementations.
///
/// # Example
///
/// ```rust,ignore
/// struct LoggingListener;
/// impl EventListener for LoggingListener {
///     fn on_flush_complete(&self, event: FlushEvent) {
///         println!("flushed {} entries in {:?}", event.entries, event.duration);
///     }
/// }
/// ```
pub trait EventListener: Send + Sync {
    /// Called after a namespace flush completes.
    fn on_flush_complete(&self, _event: FlushEvent) {}
    /// Called after a compaction merge completes.
    fn on_compaction_complete(&self, _event: CompactionEvent) {}
}

// --- Prometheus text format ---

/// Render all metrics in Prometheus exposition text format.
pub(crate) fn render_prometheus(stats: &super::Stats, metrics: &Metrics) -> String {
    let mut out = String::with_capacity(4096);

    // Operation counters
    out.push_str("# HELP rkv_ops_total Total database operations.\n");
    out.push_str("# TYPE rkv_ops_total counter\n");
    prom_counter_label(&mut out, "rkv_ops_total", "op", "put", stats.op_puts);
    prom_counter_label(&mut out, "rkv_ops_total", "op", "get", stats.op_gets);
    prom_counter_label(&mut out, "rkv_ops_total", "op", "delete", stats.op_deletes);

    // Cache
    out.push_str("# HELP rkv_cache_total Block cache operations.\n");
    out.push_str("# TYPE rkv_cache_total counter\n");
    prom_counter_label(
        &mut out,
        "rkv_cache_total",
        "result",
        "hit",
        stats.cache_hits,
    );
    prom_counter_label(
        &mut out,
        "rkv_cache_total",
        "result",
        "miss",
        stats.cache_misses,
    );

    // Flush / compaction counters
    out.push_str("# HELP rkv_flush_total Total flush operations.\n");
    out.push_str("# TYPE rkv_flush_total counter\n");
    prom_counter(
        &mut out,
        "rkv_flush_total",
        metrics.flush_total.load(Ordering::Relaxed),
    );
    out.push_str("# HELP rkv_compaction_total Total compaction operations.\n");
    out.push_str("# TYPE rkv_compaction_total counter\n");
    prom_counter(
        &mut out,
        "rkv_compaction_total",
        metrics.compaction_total.load(Ordering::Relaxed),
    );

    // Bytes flushed / compacted
    out.push_str("# HELP rkv_bytes_flushed_total Total bytes flushed to SSTables.\n");
    out.push_str("# TYPE rkv_bytes_flushed_total counter\n");
    prom_counter(
        &mut out,
        "rkv_bytes_flushed_total",
        metrics.bytes_flushed.load(Ordering::Relaxed),
    );
    out.push_str("# HELP rkv_bytes_compacted_total Total bytes written by compaction.\n");
    out.push_str("# TYPE rkv_bytes_compacted_total counter\n");
    prom_counter(
        &mut out,
        "rkv_bytes_compacted_total",
        metrics.bytes_compacted.load(Ordering::Relaxed),
    );

    // Replication
    out.push_str("# HELP rkv_conflicts_resolved_total Replication conflicts resolved (LWW).\n");
    out.push_str("# TYPE rkv_conflicts_resolved_total counter\n");
    prom_counter(
        &mut out,
        "rkv_conflicts_resolved_total",
        stats.conflicts_resolved,
    );

    // Gauges
    out.push_str("# HELP rkv_keys Current total keys in memtables.\n");
    out.push_str("# TYPE rkv_keys gauge\n");
    prom_gauge(&mut out, "rkv_keys", stats.total_keys);
    out.push_str("# HELP rkv_data_size_bytes Current data size on disk.\n");
    out.push_str("# TYPE rkv_data_size_bytes gauge\n");
    prom_gauge(&mut out, "rkv_data_size_bytes", stats.data_size_bytes);
    out.push_str("# HELP rkv_namespaces Current namespace count.\n");
    out.push_str("# TYPE rkv_namespaces gauge\n");
    prom_gauge(&mut out, "rkv_namespaces", stats.namespace_count);
    out.push_str("# HELP rkv_sstables Current SSTable count.\n");
    out.push_str("# TYPE rkv_sstables gauge\n");
    prom_gauge(&mut out, "rkv_sstables", stats.sstable_count);
    out.push_str("# HELP rkv_write_buffer_bytes Current write buffer memory usage.\n");
    out.push_str("# TYPE rkv_write_buffer_bytes gauge\n");
    prom_gauge(&mut out, "rkv_write_buffer_bytes", stats.write_buffer_bytes);
    out.push_str("# HELP rkv_pending_compactions Namespaces needing compaction.\n");
    out.push_str("# TYPE rkv_pending_compactions gauge\n");
    prom_gauge(
        &mut out,
        "rkv_pending_compactions",
        stats.pending_compactions,
    );
    out.push_str("# HELP rkv_uptime_seconds Time since database opened.\n");
    out.push_str("# TYPE rkv_uptime_seconds gauge\n");
    prom_gauge_f64(&mut out, "rkv_uptime_seconds", stats.uptime.as_secs_f64());

    // Per-level gauges
    out.push_str("# HELP rkv_level_files SSTable files per level.\n");
    out.push_str("# TYPE rkv_level_files gauge\n");
    for (i, l) in stats.level_stats.iter().enumerate() {
        prom_gauge_label(
            &mut out,
            "rkv_level_files",
            "level",
            &i.to_string(),
            l.file_count,
        );
    }
    out.push_str("# HELP rkv_level_bytes Bytes per level.\n");
    out.push_str("# TYPE rkv_level_bytes gauge\n");
    for (i, l) in stats.level_stats.iter().enumerate() {
        prom_gauge_label(
            &mut out,
            "rkv_level_bytes",
            "level",
            &i.to_string(),
            l.size_bytes,
        );
    }

    // Operation latency histograms
    out.push_str("# HELP rkv_op_duration_seconds Operation latency.\n");
    out.push_str("# TYPE rkv_op_duration_seconds histogram\n");
    prom_histogram_labeled(
        &mut out,
        "rkv_op_duration_seconds",
        "op",
        "put",
        &metrics.op_put,
    );
    prom_histogram_labeled(
        &mut out,
        "rkv_op_duration_seconds",
        "op",
        "get",
        &metrics.op_get,
    );
    prom_histogram_labeled(
        &mut out,
        "rkv_op_duration_seconds",
        "op",
        "delete",
        &metrics.op_delete,
    );
    prom_histogram_labeled(
        &mut out,
        "rkv_op_duration_seconds",
        "op",
        "scan",
        &metrics.op_scan,
    );

    // Maintenance latency histograms
    out.push_str("# HELP rkv_flush_duration_seconds Flush operation latency.\n");
    out.push_str("# TYPE rkv_flush_duration_seconds histogram\n");
    prom_histogram(&mut out, "rkv_flush_duration_seconds", &metrics.flush);
    out.push_str("# HELP rkv_compaction_duration_seconds Compaction operation latency.\n");
    out.push_str("# TYPE rkv_compaction_duration_seconds histogram\n");
    prom_histogram(
        &mut out,
        "rkv_compaction_duration_seconds",
        &metrics.compaction,
    );

    out
}

// --- Prometheus format helpers ---

fn prom_counter(out: &mut String, name: &str, value: u64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn prom_counter_label(out: &mut String, name: &str, label: &str, lval: &str, value: u64) {
    out.push_str(name);
    out.push('{');
    out.push_str(label);
    out.push_str("=\"");
    out.push_str(lval);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn prom_gauge(out: &mut String, name: &str, value: u64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn prom_gauge_f64(out: &mut String, name: &str, value: f64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&format!("{value:.3}"));
    out.push('\n');
}

fn prom_gauge_label(out: &mut String, name: &str, label: &str, lval: &str, value: u64) {
    out.push_str(name);
    out.push('{');
    out.push_str(label);
    out.push_str("=\"");
    out.push_str(lval);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn prom_histogram(out: &mut String, name: &str, hist: &Histogram) {
    let snap = hist.snapshot();
    let mut cumulative = 0u64;
    for (bound, count) in &snap.buckets {
        cumulative += count;
        out.push_str(name);
        out.push_str("_bucket{le=\"");
        out.push_str(&format_le(*bound));
        out.push_str("\"} ");
        out.push_str(&cumulative.to_string());
        out.push('\n');
    }
    // +Inf bucket
    out.push_str(name);
    out.push_str("_bucket{le=\"+Inf\"} ");
    out.push_str(&snap.count.to_string());
    out.push('\n');
    // sum
    out.push_str(name);
    out.push_str("_sum ");
    out.push_str(&format!("{:.6}", snap.sum_seconds()));
    out.push('\n');
    // count
    out.push_str(name);
    out.push_str("_count ");
    out.push_str(&snap.count.to_string());
    out.push('\n');
}

fn prom_histogram_labeled(out: &mut String, name: &str, label: &str, lval: &str, hist: &Histogram) {
    let snap = hist.snapshot();
    let mut cumulative = 0u64;
    for (bound, count) in &snap.buckets {
        cumulative += count;
        out.push_str(name);
        out.push_str("_bucket{");
        out.push_str(label);
        out.push_str("=\"");
        out.push_str(lval);
        out.push_str("\",le=\"");
        out.push_str(&format_le(*bound));
        out.push_str("\"} ");
        out.push_str(&cumulative.to_string());
        out.push('\n');
    }
    // +Inf
    out.push_str(name);
    out.push_str("_bucket{");
    out.push_str(label);
    out.push_str("=\"");
    out.push_str(lval);
    out.push_str("\",le=\"+Inf\"} ");
    out.push_str(&snap.count.to_string());
    out.push('\n');
    // sum
    out.push_str(name);
    out.push_str("_sum{");
    out.push_str(label);
    out.push_str("=\"");
    out.push_str(lval);
    out.push_str("\"} ");
    out.push_str(&format!("{:.6}", snap.sum_seconds()));
    out.push('\n');
    // count
    out.push_str(name);
    out.push_str("_count{");
    out.push_str(label);
    out.push_str("=\"");
    out.push_str(lval);
    out.push_str("\"} ");
    out.push_str(&snap.count.to_string());
    out.push('\n');
}

fn format_le(bound: f64) -> String {
    if bound >= 1.0 {
        format!("{bound}")
    } else if bound >= 0.001 {
        format!("{bound:.3}")
    } else {
        format!("{bound:.6}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_observe_and_snapshot() {
        let h = Histogram::new();
        h.observe(0.0005); // 500 µs → bucket[3] (≤0.0005)
        h.observe(0.002); // 2 ms → bucket[4] (≤0.005)
        h.observe(0.5); // 500 ms → bucket[9] (≤0.5)
        h.observe(100.0); // 100 s → no bucket (only in +Inf)

        let snap = h.snapshot();
        assert_eq!(snap.count, 4);
        assert!(snap.sum_seconds() > 100.0);

        // Bucket 3 (≤0.0005) should have 1 observation
        assert_eq!(snap.buckets[3].1, 1);
        // Bucket 5 (≤0.005) should have 1 observation (0.002 > 0.001=bucket[4])
        assert_eq!(snap.buckets[5].1, 1);
        // Bucket 9 (≤0.5) should have 1 observation
        assert_eq!(snap.buckets[9].1, 1);
    }

    #[test]
    fn histogram_empty_snapshot() {
        let h = Histogram::new();
        let snap = h.snapshot();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.sum_nanos, 0);
        for (_, count) in &snap.buckets {
            assert_eq!(*count, 0);
        }
    }

    #[test]
    fn timer_records_duration() {
        let h = Histogram::new();
        {
            let _t = Timer::start(&h);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let snap = h.snapshot();
        assert_eq!(snap.count, 1);
        assert!(snap.sum_seconds() >= 0.001);
    }

    #[test]
    fn prometheus_format_includes_all_sections() {
        let stats = super::super::Stats::default();
        let metrics = Metrics::new();
        metrics.op_put.observe(0.001);
        metrics.flush_total.fetch_add(1, Ordering::Relaxed);

        let output = render_prometheus(&stats, &metrics);

        // Counters
        assert!(output.contains("rkv_ops_total{op=\"put\"}"));
        assert!(output.contains("rkv_ops_total{op=\"get\"}"));
        assert!(output.contains("rkv_ops_total{op=\"delete\"}"));
        assert!(output.contains("rkv_cache_total{result=\"hit\"}"));
        assert!(output.contains("rkv_flush_total 1"));
        assert!(output.contains("rkv_compaction_total 0"));

        // Gauges
        assert!(output.contains("rkv_keys 0"));
        assert!(output.contains("rkv_uptime_seconds"));

        // Histograms
        assert!(output.contains("rkv_op_duration_seconds_bucket{op=\"put\""));
        assert!(output.contains("rkv_op_duration_seconds_sum{op=\"put\"}"));
        assert!(output.contains("rkv_op_duration_seconds_count{op=\"put\"} 1"));
        assert!(output.contains("rkv_flush_duration_seconds_bucket"));

        // TYPE declarations
        assert!(output.contains("# TYPE rkv_ops_total counter"));
        assert!(output.contains("# TYPE rkv_keys gauge"));
        assert!(output.contains("# TYPE rkv_op_duration_seconds histogram"));
    }

    #[test]
    fn format_le_outputs() {
        assert_eq!(format_le(0.00001), "0.000010");
        assert_eq!(format_le(0.001), "0.001");
        assert_eq!(format_le(1.0), "1");
        assert_eq!(format_le(60.0), "60");
    }
}
