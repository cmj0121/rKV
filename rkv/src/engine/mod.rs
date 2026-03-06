mod aol;
mod batch;
mod bloom;
mod cache;
mod checksum;
mod cluster;
pub(crate) mod crypto;
mod dump;
mod error;
mod io;
mod key;
mod memtable;
mod merge_iter;
pub(crate) mod metrics;
mod namespace;
mod objects;
mod recovery;
mod repl_peer;
mod repl_receiver;
mod repl_sender;
pub(crate) mod replication;
mod revision;
mod sstable;
mod stats;
mod value;

pub use batch::{BatchOp, WriteBatch};
pub use cluster::{NodeInfo, RoutingTable, ShardGroup};
pub use error::{Error, Result};
pub use key::Key;
pub use metrics::{CompactionEvent, EventListener, FlushEvent};
pub use namespace::Namespace;
pub use recovery::RecoveryReport;
pub use replication::Role;
pub use revision::RevisionID;
pub use stats::{LevelStat, Stats};
pub use value::Value;

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Current time as epoch milliseconds.
fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Check if an `expires_at_ms` timestamp has passed.
/// Returns `false` for 0 (no expiration).
fn is_expired(expires_at_ms: u64) -> bool {
    expires_at_ms != 0 && now_epoch_ms() >= expires_at_ms
}

/// Default namespace name.
pub const DEFAULT_NAMESPACE: &str = "_";

/// I/O model for file access.
///
/// Controls how the engine reads and writes data files. The three modes are
/// mutually exclusive.
#[derive(Clone, Debug, PartialEq)]
pub enum IoModel {
    /// Buffered I/O — relies on the OS page cache.
    None,
    /// Direct I/O — bypasses the OS page cache (O_DIRECT).
    DirectIO,
    /// Memory-mapped I/O — zero-copy reads via mmap (default).
    Mmap,
}

impl Default for IoModel {
    fn default() -> Self {
        Self::Mmap
    }
}

impl fmt::Display for IoModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::DirectIO => write!(f, "directio"),
            Self::Mmap => write!(f, "mmap"),
        }
    }
}

impl FromStr for IoModel {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "directio" => Ok(Self::DirectIO),
            "mmap" => Ok(Self::Mmap),
            _ => Err(Error::InvalidConfig(format!(
                "unknown io_model '{s}' (expected: none, directio, mmap)"
            ))),
        }
    }
}

/// SSTable block compression algorithm.
///
/// Controls how data blocks are compressed when flushed to SSTable files.
/// Bin objects use their own compression setting (`compress`).
#[derive(Clone, Debug, PartialEq)]
pub enum Compression {
    /// No compression — blocks are stored as-is.
    None,
    /// LZ4 block compression — fast with moderate ratio (default).
    LZ4,
    /// Zstandard compression — better ratio, higher CPU cost.
    Zstd,
}

impl Default for Compression {
    fn default() -> Self {
        Self::LZ4
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::LZ4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
        }
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::LZ4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(Error::InvalidConfig(format!(
                "unknown compression '{s}' (expected: none, lz4, zstd)"
            ))),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub path: PathBuf,
    pub create_if_missing: bool,
    /// Write buffer size in bytes (default: 4 MB).
    pub write_buffer_size: usize,
    /// Maximum number of LSM levels (default: 3).
    pub max_levels: usize,
    /// SSTable block size in bytes (default: 4 KB).
    pub block_size: usize,
    /// Block cache size in bytes (default: 8 MB).
    pub cache_size: usize,
    /// Object size threshold in bytes for value separation (default: 1 KB).
    /// Values larger than this are stored as bin objects in the value log;
    /// smaller values stay inline in the LSM-tree.
    pub object_size: usize,
    /// Whether to LZ4-compress bin objects on disk (default: true).
    pub compress: bool,
    /// Bloom filter bits per key (default: 10, ~1% false-positive rate).
    /// Set to 0 to disable bloom filters.
    pub bloom_bits: usize,
    /// Bloom filter prefix length for scan optimization (default: 0 = disabled).
    /// When > 0, the first `bloom_prefix_len` bytes of each key's serialized
    /// form are hashed into a prefix bloom filter per SSTable. Scans check
    /// this filter to skip SSTables that definitely don't contain matching
    /// prefixes.
    pub bloom_prefix_len: usize,
    /// Verify checksums on read (default: true).
    /// When enabled, every WAL entry and SSTable block is verified against
    /// its stored checksum during reads. Disabling trades safety for speed.
    pub verify_checksums: bool,
    /// SSTable block compression algorithm (default: LZ4).
    pub compression: Compression,
    /// I/O model for file access (default: Mmap).
    pub io_model: IoModel,
    /// Cluster ID for RevisionID generation (default: None = random at startup).
    pub cluster_id: Option<u16>,
    /// AOL flush threshold in records (default: 128).
    /// Set to 0 for per-record flush (maximum durability).
    pub aol_buffer_size: usize,
    /// Maximum number of L0 SSTable files before compaction (default: 4).
    pub l0_max_count: usize,
    /// Maximum total L0 size in bytes before compaction (default: 64 MB).
    pub l0_max_size: usize,
    /// Maximum L1 size in bytes before compaction to L2 (default: 256 MB).
    pub l1_max_size: usize,
    /// Default maximum size in bytes for L2+ levels (default: 2 GB).
    pub default_max_size: usize,
    /// Node role in a replication topology (default: Standalone).
    pub role: Role,
    /// Replication listen address (primary only, default: "0.0.0.0").
    pub repl_bind: String,
    /// Replication listen port (primary only, default: 8322).
    pub repl_port: u16,
    /// Primary address to connect to (replica only, e.g. "10.0.0.1:8322").
    pub primary_addr: Option<String>,
    /// Peer addresses for master-master replication (peer only).
    pub peers: Vec<String>,
    /// Write stall threshold in bytes (default: 2 * write_buffer_size).
    /// When a namespace's memtable exceeds this size, `put()` blocks until
    /// the flush thread drains it below `write_buffer_size`.
    /// Set to 0 to disable write stalling.
    pub write_stall_size: usize,
    /// Optional event listener for flush/compaction lifecycle hooks.
    pub event_listener: Option<Arc<dyn metrics::EventListener>>,
    /// Shard group ID for cluster mode (default: 0 = standalone).
    pub shard_group: u16,
    /// Namespaces owned by this node in cluster mode.
    /// Empty means all namespaces are accepted (standalone behavior).
    pub owned_namespaces: Vec<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("path", &self.path)
            .field("write_buffer_size", &self.write_buffer_size)
            .field("max_levels", &self.max_levels)
            .field("block_size", &self.block_size)
            .field("cache_size", &self.cache_size)
            .field("event_listener", &self.event_listener.is_some())
            .finish_non_exhaustive()
    }
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            create_if_missing: true,
            write_buffer_size: 4 * 1024 * 1024,
            max_levels: 3,
            block_size: 4 * 1024,
            cache_size: 8 * 1024 * 1024,
            object_size: 1024,
            compress: true,
            bloom_bits: 10,
            bloom_prefix_len: 0,
            verify_checksums: true,
            compression: Compression::default(),
            io_model: IoModel::default(),
            cluster_id: None,
            aol_buffer_size: 128,
            l0_max_count: 4,
            l0_max_size: 64 * 1024 * 1024,
            l1_max_size: 256 * 1024 * 1024,
            default_max_size: 2 * 1024 * 1024 * 1024,
            role: Role::default(),
            repl_bind: "0.0.0.0".to_owned(),
            repl_port: 8322,
            primary_addr: None,
            peers: Vec::new(),
            write_stall_size: 8 * 1024 * 1024,
            event_listener: None,
            shard_group: 0,
            owned_namespaces: Vec::new(),
        }
    }
}

/// Per-namespace, per-level SSTable readers.
type LeveledSSTables = HashMap<String, Vec<Vec<sstable::SSTableReader>>>;

/// Stats metadata file name within the DB directory.
const STATS_META: &str = "stats.meta";
/// Magic bytes for the stats metadata file.
const STATS_MAGIC: &[u8; 4] = b"rKVT";
/// Current stats metadata format version.
const STATS_VERSION: u16 = 1;

pub struct DB {
    config: Config,
    opened_at: Instant,
    encrypted_namespaces: Mutex<HashMap<String, bool>>,
    io_backend: Arc<dyn io::IoBackend>,
    revision_gen: revision::RevisionGen,
    namespace_data: Arc<RwLock<HashMap<String, Mutex<memtable::MemTable>>>>,
    aol: Arc<Mutex<aol::Aol>>,
    object_stores: Arc<RwLock<HashMap<String, objects::ObjectStore>>>,
    /// Per-namespace, per-level SSTable readers.
    /// `sstables[ns][level]` = Vec of readers.
    /// Level 0: newest-first (overlapping key ranges).
    /// Level 1+: key-order (non-overlapping key ranges after compaction).
    sstables: Arc<RwLock<LeveledSSTables>>,
    /// Monotonically increasing counter for SSTable file naming.
    sst_sequence: Arc<AtomicU64>,
    /// Shared LRU block cache for decompressed SSTable blocks.
    block_cache: Option<Arc<Mutex<cache::BlockCache>>>,
    flush_stop: Arc<AtomicBool>,
    flush_thread: Option<JoinHandle<()>>,
    compaction_stop: Arc<AtomicBool>,
    compaction_notify: Arc<(Mutex<bool>, Condvar)>,
    compaction_mutex: Arc<Mutex<()>>,
    compaction_done: Arc<(Mutex<bool>, Condvar)>,
    compaction_thread: Option<JoinHandle<()>>,
    // Operation counters (persistent across restarts)
    op_puts: AtomicU64,
    op_gets: AtomicU64,
    op_deletes: AtomicU64,
    // Metrics (latency histograms, maintenance counters)
    metrics: Arc<metrics::Metrics>,
    event_listener: Option<Arc<dyn metrics::EventListener>>,
    // Replication
    repl_sender: Option<repl_sender::ReplSender>,
    repl_receiver: Option<repl_receiver::ReplReceiver>,
    /// Set to trigger a force-sync on the replica receiver thread.
    repl_force_sync: Option<Arc<AtomicBool>>,
    /// Counter for LWW conflict resolutions (peer replication).
    conflicts_resolved: Arc<AtomicU64>,
    // Peer replication
    peer_sessions: Arc<Mutex<Vec<repl_peer::PeerSession>>>,
    peer_listener: Option<repl_peer::PeerListener>,
    peer_connectors: Vec<repl_peer::PeerConnector>,
    /// Peer last-revision tracker for checkpoint persistence on close.
    peer_last_revision: Option<Arc<Mutex<u128>>>,
}

impl DB {
    pub fn open(config: Config) -> Result<Self> {
        if config.create_if_missing {
            fs::create_dir_all(&config.path)?;
        }
        let io_backend = io::create_backend(&config.io_model);
        let revision_gen = revision::RevisionGen::new(config.cluster_id);

        // Replay AOL to reconstruct memtables
        let namespace_data = Arc::new(RwLock::new(HashMap::new()));
        let (records, _skipped) =
            aol::Aol::replay(&config.path, config.verify_checksums, &*io_backend)?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        {
            let mut map = namespace_data.write().unwrap_or_else(|e| e.into_inner());
            for record in records {
                // Skip expired records — replaying them would create
                // tombstones in the memtable that shadow valid SSTable data.
                if record.expires_at_ms > 0 && record.expires_at_ms <= now_ms {
                    continue;
                }

                // Namespace-creation sentinel: only register the namespace.
                let is_sentinel = record.key == Key::Str(String::new()) && record.value.is_null();

                let mt = map
                    .entry(record.namespace)
                    .or_insert_with(|| Mutex::new(memtable::MemTable::new()));

                if !is_sentinel {
                    let mt = mt.get_mut().unwrap();
                    let rev = RevisionID::from(record.revision);
                    let ttl = if record.expires_at_ms > 0 {
                        // Convert absolute expiry back to remaining duration
                        let remaining_ms = record.expires_at_ms.saturating_sub(now_ms);
                        Some(Duration::from_millis(remaining_ms))
                    } else {
                        None
                    };
                    mt.put(record.key, record.value, rev, ttl);
                }
            }
        }

        // Per-namespace object stores (created lazily on first access)
        let object_stores = Arc::new(RwLock::new(HashMap::new()));

        // Block cache for decompressed SSTable blocks
        let block_cache = if config.cache_size > 0 {
            Some(Arc::new(Mutex::new(cache::BlockCache::new(
                config.cache_size,
            ))))
        } else {
            None
        };

        // Scan existing SSTable files across all levels and recover sequence counter
        let (sstables, sst_sequence) =
            Self::scan_sstables(&config.path, config.max_levels, &block_cache, &*io_backend)?;
        let sstables = Arc::new(RwLock::new(sstables));
        let sst_sequence = Arc::new(AtomicU64::new(sst_sequence));

        // Open AOL for appending
        let aol = Arc::new(Mutex::new(aol::Aol::open(
            &config.path,
            config.aol_buffer_size,
        )?));

        let flush_stop = Arc::new(AtomicBool::new(false));
        let flush_thread = {
            let aol = Arc::clone(&aol);
            let stop = Arc::clone(&flush_stop);
            Some(thread::spawn(move || {
                let mut tick = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                    tick += 1;
                    if tick >= 60 {
                        tick = 0;
                        let mut aol = aol.lock().unwrap_or_else(|e| e.into_inner());
                        let _ = aol.flush_if_dirty();
                    }
                }
                // Final flush on shutdown
                let mut aol = aol.lock().unwrap_or_else(|e| e.into_inner());
                let _ = aol.flush_if_dirty();
            }))
        };

        // Metrics registry (shared across threads)
        let metrics_arc = Arc::new(metrics::Metrics::new());

        // Background compaction thread
        let compaction_stop = Arc::new(AtomicBool::new(false));
        let compaction_notify = Arc::new((Mutex::new(false), Condvar::new()));
        let compaction_mutex = Arc::new(Mutex::new(()));
        let compaction_done = Arc::new((Mutex::new(false), Condvar::new()));
        let compaction_thread = {
            let stop = Arc::clone(&compaction_stop);
            let notify = Arc::clone(&compaction_notify);
            let c_mutex = Arc::clone(&compaction_mutex);
            let done = Arc::clone(&compaction_done);
            let config = config.clone();
            let sstables = Arc::clone(&sstables);
            let sst_sequence = Arc::clone(&sst_sequence);
            let block_cache = block_cache.clone();
            let io = Arc::clone(&io_backend);
            let ns_data = Arc::clone(&namespace_data);
            let bg_metrics = Arc::clone(&metrics_arc);
            let bg_listener = config.event_listener.clone();

            Some(thread::spawn(move || {
                loop {
                    // Wait for notification or 30s safety-net poll
                    {
                        let (lock, cvar) = &*notify;
                        let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
                        if !*pending && !stop.load(Ordering::Relaxed) {
                            let result =
                                cvar.wait_timeout(pending, Duration::from_secs(30)).unwrap();
                            pending = result.0;
                        }
                        *pending = false;
                    }

                    if stop.load(Ordering::Relaxed) {
                        let (lock, cvar) = &*done;
                        let mut d = lock.lock().unwrap_or_else(|e| e.into_inner());
                        *d = true;
                        cvar.notify_all();
                        break;
                    }

                    // Drain loop: keep compacting while any level is over threshold
                    loop {
                        if !DB::check_should_compact(&sstables, &config) {
                            break;
                        }
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let _guard = c_mutex.lock().unwrap_or_else(|e| e.into_inner());
                        // Re-check after acquiring mutex (another thread may have compacted)
                        if !DB::check_should_compact(&sstables, &config) {
                            break;
                        }
                        let _ = DB::do_compact(
                            &config,
                            &sstables,
                            &sst_sequence,
                            &block_cache,
                            &io,
                            &ns_data,
                            &bg_metrics,
                            &bg_listener,
                        );
                    }

                    // Signal wait_for_compaction() callers
                    {
                        let (lock, cvar) = &*done;
                        let mut d = lock.lock().unwrap_or_else(|e| e.into_inner());
                        *d = true;
                        cvar.notify_all();
                    }
                }
            }))
        };

        // Load persisted operation counters
        let (op_puts, op_gets, op_deletes) = Self::load_stats_meta(&config.path);
        let event_listener = config.event_listener.clone();

        let mut db = Self {
            config,
            opened_at: Instant::now(),
            encrypted_namespaces: Mutex::new(HashMap::new()),
            io_backend,
            revision_gen,
            namespace_data,
            aol,
            object_stores,
            sstables,
            sst_sequence,
            block_cache,
            flush_stop,
            flush_thread,
            compaction_stop,
            compaction_notify,
            compaction_mutex,
            compaction_done,
            compaction_thread,
            op_puts: AtomicU64::new(op_puts),
            op_gets: AtomicU64::new(op_gets),
            op_deletes: AtomicU64::new(op_deletes),
            metrics: metrics_arc,
            event_listener,
            repl_sender: None,
            repl_receiver: None,
            repl_force_sync: None,
            conflicts_resolved: Arc::new(AtomicU64::new(0)),
            peer_sessions: Arc::new(Mutex::new(Vec::new())),
            peer_listener: None,
            peer_connectors: Vec::new(),
            peer_last_revision: None,
        };

        // Eagerly register the default namespace so it always appears in
        // list_namespaces(), regardless of whether any data has been written.
        db.get_or_create_memtable(DEFAULT_NAMESPACE);

        db.start_replication()?;

        Ok(db)
    }

    pub fn close(mut self) -> Result<()> {
        self.stop_replication();
        self.flush_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
        self.compaction_stop.store(true, Ordering::Relaxed);
        {
            let (lock, cvar) = &*self.compaction_notify;
            let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
            *pending = true;
            cvar.notify_one();
        }
        if let Some(handle) = self.compaction_thread.take() {
            let _ = handle.join();
        }
        self.save_stats_meta();
        Ok(())
    }

    /// Returns true if this node is configured as a read-only replica.
    pub fn is_replica(&self) -> bool {
        self.config.role == replication::Role::Replica
    }

    /// Returns true if this node is configured as a peer (master-master).
    pub fn is_peer(&self) -> bool {
        self.config.role == replication::Role::Peer
    }

    /// Trigger a force-sync on the replica: wipe local state and perform
    /// a fresh full sync from the primary. No-op if not a replica.
    pub fn force_sync(&self) -> Result<()> {
        if !self.is_replica() {
            return Err(Error::ReadOnlyReplica);
        }
        match self.repl_force_sync {
            Some(ref flag) => {
                flag.store(true, Ordering::Relaxed);
                Ok(())
            }
            None => Err(Error::InvalidConfig("replica receiver not running".into())),
        }
    }

    /// Build a callback that performs a memtable→SSTable flush without
    /// broadcasting `FlushNotify` (to avoid infinite loops when the flush
    /// is triggered by a remote `FlushNotify`).
    fn make_memtable_flush_fn(&self) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
        let ns_data = Arc::clone(&self.namespace_data);
        let sst_seq = Arc::clone(&self.sst_sequence);
        let sstables = Arc::clone(&self.sstables);
        let aol = Arc::clone(&self.aol);
        let io_backend = Arc::clone(&self.io_backend);
        let block_cache = self.block_cache.clone();
        let compaction_notify = Arc::clone(&self.compaction_notify);
        let db_path = self.config.path.clone();
        let block_size = self.config.block_size;
        let compression = self.config.compression.clone();
        let bloom_bits = self.config.bloom_bits;
        let bloom_prefix_len = self.config.bloom_prefix_len;

        Arc::new(move || {
            let namespaces: Vec<String> = {
                let map = ns_data.read().unwrap_or_else(|e| e.into_inner());
                map.keys().cloned().collect()
            };

            let mut flushed_any = false;

            for ns_name in &namespaces {
                let entries = {
                    let map = ns_data.read().unwrap_or_else(|e| e.into_inner());
                    let Some(mt_mutex) = map.get(ns_name) else {
                        continue;
                    };
                    let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                    if mt.is_empty() {
                        continue;
                    }
                    mt.drain_all()
                };

                if entries.is_empty() {
                    continue;
                }

                let seq = sst_seq.fetch_add(1, Ordering::Relaxed) + 1;
                let l0_dir = DB::static_sst_level_dir(&db_path, ns_name, 0);
                fs::create_dir_all(&l0_dir)?;
                let sst_path = l0_dir.join(format!("{seq:06}.sst"));

                let mut writer = sstable::SSTableWriter::new(
                    &sst_path,
                    block_size,
                    compression.clone(),
                    bloom_bits,
                    bloom_prefix_len,
                    &*io_backend,
                )?;
                for (key, value, revision, expires_at_ms) in &entries {
                    writer.add(key, value, *revision, *expires_at_ms)?;
                }
                writer.finish()?;

                let reader = sstable::SSTableReader::open(
                    &sst_path,
                    seq,
                    block_cache.clone(),
                    &*io_backend,
                )?;
                let mut sst = sstables.write().unwrap_or_else(|e| e.into_inner());
                let levels = sst
                    .entry(ns_name.clone())
                    .or_insert_with(|| vec![Vec::new()]);
                if levels.is_empty() {
                    levels.push(Vec::new());
                }
                levels[0].insert(0, reader);

                flushed_any = true;
            }

            if flushed_any {
                let mut aol = aol.lock().unwrap_or_else(|e| e.into_inner());
                aol.truncate(&db_path)?;
            }

            if flushed_any {
                let (lock, cvar) = &*compaction_notify;
                let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
                *pending = true;
                cvar.notify_one();
            }

            Ok(())
        })
    }

    /// Start replication based on the configured role.
    fn start_replication(&mut self) -> Result<()> {
        match self.config.role {
            replication::Role::Primary => {
                let cluster_id = self.revision_gen.cluster_id();
                let db_path = self.config.path.clone();
                let max_levels = self.config.max_levels;
                let stop = Arc::new(AtomicBool::new(false));

                // Build a flush callback for full sync
                let aol_for_flush = Arc::clone(&self.aol);
                let flush_fn = move || {
                    // Minimal flush: just flush AOL buffer so data is on disk
                    let mut aol = aol_for_flush.lock().unwrap_or_else(|e| e.into_inner());
                    aol.flush_if_dirty()?;
                    Ok(())
                };

                let primary_config = repl_sender::PrimaryConfig {
                    db_path,
                    cluster_id,
                    max_levels,
                    io_backend: Arc::clone(&self.io_backend),
                };
                let sender = repl_sender::ReplSender::start(
                    &self.config.repl_bind,
                    self.config.repl_port,
                    primary_config,
                    flush_fn,
                    stop,
                )?;
                self.repl_sender = Some(sender);
            }
            replication::Role::Replica => {
                let addr = self.config.primary_addr.as_deref().ok_or_else(|| {
                    Error::InvalidConfig("primary_addr is required when role is replica".into())
                })?;
                let cluster_id = self.revision_gen.cluster_id();
                let db_path = self.config.path.clone();
                let max_levels = self.config.max_levels;
                let stop = Arc::new(AtomicBool::new(false));

                // Shared revision tracker for incremental sync
                let last_revision = Arc::new(Mutex::new(0u128));
                let rev_tracker = Arc::clone(&last_revision);

                // Build a replay callback that writes to local AOL + memtable
                let aol = Arc::clone(&self.aol);
                let ns_data = Arc::clone(&self.namespace_data);
                let replay_fn: repl_receiver::ReplayFn = Box::new(move |payload: &[u8]| {
                    let record = aol::decode_payload(payload)?;

                    // Track highest revision for incremental sync
                    {
                        let mut lr = rev_tracker.lock().unwrap_or_else(|e| e.into_inner());
                        if record.revision > *lr {
                            *lr = record.revision;
                        }
                    }

                    // Write to local AOL for crash recovery
                    {
                        let mut aol = aol.lock().unwrap_or_else(|e| e.into_inner());
                        aol.append_encoded(payload)?;
                    }

                    // Apply to memtable
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    // Skip expired records — replaying them would create
                    // tombstones in the memtable that shadow valid SSTable data.
                    if record.expires_at_ms > 0 && record.expires_at_ms <= now_ms {
                        return Ok(());
                    }

                    // Namespace-creation sentinel: empty key + Null value.
                    // Only ensures the namespace memtable exists; no data to store.
                    let is_sentinel =
                        record.key == Key::Str(String::new()) && record.value.is_null();

                    let map = ns_data.read().unwrap_or_else(|e| e.into_inner());
                    if let Some(mt_mutex) = map.get(&record.namespace) {
                        if !is_sentinel {
                            let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                            let rev = RevisionID::from(record.revision);
                            let ttl = if record.expires_at_ms > 0 {
                                let remaining_ms = record.expires_at_ms.saturating_sub(now_ms);
                                Some(Duration::from_millis(remaining_ms))
                            } else {
                                None
                            };
                            mt.put(record.key, record.value, rev, ttl);
                        }
                    } else {
                        drop(map);
                        let mut map = ns_data.write().unwrap_or_else(|e| e.into_inner());
                        let mt = map
                            .entry(record.namespace)
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                        if !is_sentinel {
                            let mt = mt.get_mut().unwrap_or_else(|e| e.into_inner());
                            let rev = RevisionID::from(record.revision);
                            let ttl = if record.expires_at_ms > 0 {
                                let remaining_ms = record.expires_at_ms.saturating_sub(now_ms);
                                Some(Duration::from_millis(remaining_ms))
                            } else {
                                None
                            };
                            mt.put(record.key, record.value, rev, ttl);
                        }
                    }

                    Ok(())
                });

                // Build a post-sync callback to reload SSTable index
                let sync_sstables = Arc::clone(&self.sstables);
                let sync_sst_seq = Arc::clone(&self.sst_sequence);
                let sync_db_path = self.config.path.clone();
                let sync_max_levels = self.config.max_levels;
                let sync_cache = self.block_cache.clone();
                let sync_io = Arc::clone(&self.io_backend);
                let sync_ns_data = Arc::clone(&self.namespace_data);
                let sync_aol = Arc::clone(&self.aol);
                let post_sync_fn: repl_receiver::PostSyncFn = Box::new(move || {
                    let (new_sst, new_seq) = Self::scan_sstables(
                        &sync_db_path,
                        sync_max_levels,
                        &sync_cache,
                        sync_io.as_ref(),
                    )?;

                    // Clear stale memtable entries so they don't shadow the
                    // authoritative SSTable snapshot from the primary.
                    //
                    // IMPORTANT: We must NOT call `ns_map.clear()` because
                    // `get_or_create_memtable()` returns raw pointers into the
                    // HashMap under the SAFETY invariant "we only grow, never
                    // shrink". Clearing the map would invalidate those pointers.
                    // Instead, reset each MemTable in-place and add any new
                    // namespace entries.
                    {
                        let mut ns_map = sync_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        // Reset all existing memtables in-place (Mutex stays at same address)
                        for mt_mutex in ns_map.values() {
                            let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                            *mt = memtable::MemTable::new();
                        }
                        // Register new SST namespaces (HashMap only grows)
                        for ns_name in new_sst.keys() {
                            ns_map
                                .entry(ns_name.clone())
                                .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                        }
                        ns_map
                            .entry(DEFAULT_NAMESPACE.to_owned())
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                    }

                    // Truncate local AOL so stale records don't reappear
                    // on restart. Future live-stream records will be appended
                    // to a fresh AOL.
                    {
                        let mut aol = sync_aol.lock().unwrap_or_else(|e| e.into_inner());
                        aol.truncate(&sync_db_path)?;
                    }

                    // Replace SSTable index
                    {
                        let mut sst = sync_sstables.write().unwrap_or_else(|e| e.into_inner());
                        *sst = new_sst;
                    }

                    // Update sequence counter
                    let old_seq = sync_sst_seq.load(Ordering::Relaxed);
                    if new_seq > old_seq {
                        sync_sst_seq.store(new_seq, Ordering::Relaxed);
                    }

                    Ok(())
                });

                // Build a drop-namespace callback for live-stream drops
                let drop_ns_data = Arc::clone(&self.namespace_data);
                let drop_sstables = Arc::clone(&self.sstables);
                let drop_obj_stores = Arc::clone(&self.object_stores);
                let drop_db_path = self.config.path.clone();
                let drop_ns_fn: repl_receiver::DropNsFn = Box::new(move |namespace: &str| {
                    // 1. Remove from in-memory maps (matches primary behavior)
                    {
                        let mut map = drop_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }
                    {
                        let mut map = drop_sstables.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }
                    {
                        let mut map = drop_obj_stores.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }

                    // 2. Delete on-disk files
                    let sst_dir = drop_db_path.join("sst").join(namespace);
                    if sst_dir.exists() {
                        fs::remove_dir_all(&sst_dir)?;
                    }
                    let obj_dir = drop_db_path.join("objects").join(namespace);
                    if obj_dir.exists() {
                        fs::remove_dir_all(&obj_dir)?;
                    }

                    // 3. Re-create default namespace if it was dropped
                    if namespace == DEFAULT_NAMESPACE {
                        let mut map = drop_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        map.entry(DEFAULT_NAMESPACE.to_owned())
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                    }

                    Ok(())
                });

                // Build a cleanup callback for force-sync
                let cleanup_ns_data = Arc::clone(&self.namespace_data);
                let cleanup_sstables = Arc::clone(&self.sstables);
                let cleanup_obj_stores = Arc::clone(&self.object_stores);
                let cleanup_aol = Arc::clone(&self.aol);
                let cleanup_db_path = self.config.path.clone();
                let cleanup_fn: repl_receiver::CleanupFn = Box::new(move || {
                    // Reset all memtables in-place (SAFETY: HashMap only grows)
                    {
                        let map = cleanup_ns_data.read().unwrap_or_else(|e| e.into_inner());
                        for mt_mutex in map.values() {
                            let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                            *mt = memtable::MemTable::new();
                        }
                    }
                    // Clear SSTables
                    {
                        let mut sst = cleanup_sstables.write().unwrap_or_else(|e| e.into_inner());
                        sst.clear();
                    }
                    // Clear object stores
                    {
                        let mut obj = cleanup_obj_stores
                            .write()
                            .unwrap_or_else(|e| e.into_inner());
                        obj.clear();
                    }
                    // Delete on-disk sst/ and objects/ directories
                    let sst_root = cleanup_db_path.join("sst");
                    if sst_root.exists() {
                        fs::remove_dir_all(&sst_root)?;
                    }
                    let obj_root = cleanup_db_path.join("objects");
                    if obj_root.exists() {
                        fs::remove_dir_all(&obj_root)?;
                    }
                    // Truncate AOL
                    {
                        let mut aol = cleanup_aol.lock().unwrap_or_else(|e| e.into_inner());
                        aol.truncate(&cleanup_db_path)?;
                    }
                    Ok(())
                });

                // Build a flush callback for FlushNotify messages.
                // Uses make_memtable_flush_fn to avoid re-broadcasting.
                let flush_fn: repl_receiver::FlushFn = self.make_memtable_flush_fn();

                let force_sync = Arc::new(AtomicBool::new(false));
                self.repl_force_sync = Some(Arc::clone(&force_sync));

                let callbacks = repl_receiver::ReplicaCallbacks {
                    replay_fn,
                    post_sync_fn,
                    drop_ns_fn,
                    cleanup_fn,
                    flush_fn,
                    last_revision,
                    force_sync,
                };
                let receiver = repl_receiver::ReplReceiver::start(
                    addr, cluster_id, db_path, max_levels, callbacks, stop,
                )?;
                self.repl_receiver = Some(receiver);
            }
            replication::Role::Peer => {
                if self.config.peers.is_empty() {
                    return Err(Error::InvalidConfig(
                        "peers list is required when role is peer".into(),
                    ));
                }
                let cluster_id = self.revision_gen.cluster_id();
                let db_path = self.config.path.clone();
                let max_levels = self.config.max_levels;
                let stop = Arc::new(AtomicBool::new(false));

                // Shared revision tracker for incremental sync
                let last_revision = Arc::new(Mutex::new(0u128));
                let rev_tracker = Arc::clone(&last_revision);

                // Build a peer replay callback using LWW
                let db_ns_data = Arc::clone(&self.namespace_data);
                let db_aol = Arc::clone(&self.aol);
                let db_rev_gen_cluster = self.revision_gen.cluster_id();
                let db_conflicts = Arc::clone(&self.conflicts_resolved);
                let replay_fn: repl_peer::PeerReplayFn = Arc::new(move |payload: &[u8]| {
                    let record = aol::decode_payload(payload)?;
                    let incoming_rev = RevisionID::from(record.revision);

                    // Loop prevention
                    if incoming_rev.cluster_id() == db_rev_gen_cluster {
                        return Ok(false);
                    }

                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    if record.expires_at_ms > 0 && record.expires_at_ms <= now_ms {
                        return Ok(false);
                    }

                    let is_sentinel =
                        record.key == Key::Str(String::new()) && record.value.is_null();

                    let applied = if is_sentinel {
                        let mut map = db_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        map.entry(record.namespace.clone())
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                        true
                    } else {
                        let ttl = if record.expires_at_ms > 0 {
                            let remaining_ms = record.expires_at_ms.saturating_sub(now_ms);
                            Some(Duration::from_millis(remaining_ms))
                        } else {
                            None
                        };

                        let map = db_ns_data.read().unwrap_or_else(|e| e.into_inner());
                        if let Some(mt_mutex) = map.get(&record.namespace) {
                            let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                            mt.put_if_newer(
                                record.key.clone(),
                                record.value.clone(),
                                incoming_rev,
                                ttl,
                            )
                        } else {
                            drop(map);
                            let mut map = db_ns_data.write().unwrap_or_else(|e| e.into_inner());
                            let mt = map
                                .entry(record.namespace.clone())
                                .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                            let mt = mt.get_mut().unwrap_or_else(|e| e.into_inner());
                            mt.put_if_newer(
                                record.key.clone(),
                                record.value.clone(),
                                incoming_rev,
                                ttl,
                            )
                        }
                    };

                    if applied {
                        // Track highest revision for incremental sync
                        {
                            let mut lr = rev_tracker.lock().unwrap_or_else(|e| e.into_inner());
                            if record.revision > *lr {
                                *lr = record.revision;
                            }
                        }
                        let mut aol = db_aol.lock().unwrap_or_else(|e| e.into_inner());
                        aol.append_encoded(payload)?;
                    } else {
                        db_conflicts.fetch_add(1, Ordering::Relaxed);
                    }

                    Ok(applied)
                });

                // Build broadcast callback
                let peer_sessions_for_broadcast = Arc::clone(&self.peer_sessions);
                let broadcast_fn: repl_peer::PeerBroadcastFn =
                    Arc::new(move |payload: &[u8], from_cluster: u16| {
                        let mut sessions = peer_sessions_for_broadcast
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        // Clean up dead sessions before broadcasting
                        sessions.retain(|s| s.is_alive());
                        for session in sessions.iter() {
                            // Don't send back to the source peer
                            if session.remote_cluster_id() != from_cluster {
                                session.send(payload);
                            }
                        }
                    });

                // Build a post-sync callback to reload SSTable index (same as replica)
                let sync_sstables = Arc::clone(&self.sstables);
                let sync_sst_seq = Arc::clone(&self.sst_sequence);
                let sync_db_path = self.config.path.clone();
                let sync_max_levels = self.config.max_levels;
                let sync_cache = self.block_cache.clone();
                let sync_io = Arc::clone(&self.io_backend);
                let sync_ns_data = Arc::clone(&self.namespace_data);
                let sync_aol = Arc::clone(&self.aol);
                let post_sync_fn: repl_peer::PeerPostSyncFn = Arc::new(move || {
                    let (new_sst, new_seq) = Self::scan_sstables(
                        &sync_db_path,
                        sync_max_levels,
                        &sync_cache,
                        sync_io.as_ref(),
                    )?;

                    // Reset memtables in-place and register new namespaces
                    {
                        let mut ns_map = sync_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        for mt_mutex in ns_map.values() {
                            let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                            *mt = memtable::MemTable::new();
                        }
                        for ns_name in new_sst.keys() {
                            ns_map
                                .entry(ns_name.clone())
                                .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                        }
                        ns_map
                            .entry(DEFAULT_NAMESPACE.to_owned())
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                    }

                    // Truncate local AOL so stale records don't reappear on restart
                    {
                        let mut aol = sync_aol.lock().unwrap_or_else(|e| e.into_inner());
                        aol.truncate(&sync_db_path)?;
                    }

                    // Replace SSTable index
                    {
                        let mut sst = sync_sstables.write().unwrap_or_else(|e| e.into_inner());
                        *sst = new_sst;
                    }

                    // Update sequence counter
                    let old_seq = sync_sst_seq.load(Ordering::Relaxed);
                    if new_seq > old_seq {
                        sync_sst_seq.store(new_seq, Ordering::Relaxed);
                    }

                    Ok(())
                });

                // Build a flush callback for AOL (needed before incremental sync)
                let flush_aol = Arc::clone(&self.aol);
                let flush_fn: repl_peer::PeerFlushFn = Arc::new(move || {
                    let mut aol = flush_aol.lock().unwrap_or_else(|e| e.into_inner());
                    aol.flush_if_dirty()?;
                    Ok(())
                });

                // Build a drop-namespace callback for peer DropNamespace messages
                let drop_ns_data = Arc::clone(&self.namespace_data);
                let drop_sstables = Arc::clone(&self.sstables);
                let drop_obj_stores = Arc::clone(&self.object_stores);
                let drop_db_path = self.config.path.clone();
                let drop_ns_fn: repl_peer::PeerDropNsFn = Arc::new(move |namespace: &str| {
                    // 1. Remove from in-memory maps
                    {
                        let mut map = drop_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }
                    {
                        let mut map = drop_sstables.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }
                    {
                        let mut map = drop_obj_stores.write().unwrap_or_else(|e| e.into_inner());
                        map.remove(namespace);
                    }

                    // 2. Delete on-disk files
                    let sst_dir = drop_db_path.join("sst").join(namespace);
                    if sst_dir.exists() {
                        fs::remove_dir_all(&sst_dir)?;
                    }
                    let obj_dir = drop_db_path.join("objects").join(namespace);
                    if obj_dir.exists() {
                        fs::remove_dir_all(&obj_dir)?;
                    }

                    // 3. Re-create default namespace if it was dropped
                    if namespace == DEFAULT_NAMESPACE {
                        let mut map = drop_ns_data.write().unwrap_or_else(|e| e.into_inner());
                        map.entry(DEFAULT_NAMESPACE.to_owned())
                            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                    }

                    Ok(())
                });

                // Build a memtable flush callback for FlushNotify messages
                let memtable_flush_fn = self.make_memtable_flush_fn();

                let peer_config = Arc::new(repl_peer::PeerSessionConfig {
                    local_cluster_id: cluster_id,
                    db_path: db_path.clone(),
                    max_levels,
                    io_backend: Arc::clone(&self.io_backend),
                    replay_fn,
                    broadcast_fn,
                    post_sync_fn,
                    last_revision: Arc::clone(&last_revision),
                    flush_fn,
                    memtable_flush_fn,
                    drop_ns_fn,
                });

                // Start listener
                let listener = repl_peer::PeerListener::start(
                    &self.config.repl_bind,
                    self.config.repl_port,
                    Arc::clone(&peer_config),
                    Arc::clone(&self.peer_sessions),
                    Arc::clone(&stop),
                )?;
                self.peer_listener = Some(listener);

                // Start connectors for each peer
                for peer_addr in &self.config.peers {
                    let connector = repl_peer::PeerConnector::start(
                        peer_addr.clone(),
                        Arc::clone(&peer_config),
                        Arc::clone(&self.peer_sessions),
                        Arc::clone(&stop),
                    );
                    self.peer_connectors.push(connector);
                }

                // Store revision tracker for checkpoint persistence on close
                self.peer_last_revision = Some(last_revision);
            }
            replication::Role::Standalone => {
                // No replication — nothing to start
            }
        }
        Ok(())
    }

    fn stop_replication(&mut self) {
        if let Some(ref mut sender) = self.repl_sender {
            sender.stop();
        }
        self.repl_sender = None;
        if let Some(ref mut receiver) = self.repl_receiver {
            receiver.stop();
        }
        self.repl_receiver = None;

        // Save peer checkpoint before stopping sessions
        if let Some(ref lr) = self.peer_last_revision {
            let rev = *lr.lock().unwrap_or_else(|e| e.into_inner());
            repl_peer::save_peer_checkpoint(&self.config.path, rev);
        }

        // Stop peer replication components
        if let Some(ref mut listener) = self.peer_listener {
            listener.stop();
        }
        self.peer_listener = None;
        for connector in &mut self.peer_connectors {
            connector.stop();
        }
        self.peer_connectors.clear();
        {
            let mut sessions = self.peer_sessions.lock().unwrap_or_else(|e| e.into_inner());
            for session in sessions.iter_mut() {
                session.stop();
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.config.path
    }

    pub fn stats(&self) -> Stats {
        let map = self
            .namespace_data
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let namespace_count = map.len() as u64;
        let mut total_keys: u64 = 0;
        let mut write_buffer_bytes: u64 = 0;
        for mt in map.values() {
            let mt = mt.lock().unwrap_or_else(|e| e.into_inner());
            total_keys += mt.count();
            write_buffer_bytes += mt.approximate_size() as u64;
        }

        // SSTable stats from self.sstables
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        let max_levels = self.config.max_levels;
        let mut sstable_count: u64 = 0;
        let mut pending_compactions: u64 = 0;
        let mut level_stats = vec![stats::LevelStat::default(); max_levels];

        for (_ns, levels) in sst.iter() {
            for (level, readers) in levels.iter().enumerate() {
                let count = readers.len() as u64;
                let size: u64 = readers.iter().map(|r| r.size_bytes() as u64).sum();
                sstable_count += count;
                if level < max_levels {
                    level_stats[level].file_count += count;
                    level_stats[level].size_bytes += size;
                }
            }

            // L0 compaction trigger
            if let Some(l0_readers) = levels.first() {
                if l0_readers.len() >= self.config.l0_max_count {
                    pending_compactions += 1;
                } else {
                    let l0_size: usize = l0_readers.iter().map(|r| r.size_bytes()).sum();
                    if l0_size >= self.config.l0_max_size {
                        pending_compactions += 1;
                    }
                }
            }

            // L1+ compaction triggers
            for level in 1..max_levels {
                let total_size: usize = levels
                    .get(level)
                    .map(|readers| readers.iter().map(|r| r.size_bytes()).sum())
                    .unwrap_or(0);
                if total_size >= Self::do_level_max_size(&self.config, level) {
                    pending_compactions += 1;
                }
            }
        }
        drop(sst);

        // Cache hit/miss counters
        let (cache_hits, cache_misses) = if let Some(ref bc) = self.block_cache {
            let cache = bc.lock().unwrap_or_else(|e| e.into_inner());
            (cache.hits(), cache.misses())
        } else {
            (0, 0)
        };

        Stats {
            total_keys,
            data_size_bytes: write_buffer_bytes,
            namespace_count,
            level_count: max_levels,
            sstable_count,
            write_buffer_bytes,
            pending_compactions,
            level_stats,
            op_puts: self.op_puts.load(Ordering::Relaxed),
            op_gets: self.op_gets.load(Ordering::Relaxed),
            op_deletes: self.op_deletes.load(Ordering::Relaxed),
            cache_hits,
            cache_misses,
            uptime: self.opened_at.elapsed(),
            role: self.config.role.to_string(),
            peer_count: self
                .peer_sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len() as u64,
            conflicts_resolved: self.conflicts_resolved.load(Ordering::Relaxed),
        }
    }

    /// Re-derive all statistics from current engine state and persist
    /// operation counters. Useful as an admin recovery tool when stats
    /// may have drifted.
    pub fn analyze(&self) -> Stats {
        self.save_stats_meta();
        self.stats()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    /// Returns a reference to the shared metrics registry.
    pub(crate) fn metrics(&self) -> &Arc<metrics::Metrics> {
        &self.metrics
    }

    /// Render all metrics in Prometheus exposition text format.
    pub fn prometheus_metrics(&self) -> String {
        let stats = self.stats();
        metrics::render_prometheus(&stats, &self.metrics)
    }

    /// Switch to a namespace, creating it if it does not exist.
    ///
    /// Pass `password: Some("...")` to open an encrypted namespace, or `None`
    /// for a non-encrypted one. The encryption state is recorded on first
    /// access and enforced on subsequent calls within the same session.
    pub fn namespace(&self, name: &str, password: Option<&str>) -> Result<Namespace<'_>> {
        let encrypted = password.is_some();
        let mut map = self
            .encrypted_namespaces
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(&was_encrypted) = map.get(name) {
            if was_encrypted && !encrypted {
                return Err(Error::EncryptionRequired(format!(
                    "namespace '{name}' requires a password"
                )));
            }
            if !was_encrypted && encrypted {
                return Err(Error::NotEncrypted(format!(
                    "namespace '{name}' is not encrypted"
                )));
            }
        } else {
            // Check on-disk encryption marker for namespaces not yet seen
            // this session. This handles the case where the DB is reopened
            // and the in-memory map is empty.
            let meta_path = self.sst_namespace_dir(name).join("ns.meta");
            let persisted_encrypted = meta_path.exists();
            if persisted_encrypted && !encrypted {
                map.insert(name.to_owned(), true);
                return Err(Error::EncryptionRequired(format!(
                    "namespace '{name}' requires a password"
                )));
            }
            if !persisted_encrypted && encrypted {
                // Write encryption marker to disk
                let ns_dir = self.sst_namespace_dir(name);
                fs::create_dir_all(&ns_dir)?;
                fs::write(&meta_path, b"encrypted")?;
            }

            map.insert(name.to_owned(), encrypted);
            drop(map);

            // Ensure the namespace is registered in the memtable map so it
            // appears in `list_namespaces` immediately (not only after a write).
            self.get_or_create_memtable(name);

            // On primary/peer nodes, broadcast a Null sentinel so other nodes
            // learn about the new namespace immediately (even if no data is written).
            if self.config.role == replication::Role::Primary
                || self.config.role == replication::Role::Peer
            {
                let rev = self.revision_gen.generate();
                self.append_to_aol(
                    name,
                    rev.as_u128(),
                    &Key::Str(String::new()),
                    &Value::Null,
                    None,
                )?;
            }

            return Namespace::open(self, name, password);
        }
        drop(map);

        // Ensure the namespace is registered in the memtable map so it
        // appears in `list_namespaces` immediately (not only after a write).
        self.get_or_create_memtable(name);

        Namespace::open(self, name, password)
    }

    /// List all namespace names.
    ///
    /// Returns the sorted union of namespaces known to the in-memory
    /// MemTable map and the L0 SSTable cache.
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut names = std::collections::BTreeSet::new();

        {
            let map = self
                .namespace_data
                .read()
                .unwrap_or_else(|e| e.into_inner());
            for key in map.keys() {
                names.insert(key.clone());
            }
        }
        {
            let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
            for key in sst.keys() {
                names.insert(key.clone());
            }
        }

        Ok(names.into_iter().collect())
    }

    /// Drop a namespace and all its data. The default namespace cannot be dropped.
    ///
    /// Removes in-memory state (MemTable, L0 readers, object store, encryption
    /// tracking), deletes on-disk files (SSTables, bin objects, crypto salt),
    /// and flushes remaining namespaces + truncates the AOL so the dropped
    /// namespace cannot reappear on restart.
    pub fn drop_namespace(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidNamespace(
                "namespace name must not be empty".into(),
            ));
        }

        // Check the namespace actually exists
        let exists = {
            let nd = self
                .namespace_data
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
            nd.contains_key(name) || sst.contains_key(name)
        };
        if !exists {
            return Err(Error::InvalidNamespace(format!(
                "namespace '{name}' does not exist"
            )));
        }

        // 1. Remove from in-memory maps
        {
            let mut map = self
                .namespace_data
                .write()
                .unwrap_or_else(|e| e.into_inner());
            map.remove(name);
        }
        {
            let mut map = self.sstables.write().unwrap_or_else(|e| e.into_inner());
            map.remove(name);
        }
        {
            let mut map = self
                .object_stores
                .write()
                .unwrap_or_else(|e| e.into_inner());
            map.remove(name);
        }
        {
            let mut map = self
                .encrypted_namespaces
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.remove(name);
        }

        // 2. Delete on-disk data
        let sst_dir = self.sst_namespace_dir(name);
        if sst_dir.exists() {
            fs::remove_dir_all(&sst_dir)?;
        }
        let obj_dir = self.config.path.join("objects").join(name);
        if obj_dir.exists() {
            fs::remove_dir_all(&obj_dir)?;
        }
        let salt_path = self.config.path.join("crypto").join(format!("{name}.salt"));
        if salt_path.exists() {
            fs::remove_file(&salt_path)?;
        }

        // 3. Broadcast to replicas/peers before AOL truncation
        if let Some(ref sender) = self.repl_sender {
            sender.broadcast_drop_namespace(name);
        }
        {
            let sessions = self.peer_sessions.lock().unwrap_or_else(|e| e.into_inner());
            for session in sessions.iter() {
                session.send_drop_namespace(name);
            }
        }

        // 4. Flush remaining namespaces + truncate AOL so the dropped
        //    namespace's records don't resurrect on restart.
        //    flush() only truncates when it actually writes SSTables, so we
        //    force-truncate afterwards to cover the case where no other
        //    namespaces have pending data.
        self.flush()?;
        {
            let mut aol = self.aol.lock().unwrap_or_else(|e| e.into_inner());
            aol.truncate(&self.config.path)?;
        }

        // Re-create the default namespace if it was just dropped, so it
        // always appears in list_namespaces().
        if name == DEFAULT_NAMESPACE {
            self.get_or_create_memtable(DEFAULT_NAMESPACE);
        }

        Ok(())
    }

    // --- Flush / Sync ---

    /// Flush all in-memory write buffers to L0 SSTable files.
    ///
    /// For each namespace with a non-empty MemTable, drains the latest
    /// entry per key, writes an SSTable to `<db>/sst/<namespace>/L0/`,
    /// and prepends the reader to the L0 cache. After all namespaces are
    /// flushed, the AOL is truncated.
    pub fn flush(&self) -> Result<()> {
        self.flush_internal(true)
    }

    /// Internal flush implementation. When `broadcast` is true, notifies
    /// connected replicas/peers that a flush occurred. The callback-driven
    /// flushes (triggered by a remote `FlushNotify`) pass `false` to avoid
    /// infinite broadcast loops.
    fn flush_internal(&self, broadcast: bool) -> Result<()> {
        let flush_start = Instant::now();
        let namespaces: Vec<String> = {
            let map = self
                .namespace_data
                .read()
                .unwrap_or_else(|e| e.into_inner());
            map.keys().cloned().collect()
        };

        let mut flushed_any = false;

        for ns_name in &namespaces {
            let entries = {
                let mt = self.get_or_create_memtable(ns_name);
                let mut mt = mt.lock().unwrap_or_else(|e| e.into_inner());
                if mt.is_empty() {
                    continue;
                }
                mt.drain_all()
            };

            if entries.is_empty() {
                continue;
            }

            let entry_count = entries.len() as u64;

            // Allocate a new sequence number and write the SSTable
            let seq = self.sst_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let l0_dir = self.sst_level_dir(ns_name, 0);
            fs::create_dir_all(&l0_dir)?;
            let sst_path = l0_dir.join(format!("{seq:06}.sst"));

            let mut writer = sstable::SSTableWriter::new(
                &sst_path,
                self.config.block_size,
                self.config.compression.clone(),
                self.config.bloom_bits,
                self.config.bloom_prefix_len,
                &*self.io_backend,
            )?;
            for (key, value, revision, expires_at_ms) in &entries {
                writer.add(key, value, *revision, *expires_at_ms)?;
            }
            writer.finish()?;

            // Open the reader and prepend to L0 cache (newest first)
            let reader = sstable::SSTableReader::open(
                &sst_path,
                seq,
                self.block_cache.clone(),
                &*self.io_backend,
            )?;
            let sst_bytes = reader.size_bytes() as u64;
            let mut sst = self.sstables.write().unwrap_or_else(|e| e.into_inner());
            let levels = sst
                .entry(ns_name.clone())
                .or_insert_with(|| vec![Vec::new()]);
            if levels.is_empty() {
                levels.push(Vec::new());
            }
            levels[0].insert(0, reader);

            // Record flush metrics
            self.metrics.flush_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .bytes_flushed
                .fetch_add(sst_bytes, Ordering::Relaxed);

            // Notify event listener
            if let Some(ref listener) = self.event_listener {
                listener.on_flush_complete(metrics::FlushEvent {
                    namespace: ns_name.clone(),
                    entries: entry_count,
                    bytes: sst_bytes,
                    duration: flush_start.elapsed(),
                });
            }

            flushed_any = true;
        }

        if flushed_any {
            self.metrics
                .flush
                .observe(flush_start.elapsed().as_secs_f64());
            let mut aol = self.aol.lock().unwrap_or_else(|e| e.into_inner());
            aol.truncate(&self.config.path)?;

            // Flush pack writers for durability (best-effort)
            let obj_stores = self.object_stores.read().unwrap_or_else(|e| e.into_inner());
            for (_, store) in obj_stores.iter() {
                let _ = store.flush();
            }
        }

        // Signal background compaction thread
        if flushed_any {
            let (lock, cvar) = &*self.compaction_notify;
            let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
            *pending = true;
            cvar.notify_one();
        }

        // Notify connected replicas/peers about the flush
        if flushed_any && broadcast {
            if let Some(ref sender) = self.repl_sender {
                sender.broadcast_flush();
            }
            if self.config.role == replication::Role::Peer {
                let sessions = self.peer_sessions.lock().unwrap_or_else(|e| e.into_inner());
                for session in sessions.iter() {
                    session.send_flush();
                }
            }
        }

        Ok(())
    }

    /// Flush and fsync all data to durable storage.
    ///
    /// Flushes any buffered AOL writes and then calls `fsync` on the
    /// underlying file descriptor, guaranteeing that all committed data
    /// is persisted to the storage device.
    pub fn sync(&self) -> Result<()> {
        let mut aol = self.aol.lock().unwrap_or_else(|e| e.into_inner());
        aol.sync()
    }

    // --- Destroy / Repair ---

    /// Destroy the database at the given path, deleting all data.
    ///
    /// Validates that the path looks like an rKV database (contains an `aol`
    /// file or `sst/` directory) before removing the entire directory tree.
    /// Returns `Io(NotFound)` if the path does not exist.
    pub fn destroy(path: impl Into<PathBuf>) -> Result<()> {
        let path = path.into();

        if !path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("database path does not exist: {}", path.display()),
            )));
        }

        // Safety check: verify this looks like an rKV database
        let has_aol = path.join("aol").exists();
        let has_sst = path.join("sst").exists();
        if !has_aol && !has_sst {
            return Err(Error::Corruption(format!(
                "path does not appear to be an rKV database: {}",
                path.display()
            )));
        }

        fs::remove_dir_all(&path)?;
        Ok(())
    }

    /// Attempt to repair a corrupted database at the given path.
    ///
    /// Scans three data sources (AOL, SSTables, bin objects), tolerating
    /// corruption in each. Corrupted SSTable files and bin objects are
    /// deleted. The AOL is rewritten to contain only valid records.
    ///
    /// Returns a `RecoveryReport` describing what was scanned, recovered,
    /// and lost. Callers should inspect the report to determine whether
    /// the database is usable.
    pub fn repair(path: impl Into<PathBuf>) -> Result<RecoveryReport> {
        let path = path.into();
        let mut report = RecoveryReport::default();

        if !path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("database path does not exist: {}", path.display()),
            )));
        }

        // --- Phase 1: Replay AOL with verification ---
        let aol_file = path.join("aol");
        let mut good_records: Vec<aol::AolRecord> = Vec::new();
        if aol_file.exists() {
            let repair_aol_io = io::BufferedIo;
            let (records, skipped) = aol::Aol::replay(&path, true, &repair_aol_io)?;
            let skipped_count = skipped.len() as u64;
            report.wal_records_scanned = (records.len() as u64) + skipped_count;
            report.wal_records_skipped = skipped_count;
            if !skipped.is_empty() {
                for detail in &skipped {
                    report.warnings.push(format!("AOL: {detail}"));
                }
            }
            good_records = records;
        }

        // Rewrite AOL with only valid records
        if report.wal_records_skipped > 0 {
            Self::rewrite_aol(&path, &good_records)?;
            report
                .warnings
                .push("AOL rewritten with valid records only".into());
        }

        // --- Phase 2: Scan SSTables ---
        let sst_root = path.join("sst");
        if sst_root.exists() {
            Self::repair_sstables(&sst_root, &mut report)?;
        }

        // --- Phase 3: Scan bin objects ---
        let obj_root = path.join("objects");
        if obj_root.exists() {
            Self::repair_objects(&obj_root, &mut report)?;
        }

        // Compute keys_recovered from surviving AOL records
        report.keys_recovered = good_records.len() as u64;

        Ok(report)
    }

    /// Rewrite the AOL file with only the given valid records.
    fn rewrite_aol(db_path: &Path, records: &[aol::AolRecord]) -> Result<()> {
        // Open the AOL (positions at end), then truncate to header-only
        let mut aol = aol::Aol::open(db_path, 0)?;
        aol.truncate(db_path)?;

        // Re-append only valid records
        for record in records {
            aol.append_raw(
                &record.namespace,
                record.revision,
                &record.key,
                &record.value,
                record.expires_at_ms,
            )?;
        }

        Ok(())
    }

    /// Scan all SSTable files, removing corrupted ones.
    fn repair_sstables(sst_root: &Path, report: &mut RecoveryReport) -> Result<()> {
        for ns_entry in fs::read_dir(sst_root)? {
            let ns_entry = ns_entry?;
            if !ns_entry.file_type()?.is_dir() {
                continue;
            }

            for level_entry in fs::read_dir(ns_entry.path())? {
                let level_entry = level_entry?;
                if !level_entry.file_type()?.is_dir() {
                    continue;
                }

                for file_entry in fs::read_dir(level_entry.path())? {
                    let file_entry = file_entry?;
                    let fpath = file_entry.path();
                    if fpath.extension().and_then(|e| e.to_str()) != Some("sst") {
                        continue;
                    }

                    report.sstable_blocks_scanned += 1;

                    let repair_io = io::BufferedIo;
                    match sstable::SSTableReader::open(&fpath, 0, None, &repair_io) {
                        Ok(reader) => {
                            // Verify block checksums
                            if let Err(_e) = reader.iter_entries(true) {
                                report.sstable_blocks_corrupted += 1;
                                report.warnings.push(format!(
                                    "SSTable corrupted, removed: {}",
                                    fpath.display()
                                ));
                                fs::remove_file(&fpath)?;
                            }
                        }
                        Err(_e) => {
                            report.sstable_blocks_corrupted += 1;
                            report
                                .warnings
                                .push(format!("SSTable unreadable, removed: {}", fpath.display()));
                            fs::remove_file(&fpath)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Scan all bin objects, removing corrupted ones.
    fn repair_objects(obj_root: &Path, report: &mut RecoveryReport) -> Result<()> {
        for ns_entry in fs::read_dir(obj_root)? {
            let ns_entry = ns_entry?;
            if !ns_entry.file_type()?.is_dir() {
                continue;
            }
            let ns = ns_entry.file_name().to_string_lossy().to_string();
            let repair_io: Arc<dyn io::IoBackend> = Arc::new(io::BufferedIo);
            let store =
                objects::ObjectStore::open(obj_root.parent().unwrap_or(obj_root), &ns, repair_io)?;

            let hashes = store.list_object_hashes()?;
            for hash_str in &hashes {
                report.objects_scanned += 1;

                // Reconstruct a ValuePointer from the hex hash to verify
                let mut hash_bytes = [0u8; 32];
                let ok = hex_decode(hash_str, &mut hash_bytes);
                if !ok {
                    report.objects_corrupted += 1;
                    report
                        .warnings
                        .push(format!("Object invalid hash name: {hash_str}"));
                    store.delete_object(hash_str)?;
                    continue;
                }

                let vp = value::ValuePointer::new(hash_bytes, 0);
                match store.read(&vp, true) {
                    Ok(_) => {}
                    Err(_) => {
                        report.objects_corrupted += 1;
                        report
                            .warnings
                            .push(format!("Object corrupted, removed: {hash_str}"));
                        store.delete_object(hash_str)?;
                    }
                }
            }
        }
        Ok(())
    }

    // --- Dump / Load ---

    /// Export the database to a portable backup file.
    ///
    /// Flushes all in-memory write buffers, then iterates every namespace's
    /// SSTables to produce a self-contained dump. Tombstones are filtered,
    /// `Pointer` values are resolved to inline `Data`, and expired entries
    /// are skipped. Encrypted namespaces are skipped (no password available
    /// for decryption during raw iteration).
    pub fn dump(&self, path: impl Into<PathBuf>) -> Result<()> {
        let dump_path = path.into();

        // Flush memtables so all data is in SSTables
        self.flush()?;

        let mut writer = dump::DumpWriter::new(&dump_path, &*self.io_backend)?;
        writer.write_header(&self.config.path)?;

        let namespaces = self.list_namespaces()?;

        for ns_name in &namespaces {
            // Skip encrypted namespaces
            {
                let enc = self
                    .encrypted_namespaces
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if enc.get(ns_name).copied().unwrap_or(false) {
                    continue;
                }
            }

            // Merge all SSTable levels into a single sorted map (bottom-up,
            // newest wins) — same merge strategy as compaction.
            let merged = {
                let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
                let levels = match sst.get(ns_name) {
                    Some(l) => l,
                    None => continue,
                };

                let mut merged = std::collections::BTreeMap::<Key, Value>::new();

                // Process levels from bottom (oldest) to top (newest)
                for (level_idx, level_readers) in levels.iter().enumerate().rev() {
                    if level_idx == 0 {
                        // L0: reverse (oldest-to-newest within L0)
                        for reader in level_readers.iter().rev() {
                            for (key, value, _rev, _exp) in
                                reader.iter_entries(self.config.verify_checksums)?
                            {
                                merged.insert(key, value);
                            }
                        }
                    } else {
                        for reader in level_readers {
                            for (key, value, _rev, _exp) in
                                reader.iter_entries(self.config.verify_checksums)?
                            {
                                merged.insert(key, value);
                            }
                        }
                    }
                }

                // Filter tombstones
                merged.retain(|_, v| !v.is_tombstone());
                merged
            };

            for (key, value) in &merged {
                // Resolve Pointer → inline Data
                let resolved = self.resolve_value(ns_name, value)?;
                // TTL is not preserved — SSTables don't store expiry.
                writer.write_record(ns_name, key, &resolved, 0)?;
            }
        }

        writer.finish()?;
        Ok(())
    }

    /// Import a database from a portable backup file.
    ///
    /// Reads the dump file header to recover the original DB path, creates
    /// a fresh database at that path, and replays all records. Returns the
    /// populated `DB` handle. Expired entries are skipped during import.
    ///
    /// Returns `InvalidConfig` if the target path already contains data.
    pub fn load(path: impl Into<PathBuf>) -> Result<DB> {
        let dump_path = path.into();

        let load_io = io::BufferedIo;
        let mut reader = dump::DumpReader::open(&dump_path, &load_io)?;
        let header = reader.read_header()?;

        let db_path = PathBuf::from(&header.db_path);

        // Refuse to overwrite an existing database
        if db_path.exists() && fs::read_dir(&db_path)?.next().is_some() {
            return Err(Error::InvalidConfig(format!(
                "target path '{}' is not empty",
                db_path.display()
            )));
        }

        let config = Config::new(&db_path);
        let db = DB::open(config)?;

        while let Some(record) = reader.read_record(true)? {
            let ns = db.namespace(&record.namespace, None)?;
            ns.put(record.key, record.value, None)?;
        }

        db.flush()?;
        Ok(db)
    }

    // --- Compaction ---

    /// Trigger a manual compaction.
    ///
    /// Serializes with the background compaction thread via a shared mutex.
    /// For each namespace, merges L0 into L1, then cascades through
    /// deeper levels when a level exceeds its size threshold. Tombstones
    /// are dropped only at the bottommost level (`max_levels - 1`).
    pub fn compact(&self) -> Result<()> {
        let _guard = self
            .compaction_mutex
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Self::do_compact(
            &self.config,
            &self.sstables,
            &self.sst_sequence,
            &self.block_cache,
            &self.io_backend,
            &self.namespace_data,
            &self.metrics,
            &self.event_listener,
        )
    }

    /// Block until the background compaction thread completes its current cycle.
    ///
    /// Signals the compaction thread to wake up and waits until it finishes
    /// processing. Useful for deterministic testing — ensures all pending
    /// compaction is done before asserting on-disk state.
    pub fn wait_for_compaction(&self) {
        // Clear done flag to ensure we wait for the NEXT cycle, not a stale one
        {
            let (lock, _cvar) = &*self.compaction_done;
            let mut done = lock.lock().unwrap_or_else(|e| e.into_inner());
            *done = false;
        }
        // Signal the compaction thread
        {
            let (lock, cvar) = &*self.compaction_notify;
            let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
            *pending = true;
            cvar.notify_one();
        }
        // Wait for the compaction thread to signal done
        let (lock, cvar) = &*self.compaction_done;
        let mut done = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !*done {
            done = cvar.wait(done).unwrap();
        }
        *done = false;
    }

    /// Check if any namespace's L0 level exceeds the compaction thresholds,
    /// or if any L1+ level exceeds its max size.
    fn check_should_compact(sstables: &RwLock<LeveledSSTables>, config: &Config) -> bool {
        let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
        for (_ns, levels) in sst.iter() {
            // L0: check count and size thresholds
            if let Some(l0_readers) = levels.first() {
                if l0_readers.len() >= config.l0_max_count {
                    return true;
                }
                let l0_size: usize = l0_readers.iter().map(|r| r.size_bytes()).sum();
                if l0_size >= config.l0_max_size {
                    return true;
                }
            }
            // L1+: check if any level exceeds its max size
            for level in 1..config.max_levels {
                let total_size: usize = levels
                    .get(level)
                    .map(|readers| readers.iter().map(|r| r.size_bytes()).sum())
                    .unwrap_or(0);
                if total_size >= Self::do_level_max_size(config, level) {
                    return true;
                }
            }
        }
        false
    }

    /// Run compaction across all namespaces (static helper for background thread).
    #[allow(clippy::too_many_arguments)]
    fn do_compact(
        config: &Config,
        sstables: &RwLock<LeveledSSTables>,
        sst_sequence: &AtomicU64,
        block_cache: &Option<Arc<Mutex<cache::BlockCache>>>,
        io: &Arc<dyn io::IoBackend>,
        namespace_data: &RwLock<HashMap<String, Mutex<memtable::MemTable>>>,
        m: &Arc<metrics::Metrics>,
        listener: &Option<Arc<dyn metrics::EventListener>>,
    ) -> Result<()> {
        let compact_start = Instant::now();
        let namespaces: Vec<String> = {
            let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
            sst.keys().cloned().collect()
        };

        let mut compacted_any = false;
        for ns_name in &namespaces {
            let did_compact = Self::do_compact_namespace(
                ns_name,
                config,
                sstables,
                sst_sequence,
                block_cache,
                io,
                namespace_data,
                m,
                listener,
            )?;
            compacted_any = compacted_any || did_compact;
        }

        if compacted_any {
            m.compaction.observe(compact_start.elapsed().as_secs_f64());
            m.compaction_total.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Compact a single namespace with cascading level merges (static helper).
    /// Returns `true` if any merge was performed.
    #[allow(clippy::too_many_arguments)]
    fn do_compact_namespace(
        ns: &str,
        config: &Config,
        sstables: &RwLock<LeveledSSTables>,
        sst_sequence: &AtomicU64,
        block_cache: &Option<Arc<Mutex<cache::BlockCache>>>,
        io: &Arc<dyn io::IoBackend>,
        namespace_data: &RwLock<HashMap<String, Mutex<memtable::MemTable>>>,
        m: &Arc<metrics::Metrics>,
        listener: &Option<Arc<dyn metrics::EventListener>>,
    ) -> Result<bool> {
        let max_levels = config.max_levels;

        if max_levels < 2 {
            return Ok(false);
        }

        {
            let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
            let has_l0 = sst
                .get(ns)
                .and_then(|levels| levels.first())
                .is_some_and(|l0| !l0.is_empty());
            if !has_l0 {
                return Ok(false);
            }
        }

        // Step 1: merge L0 → L1
        let is_bottom = max_levels <= 2;
        let merge_start = Instant::now();
        let output_size = Self::do_merge_two_levels(
            ns,
            0,
            1,
            is_bottom,
            config,
            sstables,
            sst_sequence,
            block_cache,
            io,
        )?;

        if output_size > 0 {
            m.bytes_compacted
                .fetch_add(output_size as u64, Ordering::Relaxed);
            if let Some(ref l) = listener {
                l.on_compaction_complete(metrics::CompactionEvent {
                    namespace: ns.to_owned(),
                    source_level: 0,
                    target_level: 1,
                    bytes: output_size as u64,
                    duration: merge_start.elapsed(),
                });
            }
        }

        // Step 2: cascade through deeper levels
        for level in 1..max_levels - 1 {
            if Self::do_level_total_size(sstables, ns, level)
                <= Self::do_level_max_size(config, level)
            {
                break;
            }
            let target = level + 1;
            let drop = target >= max_levels - 1;
            let merge_start = Instant::now();
            let output_size = Self::do_merge_two_levels(
                ns,
                level,
                target,
                drop,
                config,
                sstables,
                sst_sequence,
                block_cache,
                io,
            )?;
            if output_size > 0 {
                m.bytes_compacted
                    .fetch_add(output_size as u64, Ordering::Relaxed);
                if let Some(ref l) = listener {
                    l.on_compaction_complete(metrics::CompactionEvent {
                        namespace: ns.to_owned(),
                        source_level: level,
                        target_level: target,
                        bytes: output_size as u64,
                        duration: merge_start.elapsed(),
                    });
                }
            }
        }

        // Step 3: garbage-collect orphaned bin objects
        Self::do_gc_orphaned_objects(ns, config, sstables, io, namespace_data)?;

        Ok(true)
    }

    /// Merge all SSTables from `source_level` into `target_level` (static helper).
    ///
    /// Target entries are loaded first (oldest), then source entries
    /// (newer wins via BTreeMap). For L0 source, readers are iterated
    /// in reverse (oldest-to-newest); for L1+ source, natural order.
    ///
    /// If `drop_tombstones` is true, tombstones are filtered from the
    /// output (safe only when target is the bottommost level).
    ///
    /// Returns the output SSTable size in bytes (0 if nothing to merge).
    #[allow(clippy::too_many_arguments)]
    fn do_merge_two_levels(
        ns: &str,
        source_level: usize,
        target_level: usize,
        drop_tombstones: bool,
        config: &Config,
        sstables: &RwLock<LeveledSSTables>,
        sst_sequence: &AtomicU64,
        block_cache: &Option<Arc<Mutex<cache::BlockCache>>>,
        io: &Arc<dyn io::IoBackend>,
    ) -> Result<usize> {
        let (merged_sst_ids, source_paths, merged) = {
            let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
            let levels = match sst.get(ns) {
                Some(l) => l,
                None => return Ok(0),
            };

            let source = levels
                .get(source_level)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let target = levels
                .get(target_level)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            if source.is_empty() {
                return Ok(0);
            }

            // Record the sst_ids of readers being merged so we only clean up
            // exactly these files/readers (not ones added by concurrent flush).
            let mut merged_sst_ids: Vec<u64> = Vec::with_capacity(source.len() + target.len());
            for r in source.iter().chain(target.iter()) {
                merged_sst_ids.push(r.sst_id());
            }

            // Build file paths from known sst_ids instead of scanning directories
            // to avoid picking up files from concurrent auto-flush.
            let mut source_paths = Vec::with_capacity(merged_sst_ids.len());
            let source_dir = Self::static_sst_level_dir(&config.path, ns, source_level);
            for r in source {
                source_paths.push(source_dir.join(format!("{:06}.sst", r.sst_id())));
            }
            let target_dir_path = Self::static_sst_level_dir(&config.path, ns, target_level);
            for r in target {
                source_paths.push(target_dir_path.join(format!("{:06}.sst", r.sst_id())));
            }

            // Merge all entries preserving all revisions per key.
            let mut merged =
                std::collections::BTreeMap::<Key, Vec<(Value, revision::RevisionID, u64)>>::new();

            // Target entries are oldest
            for reader in target {
                for (key, value, rev, expires_at_ms) in
                    reader.iter_entries(config.verify_checksums)?
                {
                    merged
                        .entry(key)
                        .or_default()
                        .push((value, rev, expires_at_ms));
                }
            }

            // Source entries: L0 iterate oldest-to-newest (reverse of newest-first);
            // L1+ iterate in natural order.
            if source_level == 0 {
                for reader in source.iter().rev() {
                    for (key, value, rev, expires_at_ms) in
                        reader.iter_entries(config.verify_checksums)?
                    {
                        merged
                            .entry(key)
                            .or_default()
                            .push((value, rev, expires_at_ms));
                    }
                }
            } else {
                for reader in source {
                    for (key, value, rev, expires_at_ms) in
                        reader.iter_entries(config.verify_checksums)?
                    {
                        merged
                            .entry(key)
                            .or_default()
                            .push((value, rev, expires_at_ms));
                    }
                }
            }

            // Sort revisions within each key by revision ID
            for revisions in merged.values_mut() {
                revisions.sort_by_key(|(_, rev, _)| *rev);
            }

            if drop_tombstones {
                // At the bottom level, drop entire keys whose latest revision
                // is a tombstone or expired — that key is fully deleted.
                // For other keys, remove only individual expired entries.
                merged.retain(|_, revisions| {
                    if let Some((v, _, expires_at_ms)) = revisions.last() {
                        if v.is_tombstone() || is_expired(*expires_at_ms) {
                            return false; // drop entire key
                        }
                    }
                    // Remove individual expired entries (intermediate revisions)
                    revisions.retain(|(v, _, expires_at_ms)| {
                        !v.is_tombstone() && !is_expired(*expires_at_ms)
                    });
                    !revisions.is_empty()
                });
            }

            (merged_sst_ids, source_paths, merged)
        };

        if merged.is_empty() {
            // All entries were tombstones or empty — clean up source files
            for path in &source_paths {
                let _ = fs::remove_file(path);
            }
            let mut sst = sstables.write().unwrap_or_else(|e| e.into_inner());
            if let Some(levels) = sst.get_mut(ns) {
                if let Some(ref bc) = block_cache {
                    let mut cache = bc.lock().unwrap_or_else(|e| e.into_inner());
                    for id in &merged_sst_ids {
                        cache.evict_sst(*id);
                    }
                }
                // Only remove readers that were part of this merge
                if let Some(s) = levels.get_mut(source_level) {
                    s.retain(|r| !merged_sst_ids.contains(&r.sst_id()));
                }
                if let Some(t) = levels.get_mut(target_level) {
                    t.retain(|r| !merged_sst_ids.contains(&r.sst_id()));
                }
            }
            return Ok(0);
        }

        // Write merged output as a new SSTable in target level
        let seq = sst_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let target_dir = Self::static_sst_level_dir(&config.path, ns, target_level);
        fs::create_dir_all(&target_dir)?;
        let output_path = target_dir.join(format!("{seq:06}.sst"));

        let mut writer = sstable::SSTableWriter::new(
            &output_path,
            config.block_size,
            config.compression.clone(),
            config.bloom_bits,
            config.bloom_prefix_len,
            &**io,
        )?;
        for (key, revisions) in &merged {
            for (value, rev, expires_at_ms) in revisions {
                writer.add(key, value, *rev, *expires_at_ms)?;
            }
        }
        writer.finish()?;

        // Delete old source files
        for path in &source_paths {
            let _ = fs::remove_file(path);
        }

        // Open the new reader and update the in-memory level structure
        let reader = sstable::SSTableReader::open(&output_path, seq, block_cache.clone(), &**io)?;
        let output_size = reader.size_bytes();
        let mut sst = sstables.write().unwrap_or_else(|e| e.into_inner());
        let levels = sst.entry(ns.to_owned()).or_insert_with(|| vec![Vec::new()]);

        // Evict only the merged readers' blocks from the cache
        if let Some(ref bc) = block_cache {
            let mut cache = bc.lock().unwrap_or_else(|e| e.into_inner());
            for id in &merged_sst_ids {
                cache.evict_sst(*id);
            }
        }

        // Only remove readers that were part of this merge (not ones added
        // by concurrent auto-flush).
        if let Some(s) = levels.get_mut(source_level) {
            s.retain(|r| !merged_sst_ids.contains(&r.sst_id()));
        }

        while levels.len() <= target_level {
            levels.push(Vec::new());
        }
        // Remove merged target readers and append the new merged output
        levels[target_level].retain(|r| !merged_sst_ids.contains(&r.sst_id()));
        levels[target_level].push(reader);

        Ok(output_size)
    }

    fn do_level_max_size(config: &Config, level: usize) -> usize {
        match level {
            0 => usize::MAX,
            1 => config.l1_max_size,
            _ => config.default_max_size,
        }
    }

    fn do_level_total_size(sstables: &RwLock<LeveledSSTables>, ns: &str, level: usize) -> usize {
        let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
        sst.get(ns)
            .and_then(|levels| levels.get(level))
            .map(|readers| readers.iter().map(|r| r.size_bytes()).sum())
            .unwrap_or(0)
    }

    /// Garbage-collect orphaned bin objects for a namespace (static helper).
    fn do_gc_orphaned_objects(
        ns: &str,
        config: &Config,
        sstables: &RwLock<LeveledSSTables>,
        io: &Arc<dyn io::IoBackend>,
        namespace_data: &RwLock<HashMap<String, Mutex<memtable::MemTable>>>,
    ) -> Result<()> {
        let obj_dir = config.path.join("objects").join(ns);
        if !obj_dir.exists() {
            return Ok(());
        }

        // Skip GC if the memtable has entries. Object files are written
        // before the ValuePointer is inserted into the memtable (no lock
        // held during file I/O), so concurrent puts could create objects
        // that aren't yet reflected in the memtable. Deferring GC until
        // the memtable is empty avoids deleting live objects.
        {
            let ns_map = namespace_data.read().unwrap_or_else(|e| e.into_inner());
            if let Some(mt_mutex) = ns_map.get(ns) {
                let mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                if !mt.is_empty() {
                    return Ok(());
                }
            }
        }

        let live_hashes: std::collections::HashSet<String> = {
            let sst = sstables.read().unwrap_or_else(|e| e.into_inner());
            let mut hashes = std::collections::HashSet::new();
            if let Some(levels) = sst.get(ns) {
                for level_readers in levels {
                    for reader in level_readers {
                        for (_key, value, _rev, _exp) in
                            reader.iter_entries(config.verify_checksums)?
                        {
                            if let Value::Pointer(vp) = value {
                                hashes.insert(vp.hex_hash());
                            }
                        }
                    }
                }
            }
            hashes
        };

        let store = objects::ObjectStore::open(&config.path, ns, Arc::clone(io))?;

        // Delete orphaned loose files
        let on_disk = store.list_object_hashes()?;
        for hash in &on_disk {
            if !live_hashes.contains(hash) {
                store.delete_object(hash)?;
            }
        }

        // Repack pack files to physically remove dead objects
        store.repack_gc(&live_hashes)?;

        Ok(())
    }

    // --- Internal helpers ---

    /// If the value exceeds the configured `object_size`, write it to the
    /// namespace's object store and return a `Value::Pointer`. Otherwise pass through.
    pub(crate) fn maybe_separate_value(&self, ns: &str, value: Value) -> Result<Value> {
        if let Value::Data(ref data) = value {
            if data.len() > self.config.object_size {
                let store = self.get_or_create_object_store(ns)?;
                let vp = store.write(data, self.config.compress)?;
                return Ok(Value::Pointer(vp));
            }
        }
        Ok(value)
    }

    /// If the value is a `Pointer`, read the data from the namespace's object store.
    /// Otherwise clone the value through.
    pub(crate) fn resolve_value(&self, ns: &str, value: &Value) -> Result<Value> {
        if let Value::Pointer(vp) = value {
            let store = self.get_or_create_object_store(ns)?;
            let data = store.read(vp, self.config.verify_checksums)?;
            return Ok(Value::Data(data));
        }
        Ok(value.clone())
    }

    pub(crate) fn generate_revision(&self) -> RevisionID {
        self.revision_gen.generate()
    }

    /// Acquire the AOL lock for batch operations.
    pub(crate) fn aol_lock(&self) -> std::sync::MutexGuard<'_, aol::Aol> {
        self.aol.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn append_to_aol(
        &self,
        ns: &str,
        rev: u128,
        key: &Key,
        value: &Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let mut aol = self.aol_lock();
        self.append_to_aol_locked(&mut aol, ns, rev, key, value, ttl)
    }

    /// Append to AOL using an already-acquired lock, then broadcast to
    /// replicas/peers. Used by `write_batch` to hold the lock across
    /// multiple appends.
    pub(crate) fn append_to_aol_locked(
        &self,
        aol: &mut aol::Aol,
        ns: &str,
        rev: u128,
        key: &Key,
        value: &Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        aol.append(ns, rev, key, value, ttl)?;

        // Broadcast to replicas if this node is a primary
        if let Some(ref sender) = self.repl_sender {
            let expires_at_ms = Self::ttl_to_epoch_ms(ttl);
            let payload = aol::encode_payload(ns, rev, expires_at_ms, key, value);
            sender.broadcast_aol(&payload);
        }

        // Broadcast to peers if this node is a peer (master-master)
        if self.config.role == replication::Role::Peer {
            let expires_at_ms = Self::ttl_to_epoch_ms(ttl);
            let payload = aol::encode_payload(ns, rev, expires_at_ms, key, value);
            let local_cluster = self.revision_gen.cluster_id();
            let sessions = self.peer_sessions.lock().unwrap_or_else(|e| e.into_inner());
            for session in sessions.iter() {
                if session.remote_cluster_id() != local_cluster {
                    session.send(&payload);
                }
            }
        }
        Ok(())
    }

    /// Convert an optional TTL duration to an absolute epoch timestamp in ms.
    fn ttl_to_epoch_ms(ttl: Option<Duration>) -> u64 {
        match ttl {
            Some(d) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now + d.as_millis() as u64
            }
            None => 0,
        }
    }

    /// Replay an AOL record from a peer using LWW conflict resolution.
    #[allow(dead_code)]
    ///
    /// Decodes the payload, checks if the incoming revision is newer than
    /// the current revision for that key, and applies the write only if so.
    /// Records originating from this node (same `cluster_id`) are skipped
    /// to prevent loops.
    ///
    /// Returns `true` if the record was applied, `false` if skipped.
    pub(crate) fn replay_peer_record(&self, payload: &[u8]) -> Result<bool> {
        let record = aol::decode_payload(payload)?;

        // Loop prevention: skip records originating from this node
        let incoming_rev = RevisionID::from(record.revision);
        if incoming_rev.cluster_id() == self.revision_gen.cluster_id() {
            return Ok(false);
        }

        // Skip expired records
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if record.expires_at_ms > 0 && record.expires_at_ms <= now_ms {
            return Ok(false);
        }

        // Namespace-creation sentinel: empty key + Null value
        let is_sentinel = record.key == Key::Str(String::new()) && record.value.is_null();

        // LWW check: apply only if incoming revision > current revision for key
        let applied = if is_sentinel {
            // Sentinel just creates the namespace — always accept
            self.get_or_create_memtable(&record.namespace);
            true
        } else {
            let ttl = if record.expires_at_ms > 0 {
                let remaining_ms = record.expires_at_ms.saturating_sub(now_ms);
                Some(Duration::from_millis(remaining_ms))
            } else {
                None
            };

            let map = self
                .namespace_data
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(mt_mutex) = map.get(&record.namespace) {
                let mut mt = mt_mutex.lock().unwrap_or_else(|e| e.into_inner());
                mt.put_if_newer(record.key.clone(), record.value.clone(), incoming_rev, ttl)
            } else {
                drop(map);
                let mut map = self
                    .namespace_data
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                let mt = map
                    .entry(record.namespace.clone())
                    .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
                let mt = mt.get_mut().unwrap_or_else(|e| e.into_inner());
                mt.put_if_newer(record.key.clone(), record.value.clone(), incoming_rev, ttl)
            }
        };

        if applied {
            // Write to local AOL for crash recovery
            let mut aol = self.aol.lock().unwrap_or_else(|e| e.into_inner());
            aol.append_encoded(payload)?;
        } else {
            self.conflicts_resolved.fetch_add(1, Ordering::Relaxed);
        }

        Ok(applied)
    }

    pub(crate) fn get_or_create_object_store(&self, ns: &str) -> Result<&objects::ObjectStore> {
        // Fast path: read lock to check if store already exists
        {
            let map = self.object_stores.read().unwrap_or_else(|e| e.into_inner());
            if map.contains_key(ns) {
                // SAFETY: The RwLock<HashMap> only grows (we never remove entries),
                // so a reference obtained under the read lock remains valid.
                let ptr = map.get(ns).unwrap() as *const objects::ObjectStore;
                return Ok(unsafe { &*ptr });
            }
        }

        // Slow path: write lock to insert
        let mut map = self
            .object_stores
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if !map.contains_key(ns) {
            let store =
                objects::ObjectStore::open(&self.config.path, ns, Arc::clone(&self.io_backend))?;
            map.insert(ns.to_owned(), store);
        }
        let ptr = map.get(ns).unwrap() as *const objects::ObjectStore;
        // SAFETY: Same as above — the HashMap only grows, so the reference is stable.
        Ok(unsafe { &*ptr })
    }

    /// Scan entries matching a prefix across all SSTable levels.
    ///
    /// Merges results from oldest-to-newest so newer entries overwrite older
    /// ones. Returns raw `(Key, Value)` pairs including tombstones.
    ///
    /// Merge order (oldest-to-newest, so newest wins):
    /// - L_max, L_max-1, ..., L1 (ascending key order within each level)
    /// - L0: reverse order (oldest reader → newest reader)
    pub(crate) fn scan_from_sstables(
        &self,
        ns: &str,
        prefix: &Key,
        ordered_mode: bool,
    ) -> Result<std::collections::BTreeMap<Key, Value>> {
        // In ordered mode, Key::Str("").to_bytes() is [0x02, 0x00] which
        // sits after all Int keys in byte order. An empty Str prefix means
        // "scan everything", so switch to unordered prefix matching with
        // empty bytes — starts_with(&[]) is always true.
        let scan_all = ordered_mode && *prefix == Key::Str(String::new());
        let (prefix_bytes, effective_ordered) = if scan_all {
            (vec![], false)
        } else if ordered_mode {
            (prefix.to_bytes(), true)
        } else {
            (prefix.to_prefix_bytes(), false)
        };
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        let mut merged = std::collections::BTreeMap::<Key, Value>::new();

        if let Some(levels) = sst.get(ns) {
            // Process levels from bottom (oldest) to top (newest)
            for (level_idx, level_readers) in levels.iter().enumerate().rev() {
                if level_idx == 0 {
                    // L0: reverse (oldest-to-newest within L0)
                    for reader in level_readers.iter().rev() {
                        for (key, value, _rev, expires_at_ms) in reader.scan_entries(
                            &prefix_bytes,
                            effective_ordered,
                            self.config.verify_checksums,
                        )? {
                            let value = if is_expired(expires_at_ms) {
                                Value::tombstone()
                            } else {
                                value
                            };
                            merged.insert(key, value);
                        }
                    }
                } else {
                    for reader in level_readers {
                        for (key, value, _rev, expires_at_ms) in reader.scan_entries(
                            &prefix_bytes,
                            effective_ordered,
                            self.config.verify_checksums,
                        )? {
                            let value = if is_expired(expires_at_ms) {
                                Value::tombstone()
                            } else {
                                value
                            };
                            merged.insert(key, value);
                        }
                    }
                }
            }
        }

        Ok(merged)
    }

    /// Reverse-scan entries matching a prefix across all SSTable levels.
    ///
    /// For ordered mode: returns entries with keys <= prefix. For unordered
    /// mode: same as scan_from_sstables (prefix matching). Same merge order
    /// as scan_from_sstables.
    pub(crate) fn rscan_from_sstables(
        &self,
        ns: &str,
        prefix: &Key,
        ordered_mode: bool,
    ) -> Result<std::collections::BTreeMap<Key, Value>> {
        let prefix_bytes = if ordered_mode {
            prefix.to_bytes()
        } else {
            prefix.to_prefix_bytes()
        };
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        let mut merged = std::collections::BTreeMap::<Key, Value>::new();

        if let Some(levels) = sst.get(ns) {
            for (level_idx, level_readers) in levels.iter().enumerate().rev() {
                if level_idx == 0 {
                    for reader in level_readers.iter().rev() {
                        for (key, value, _rev, expires_at_ms) in reader.rscan_entries(
                            &prefix_bytes,
                            ordered_mode,
                            self.config.verify_checksums,
                        )? {
                            let value = if is_expired(expires_at_ms) {
                                Value::tombstone()
                            } else {
                                value
                            };
                            merged.insert(key, value);
                        }
                    }
                } else {
                    for reader in level_readers {
                        for (key, value, _rev, expires_at_ms) in reader.rscan_entries(
                            &prefix_bytes,
                            ordered_mode,
                            self.config.verify_checksums,
                        )? {
                            let value = if is_expired(expires_at_ms) {
                                Value::tombstone()
                            } else {
                                value
                            };
                            merged.insert(key, value);
                        }
                    }
                }
            }
        }

        Ok(merged)
    }

    /// Look up a key across all SSTable levels for a namespace.
    ///
    /// Searches L0 (newest-first), then L1, L2, etc. Returns:
    /// - `Ok(Some((value, revision)))` if found (may be `Tombstone`)
    /// - `Ok(None)` if not found in any SSTable
    ///
    /// Expired entries (non-zero `expires_at_ms` <= now) are treated as
    /// tombstones.
    pub(crate) fn get_from_sstables(
        &self,
        ns: &str,
        key: &Key,
    ) -> Result<Option<(Value, revision::RevisionID)>> {
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        if let Some(levels) = sst.get(ns) {
            for level_readers in levels {
                for reader in level_readers {
                    if let Some((value, rev, expires_at_ms)) =
                        reader.get(key, self.config.verify_checksums)?
                    {
                        if is_expired(expires_at_ms) {
                            return Ok(Some((Value::tombstone(), rev)));
                        }
                        return Ok(Some((value, rev)));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Count all non-expired revisions for a key across all SSTable levels.
    pub(crate) fn count_revisions_from_sstables(&self, ns: &str, key: &Key) -> Result<u64> {
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        let mut count = 0u64;
        if let Some(levels) = sst.get(ns) {
            for level_readers in levels {
                for reader in level_readers {
                    for (_, _, expires_at_ms) in
                        reader.get_all_revisions(key, self.config.verify_checksums)?
                    {
                        if !is_expired(expires_at_ms) {
                            count += 1;
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    /// Retrieve a specific revision by index from SSTables.
    ///
    /// Collects all non-expired revisions in chronological order:
    /// deepest level first (L_max → L1 → L0 oldest-to-newest).
    /// Index 0 = oldest revision across all SSTables.
    pub(crate) fn get_revision_from_sstables(
        &self,
        ns: &str,
        key: &Key,
        index: u64,
    ) -> Result<Option<(Value, revision::RevisionID, u64)>> {
        let sst = self.sstables.read().unwrap_or_else(|e| e.into_inner());
        let mut all_revisions = Vec::new();
        if let Some(levels) = sst.get(ns) {
            // Deepest levels first (oldest data), then shallower levels
            for (level_idx, level_readers) in levels.iter().enumerate().rev() {
                if level_idx == 0 {
                    // L0: readers are newest-first, iterate in reverse for oldest-first
                    for reader in level_readers.iter().rev() {
                        for (value, rev, expires_at_ms) in
                            reader.get_all_revisions(key, self.config.verify_checksums)?
                        {
                            if !is_expired(expires_at_ms) {
                                all_revisions.push((value, rev, expires_at_ms));
                            }
                        }
                    }
                } else {
                    for reader in level_readers {
                        for (value, rev, expires_at_ms) in
                            reader.get_all_revisions(key, self.config.verify_checksums)?
                        {
                            if !is_expired(expires_at_ms) {
                                all_revisions.push((value, rev, expires_at_ms));
                            }
                        }
                    }
                }
            }
        }

        Ok(all_revisions.into_iter().nth(index as usize))
    }

    /// SSTable directory for a namespace: `<db>/sst/<namespace>/`.
    fn sst_namespace_dir(&self, ns: &str) -> PathBuf {
        self.config.path.join("sst").join(ns)
    }

    /// SSTable directory for a specific level: `<db>/sst/<namespace>/L<level>/`.
    fn sst_level_dir(&self, ns: &str, level: usize) -> PathBuf {
        Self::static_sst_level_dir(&self.config.path, ns, level)
    }

    /// SSTable directory for a specific level (static helper for background thread).
    fn static_sst_level_dir(db_path: &Path, ns: &str, level: usize) -> PathBuf {
        db_path.join("sst").join(ns).join(format!("L{level}"))
    }

    /// Scan existing SSTable files across all levels on startup.
    ///
    /// Walks `<db>/sst/<namespace>/L<n>/` directories, opens each `.sst`
    /// file, and returns the per-namespace leveled reader lists plus the
    /// next sequence number to use.
    fn scan_sstables(
        db_path: &Path,
        max_levels: usize,
        block_cache: &Option<Arc<Mutex<cache::BlockCache>>>,
        io: &dyn io::IoBackend,
    ) -> Result<(LeveledSSTables, u64)> {
        let sst_root = db_path.join("sst");
        let mut result: LeveledSSTables = HashMap::new();
        let mut max_seq: u64 = 0;

        if !sst_root.exists() {
            return Ok((result, max_seq));
        }

        let ns_dirs = fs::read_dir(&sst_root)?;
        for ns_entry in ns_dirs {
            let ns_entry = ns_entry?;
            if !ns_entry.file_type()?.is_dir() {
                continue;
            }
            let ns_name = ns_entry.file_name().to_string_lossy().to_string();
            let mut levels: Vec<Vec<sstable::SSTableReader>> = Vec::new();

            for level in 0..max_levels {
                let level_dir = db_path.join("sst").join(&ns_name).join(format!("L{level}"));
                if !level_dir.exists() {
                    levels.push(Vec::new());
                    continue;
                }

                let mut files: Vec<(u64, PathBuf)> = Vec::new();
                for file_entry in fs::read_dir(&level_dir)? {
                    let file_entry = file_entry?;
                    let fname = file_entry.file_name().to_string_lossy().to_string();
                    if let Some(seq_str) = fname.strip_suffix(".sst") {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            files.push((seq, file_entry.path()));
                            if seq > max_seq {
                                max_seq = seq;
                            }
                        }
                    }
                }

                // L0: sort descending (newest first); L1+: sort ascending (key order)
                if level == 0 {
                    files.sort_by(|a, b| b.0.cmp(&a.0));
                } else {
                    files.sort_by(|a, b| a.0.cmp(&b.0));
                }

                let mut readers = Vec::with_capacity(files.len());
                for (seq, path) in &files {
                    readers.push(sstable::SSTableReader::open(
                        path,
                        *seq,
                        block_cache.clone(),
                        io,
                    )?);
                }
                levels.push(readers);
            }

            // Only insert if at least one level has readers
            if levels.iter().any(|l| !l.is_empty()) {
                result.insert(ns_name, levels);
            }
        }

        Ok((result, max_seq))
    }

    pub(crate) fn inc_op_puts(&self) {
        self.op_puts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_op_puts_by(&self, n: u64) {
        self.op_puts.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn inc_op_gets(&self) {
        self.op_gets.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_op_deletes(&self) {
        self.op_deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_op_deletes_by(&self, n: u64) {
        self.op_deletes.fetch_add(n, Ordering::Relaxed);
    }

    /// Load operation counters from `stats.meta`. Returns (0,0,0) if the file
    /// is missing or malformed.
    fn load_stats_meta(path: &Path) -> (u64, u64, u64) {
        let meta_path = path.join(STATS_META);
        let data = match fs::read(&meta_path) {
            Ok(d) => d,
            Err(_) => return (0, 0, 0),
        };
        // Format: [magic:4][version:2][op_puts:8][op_gets:8][op_deletes:8] = 30 bytes
        if data.len() < 30 {
            return (0, 0, 0);
        }
        if &data[0..4] != STATS_MAGIC {
            return (0, 0, 0);
        }
        let version = u16::from_be_bytes([data[4], data[5]]);
        if version != STATS_VERSION {
            return (0, 0, 0);
        }
        // SAFETY: data.len() >= 30 checked above — slices are exactly 8 bytes each
        let puts = u64::from_be_bytes(data[6..14].try_into().unwrap());
        let gets = u64::from_be_bytes(data[14..22].try_into().unwrap());
        let deletes = u64::from_be_bytes(data[22..30].try_into().unwrap());
        (puts, gets, deletes)
    }

    /// Persist operation counters to `stats.meta` via atomic write-to-temp + rename.
    fn save_stats_meta(&self) {
        let meta_path = self.config.path.join(STATS_META);
        let tmp_path = self.config.path.join("stats.meta.tmp");
        let mut buf = Vec::with_capacity(30);
        buf.extend_from_slice(STATS_MAGIC);
        buf.extend_from_slice(&STATS_VERSION.to_be_bytes());
        buf.extend_from_slice(&self.op_puts.load(Ordering::Relaxed).to_be_bytes());
        buf.extend_from_slice(&self.op_gets.load(Ordering::Relaxed).to_be_bytes());
        buf.extend_from_slice(&self.op_deletes.load(Ordering::Relaxed).to_be_bytes());
        if fs::write(&tmp_path, &buf).is_ok() {
            let _ = fs::rename(&tmp_path, &meta_path);
        }
    }

    pub(crate) fn get_or_create_memtable(&self, name: &str) -> &Mutex<memtable::MemTable> {
        // Fast path: read lock to check if memtable already exists
        {
            let map = self
                .namespace_data
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if map.contains_key(name) {
                // SAFETY: The RwLock<HashMap> only grows (we never remove entries),
                // so a reference obtained under the read lock remains valid.
                let ptr = map.get(name).unwrap() as *const Mutex<memtable::MemTable>;
                return unsafe { &*ptr };
            }
        }

        // Slow path: write lock to insert
        let mut map = self
            .namespace_data
            .write()
            .unwrap_or_else(|e| e.into_inner());
        map.entry(name.to_owned())
            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
        let ptr = map.get(name).unwrap() as *const Mutex<memtable::MemTable>;
        // SAFETY: Same as above — the HashMap only grows, so the reference is stable.
        unsafe { &*ptr }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.stop_replication();
        self.flush_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
        self.compaction_stop.store(true, Ordering::Relaxed);
        {
            let (lock, cvar) = &*self.compaction_notify;
            let mut pending = lock.lock().unwrap_or_else(|e| e.into_inner());
            *pending = true;
            cvar.notify_one();
        }
        if let Some(handle) = self.compaction_thread.take() {
            let _ = handle.join();
        }
        self.save_stats_meta();
    }
}

/// Decode a 64-char hex string into a 32-byte array.
/// Returns `false` if the string is not valid hex or wrong length.
fn hex_decode(hex: &str, out: &mut [u8; 32]) -> bool {
    if hex.len() != 64 {
        return false;
    }
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = match hex_nibble(chunk[0]) {
            Some(v) => v,
            None => return false,
        };
        let lo = match hex_nibble(chunk[1]) {
            Some(v) => v,
            None => return false,
        };
        out[i] = (hi << 4) | lo;
    }
    true
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_decode_valid() {
        let hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let mut out = [0u8; 32];
        assert!(hex_decode(hex, &mut out));
        assert_eq!(out[0], 0x00);
        assert_eq!(out[1], 0x11);
        assert_eq!(out[15], 0xFF);
    }

    #[test]
    fn hex_decode_wrong_length() {
        let mut out = [0u8; 32];
        assert!(!hex_decode("0011", &mut out)); // too short
        assert!(!hex_decode("", &mut out));
    }

    #[test]
    fn hex_decode_invalid_hi_nibble() {
        // 'g' in high nibble position
        let hex = "g0112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let mut out = [0u8; 32];
        assert!(!hex_decode(hex, &mut out));
    }

    #[test]
    fn hex_decode_invalid_lo_nibble() {
        // 'z' in low nibble position
        let hex = "0z112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let mut out = [0u8; 32];
        assert!(!hex_decode(hex, &mut out));
    }

    #[test]
    fn hex_decode_uppercase() {
        let hex = "AABBCCDD00112233445566778899EEFF00112233445566778899AABBCCDDEEFF";
        let mut out = [0u8; 32];
        assert!(hex_decode(hex, &mut out));
        assert_eq!(out[0], 0xAA);
        assert_eq!(out[1], 0xBB);
    }
}
