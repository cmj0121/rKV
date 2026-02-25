mod aol;
mod bloom;
mod checksum;
pub(crate) mod crypto;
mod dump;
mod error;
mod io;
mod key;
mod memtable;
mod namespace;
mod objects;
mod recovery;
mod revision;
mod sstable;
mod stats;
mod value;

pub use error::{Error, Result};
pub use key::Key;
pub use namespace::Namespace;
pub use recovery::RecoveryReport;
pub use revision::RevisionID;
pub use stats::Stats;
pub use value::Value;

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Debug)]
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
            verify_checksums: true,
            compression: Compression::default(),
            io_model: IoModel::default(),
            cluster_id: None,
            aol_buffer_size: 128,
            l0_max_count: 4,
            l0_max_size: 64 * 1024 * 1024,
            l1_max_size: 256 * 1024 * 1024,
            default_max_size: 2 * 1024 * 1024 * 1024,
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
    #[allow(dead_code)]
    io_backend: Box<dyn io::IoBackend>,
    revision_gen: revision::RevisionGen,
    namespace_data: RwLock<HashMap<String, Mutex<memtable::MemTable>>>,
    aol: Arc<Mutex<aol::Aol>>,
    object_stores: RwLock<HashMap<String, objects::ObjectStore>>,
    /// Per-namespace, per-level SSTable readers.
    /// `sstables[ns][level]` = Vec of readers.
    /// Level 0: newest-first (overlapping key ranges).
    /// Level 1+: key-order (non-overlapping key ranges after compaction).
    sstables: RwLock<LeveledSSTables>,
    /// Monotonically increasing counter for SSTable file naming.
    sst_sequence: AtomicU64,
    flush_stop: Arc<AtomicBool>,
    flush_thread: Option<JoinHandle<()>>,
    // Operation counters (persistent across restarts)
    op_puts: AtomicU64,
    op_gets: AtomicU64,
    op_deletes: AtomicU64,
}

impl DB {
    pub fn open(config: Config) -> Result<Self> {
        if config.create_if_missing {
            fs::create_dir_all(&config.path)?;
        }
        let io_backend = io::create_backend(&config.io_model);
        let revision_gen = revision::RevisionGen::new(config.cluster_id);

        // Replay AOL to reconstruct memtables
        let namespace_data = RwLock::new(HashMap::new());
        let (records, _skipped) = aol::Aol::replay(&config.path, config.verify_checksums)?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        {
            let mut map = namespace_data.write().unwrap();
            for record in records {
                // Skip expired records
                if record.expires_at_ms > 0 && record.expires_at_ms <= now_ms {
                    continue;
                }

                let mt = map
                    .entry(record.namespace)
                    .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
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

        // Per-namespace object stores (created lazily on first access)
        let object_stores = RwLock::new(HashMap::new());

        // Scan existing SSTable files across all levels and recover sequence counter
        let (sstables, sst_sequence) = Self::scan_sstables(&config.path, config.max_levels)?;

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
                        let mut aol = aol.lock().unwrap();
                        let _ = aol.flush_if_dirty();
                    }
                }
                // Final flush on shutdown
                let mut aol = aol.lock().unwrap();
                let _ = aol.flush_if_dirty();
            }))
        };

        // Load persisted operation counters
        let (op_puts, op_gets, op_deletes) = Self::load_stats_meta(&config.path);

        Ok(Self {
            config,
            opened_at: Instant::now(),
            encrypted_namespaces: Mutex::new(HashMap::new()),
            io_backend,
            revision_gen,
            namespace_data,
            aol,
            object_stores,
            sstables: RwLock::new(sstables),
            sst_sequence: AtomicU64::new(sst_sequence),
            flush_stop,
            flush_thread,
            op_puts: AtomicU64::new(op_puts),
            op_gets: AtomicU64::new(op_gets),
            op_deletes: AtomicU64::new(op_deletes),
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
        self.save_stats_meta();
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.config.path
    }

    pub fn stats(&self) -> Stats {
        let map = self.namespace_data.read().unwrap();
        let namespace_count = map.len() as u64;
        let mut total_keys: u64 = 0;
        let mut write_buffer_bytes: u64 = 0;
        for mt in map.values() {
            let mt = mt.lock().unwrap();
            total_keys += mt.count();
            write_buffer_bytes += mt.approximate_size() as u64;
        }

        Stats {
            total_keys,
            data_size_bytes: write_buffer_bytes,
            namespace_count,
            level_count: self.config.max_levels,
            write_buffer_bytes,
            op_puts: self.op_puts.load(Ordering::Relaxed),
            op_gets: self.op_gets.load(Ordering::Relaxed),
            op_deletes: self.op_deletes.load(Ordering::Relaxed),
            uptime: self.opened_at.elapsed(),
            ..Stats::default()
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

    /// Switch to a namespace, creating it if it does not exist.
    ///
    /// Pass `password: Some("...")` to open an encrypted namespace, or `None`
    /// for a non-encrypted one. The encryption state is recorded on first
    /// access and enforced on subsequent calls within the same session.
    pub fn namespace(&self, name: &str, password: Option<&str>) -> Result<Namespace<'_>> {
        let encrypted = password.is_some();
        let mut map = self.encrypted_namespaces.lock().unwrap();
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
            map.insert(name.to_owned(), encrypted);
        }
        drop(map);
        Namespace::open(self, name, password)
    }

    /// List all namespace names.
    ///
    /// Returns the sorted union of namespaces known to the in-memory
    /// MemTable map and the L0 SSTable cache.
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut names = std::collections::BTreeSet::new();

        {
            let map = self.namespace_data.read().unwrap();
            for key in map.keys() {
                names.insert(key.clone());
            }
        }
        {
            let sst = self.sstables.read().unwrap();
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
        if name == DEFAULT_NAMESPACE {
            return Err(Error::InvalidNamespace(
                "cannot drop the default namespace".into(),
            ));
        }
        if name.is_empty() {
            return Err(Error::InvalidNamespace(
                "namespace name must not be empty".into(),
            ));
        }

        // Check the namespace actually exists
        let exists = {
            let nd = self.namespace_data.read().unwrap();
            let sst = self.sstables.read().unwrap();
            nd.contains_key(name) || sst.contains_key(name)
        };
        if !exists {
            return Err(Error::InvalidNamespace(format!(
                "namespace '{name}' does not exist"
            )));
        }

        // 1. Remove from in-memory maps
        {
            let mut map = self.namespace_data.write().unwrap();
            map.remove(name);
        }
        {
            let mut map = self.sstables.write().unwrap();
            map.remove(name);
        }
        {
            let mut map = self.object_stores.write().unwrap();
            map.remove(name);
        }
        {
            let mut map = self.encrypted_namespaces.lock().unwrap();
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

        // 3. Flush remaining namespaces + truncate AOL so the dropped
        //    namespace's records don't resurrect on restart.
        //    flush() only truncates when it actually writes SSTables, so we
        //    force-truncate afterwards to cover the case where no other
        //    namespaces have pending data.
        self.flush()?;
        {
            let mut aol = self.aol.lock().unwrap();
            aol.truncate(&self.config.path)?;
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
        let namespaces: Vec<String> = {
            let map = self.namespace_data.read().unwrap();
            map.keys().cloned().collect()
        };

        let mut flushed_any = false;

        for ns_name in &namespaces {
            let entries = {
                let mt = self.get_or_create_memtable(ns_name);
                let mut mt = mt.lock().unwrap();
                if mt.is_empty() {
                    continue;
                }
                mt.drain_latest()
            };

            if entries.is_empty() {
                continue;
            }

            // Allocate a new sequence number and write the SSTable
            let seq = self.sst_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let l0_dir = self.sst_level_dir(ns_name, 0);
            fs::create_dir_all(&l0_dir)?;
            let sst_path = l0_dir.join(format!("{seq:06}.sst"));

            let mut writer = sstable::SSTableWriter::new(
                &sst_path,
                self.config.block_size,
                self.config.compression.clone(),
            )?;
            for (key, value) in &entries {
                writer.add(key, value)?;
            }
            writer.finish()?;

            // Open the reader and prepend to L0 cache (newest first)
            let reader = sstable::SSTableReader::open(&sst_path)?;
            let mut sst = self.sstables.write().unwrap();
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
            let mut aol = self.aol.lock().unwrap();
            aol.truncate(&self.config.path)?;
        }

        Ok(())
    }

    /// Flush and fsync all data to durable storage.
    pub fn sync(&self) -> Result<()> {
        Err(Error::NotImplemented("sync".into()))
    }

    // --- Destroy / Repair ---

    /// Destroy the database at the given path, deleting all data.
    pub fn destroy(path: impl Into<PathBuf>) -> Result<()> {
        let _path = path.into();
        Err(Error::NotImplemented("destroy".into()))
    }

    /// Attempt to repair a corrupted database at the given path.
    ///
    /// Returns a `RecoveryReport` describing what was scanned, recovered,
    /// and lost. Callers should inspect the report to determine whether
    /// the database is usable.
    pub fn repair(path: impl Into<PathBuf>) -> Result<RecoveryReport> {
        let _path = path.into();
        Err(Error::NotImplemented("repair".into()))
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

        let mut writer = dump::DumpWriter::new(&dump_path)?;
        writer.write_header(&self.config.path)?;

        let namespaces = self.list_namespaces()?;

        for ns_name in &namespaces {
            // Skip encrypted namespaces
            {
                let enc = self.encrypted_namespaces.lock().unwrap();
                if enc.get(ns_name).copied().unwrap_or(false) {
                    continue;
                }
            }

            // Merge all SSTable levels into a single sorted map (bottom-up,
            // newest wins) — same merge strategy as compaction.
            let merged = {
                let sst = self.sstables.read().unwrap();
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
                            for (key, value) in reader.iter_entries(self.config.verify_checksums)? {
                                merged.insert(key, value);
                            }
                        }
                    } else {
                        for reader in level_readers {
                            for (key, value) in reader.iter_entries(self.config.verify_checksums)? {
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

        let mut reader = dump::DumpReader::open(&dump_path)?;
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
    /// For each namespace, merges L0 into L1, then cascades through
    /// deeper levels when a level exceeds its size threshold. Tombstones
    /// are dropped only at the bottommost level (`max_levels - 1`).
    pub fn compact(&self) -> Result<()> {
        let namespaces: Vec<String> = {
            let sst = self.sstables.read().unwrap();
            sst.keys().cloned().collect()
        };

        for ns_name in &namespaces {
            self.compact_namespace(ns_name)?;
        }

        Ok(())
    }

    /// Compact a single namespace with cascading level merges.
    ///
    /// 1. Merge L0 → L1 (drop tombstones if L1 is the bottommost level).
    /// 2. For each subsequent level, if it exceeds its size threshold,
    ///    merge into the next level. Tombstones are dropped when the
    ///    target is the bottommost level (`max_levels - 1`).
    fn compact_namespace(&self, ns: &str) -> Result<()> {
        let max_levels = self.config.max_levels;

        // Nothing to do with fewer than 2 levels
        if max_levels < 2 {
            return Ok(());
        }

        // Nothing to compact if L0 is empty
        {
            let sst = self.sstables.read().unwrap();
            let has_l0 = sst
                .get(ns)
                .and_then(|levels| levels.first())
                .is_some_and(|l0| !l0.is_empty());
            if !has_l0 {
                return Ok(());
            }
        }

        // Step 1: merge L0 → L1
        let is_bottom = max_levels <= 2;
        self.merge_two_levels(ns, 0, 1, is_bottom)?;

        // Step 2: cascade through deeper levels
        for level in 1..max_levels - 1 {
            if self.level_total_size(ns, level) <= self.level_max_size(level) {
                break;
            }
            let target = level + 1;
            let drop = target >= max_levels - 1;
            self.merge_two_levels(ns, level, target, drop)?;
        }

        Ok(())
    }

    /// Merge all SSTables from `source_level` into `target_level`.
    ///
    /// Target entries are loaded first (oldest), then source entries
    /// (newer wins via BTreeMap). For L0 source, readers are iterated
    /// in reverse (oldest-to-newest); for L1+ source, natural order.
    ///
    /// If `drop_tombstones` is true, tombstones are filtered from the
    /// output (safe only when target is the bottommost level).
    ///
    /// Returns the output SSTable size in bytes (0 if nothing to merge).
    fn merge_two_levels(
        &self,
        ns: &str,
        source_level: usize,
        target_level: usize,
        drop_tombstones: bool,
    ) -> Result<usize> {
        let (source_paths, merged) = {
            let sst = self.sstables.read().unwrap();
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

            // Merge all entries: process oldest-to-newest so newer values
            // overwrite older ones in the BTreeMap.
            let mut merged = std::collections::BTreeMap::<Key, Value>::new();

            // Target entries are oldest
            for reader in target {
                for (key, value) in reader.iter_entries(self.config.verify_checksums)? {
                    merged.insert(key, value);
                }
            }

            // Source entries: L0 iterate oldest-to-newest (reverse of newest-first);
            // L1+ iterate in natural order.
            if source_level == 0 {
                for reader in source.iter().rev() {
                    for (key, value) in reader.iter_entries(self.config.verify_checksums)? {
                        merged.insert(key, value);
                    }
                }
            } else {
                for reader in source {
                    for (key, value) in reader.iter_entries(self.config.verify_checksums)? {
                        merged.insert(key, value);
                    }
                }
            }

            // Filter tombstones if requested
            if drop_tombstones {
                merged.retain(|_, v| !v.is_tombstone());
            }

            // Collect source file paths for cleanup by scanning disk
            let mut source_paths = Vec::new();
            for level in [source_level, target_level] {
                let level_dir = self.sst_level_dir(ns, level);
                if level_dir.exists() {
                    for entry in fs::read_dir(&level_dir)? {
                        let entry = entry?;
                        let fname = entry.file_name().to_string_lossy().to_string();
                        if fname.ends_with(".sst") {
                            source_paths.push(entry.path());
                        }
                    }
                }
            }

            (source_paths, merged)
        };

        if merged.is_empty() {
            // All entries were tombstones or empty — clean up source files
            for path in &source_paths {
                let _ = fs::remove_file(path);
            }
            let mut sst = self.sstables.write().unwrap();
            if let Some(levels) = sst.get_mut(ns) {
                if let Some(s) = levels.get_mut(source_level) {
                    s.clear();
                }
                if let Some(t) = levels.get_mut(target_level) {
                    t.clear();
                }
            }
            return Ok(0);
        }

        // Write merged output as a new SSTable in target level
        let seq = self.sst_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let target_dir = self.sst_level_dir(ns, target_level);
        fs::create_dir_all(&target_dir)?;
        let output_path = target_dir.join(format!("{seq:06}.sst"));

        let mut writer = sstable::SSTableWriter::new(
            &output_path,
            self.config.block_size,
            self.config.compression.clone(),
        )?;
        for (key, value) in &merged {
            writer.add(key, value)?;
        }
        writer.finish()?;

        // Delete old source files
        for path in &source_paths {
            let _ = fs::remove_file(path);
        }

        // Open the new reader and update the in-memory level structure
        let reader = sstable::SSTableReader::open(&output_path)?;
        let output_size = reader.size_bytes();
        let mut sst = self.sstables.write().unwrap();
        let levels = sst.entry(ns.to_owned()).or_insert_with(|| vec![Vec::new()]);

        // Clear source level
        if let Some(s) = levels.get_mut(source_level) {
            s.clear();
        }

        // Set target level to the single merged reader
        while levels.len() <= target_level {
            levels.push(Vec::new());
        }
        levels[target_level] = vec![reader];

        Ok(output_size)
    }

    /// Maximum size in bytes for a given level before it should be compacted.
    fn level_max_size(&self, level: usize) -> usize {
        match level {
            0 => usize::MAX, // L0 uses count/size triggers, not a cap
            1 => self.config.l1_max_size,
            _ => self.config.default_max_size,
        }
    }

    /// Total size in bytes of all SSTables at a given level for a namespace.
    fn level_total_size(&self, ns: &str, level: usize) -> usize {
        let sst = self.sstables.read().unwrap();
        sst.get(ns)
            .and_then(|levels| levels.get(level))
            .map(|readers| readers.iter().map(|r| r.size_bytes()).sum())
            .unwrap_or(0)
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

    pub(crate) fn append_to_aol(
        &self,
        ns: &str,
        rev: u128,
        key: &Key,
        value: &Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let mut aol = self.aol.lock().unwrap();
        aol.append(ns, rev, key, value, ttl)
    }

    pub(crate) fn get_or_create_object_store(&self, ns: &str) -> Result<&objects::ObjectStore> {
        // Fast path: read lock to check if store already exists
        {
            let map = self.object_stores.read().unwrap();
            if map.contains_key(ns) {
                // SAFETY: The RwLock<HashMap> only grows (we never remove entries),
                // so a reference obtained under the read lock remains valid.
                let ptr = map.get(ns).unwrap() as *const objects::ObjectStore;
                return Ok(unsafe { &*ptr });
            }
        }

        // Slow path: write lock to insert
        let mut map = self.object_stores.write().unwrap();
        if !map.contains_key(ns) {
            let store = objects::ObjectStore::open(&self.config.path, ns)?;
            map.insert(ns.to_owned(), store);
        }
        let ptr = map.get(ns).unwrap() as *const objects::ObjectStore;
        // SAFETY: Same as above — the HashMap only grows, so the reference is stable.
        Ok(unsafe { &*ptr })
    }

    /// Look up a key across all SSTable levels for a namespace.
    ///
    /// Searches L0 (newest-first), then L1, L2, etc. Returns:
    /// - `Ok(Some(value))` if found (may be `Tombstone`)
    /// - `Ok(None)` if not found in any SSTable
    pub(crate) fn get_from_sstables(&self, ns: &str, key: &Key) -> Result<Option<Value>> {
        let sst = self.sstables.read().unwrap();
        if let Some(levels) = sst.get(ns) {
            for level_readers in levels {
                for reader in level_readers {
                    if let Some(value) = reader.get(key, self.config.verify_checksums)? {
                        return Ok(Some(value));
                    }
                }
            }
        }
        Ok(None)
    }

    /// SSTable directory for a namespace: `<db>/sst/<namespace>/`.
    fn sst_namespace_dir(&self, ns: &str) -> PathBuf {
        self.config.path.join("sst").join(ns)
    }

    /// SSTable directory for a specific level: `<db>/sst/<namespace>/L<level>/`.
    fn sst_level_dir(&self, ns: &str, level: usize) -> PathBuf {
        self.config
            .path
            .join("sst")
            .join(ns)
            .join(format!("L{level}"))
    }

    /// Scan existing SSTable files across all levels on startup.
    ///
    /// Walks `<db>/sst/<namespace>/L<n>/` directories, opens each `.sst`
    /// file, and returns the per-namespace leveled reader lists plus the
    /// next sequence number to use.
    fn scan_sstables(db_path: &Path, max_levels: usize) -> Result<(LeveledSSTables, u64)> {
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
                for (_seq, path) in &files {
                    readers.push(sstable::SSTableReader::open(path)?);
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
            let map = self.namespace_data.read().unwrap();
            if map.contains_key(name) {
                // SAFETY: The RwLock<HashMap> only grows (we never remove entries),
                // so a reference obtained under the read lock remains valid.
                let ptr = map.get(name).unwrap() as *const Mutex<memtable::MemTable>;
                return unsafe { &*ptr };
            }
        }

        // Slow path: write lock to insert
        let mut map = self.namespace_data.write().unwrap();
        map.entry(name.to_owned())
            .or_insert_with(|| Mutex::new(memtable::MemTable::new()));
        let ptr = map.get(name).unwrap() as *const Mutex<memtable::MemTable>;
        // SAFETY: Same as above — the HashMap only grows, so the reference is stable.
        unsafe { &*ptr }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.flush_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
        self.save_stats_meta();
    }
}
