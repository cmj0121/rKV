mod aol;
mod bloom;
mod checksum;
mod error;
mod io;
mod key;
mod memtable;
mod namespace;
mod objects;
mod recovery;
mod revision;
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
use std::sync::atomic::{AtomicBool, Ordering};
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
        }
    }
}

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
    flush_stop: Arc<AtomicBool>,
    flush_thread: Option<JoinHandle<()>>,
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

        Ok(Self {
            config,
            opened_at: Instant::now(),
            encrypted_namespaces: Mutex::new(HashMap::new()),
            io_backend,
            revision_gen,
            namespace_data,
            aol,
            object_stores,
            flush_stop,
            flush_thread,
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.config.path
    }

    pub fn stats(&self) -> Stats {
        Stats {
            level_count: self.config.max_levels,
            uptime: self.opened_at.elapsed(),
            ..Stats::default()
        }
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
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(Error::NotImplemented("list_namespaces".into()))
    }

    /// Drop a namespace and all its data. The default namespace cannot be dropped.
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
        Err(Error::NotImplemented("drop_namespace".into()))
    }

    // --- Flush / Sync ---

    /// Flush the in-memory write buffer to disk.
    pub fn flush(&self) -> Result<()> {
        Err(Error::NotImplemented("flush".into()))
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
    pub fn dump(&self, path: impl Into<PathBuf>) -> Result<()> {
        let _path = path.into();
        Err(Error::NotImplemented("dump".into()))
    }

    /// Import a database from a portable backup file.
    pub fn load(path: impl Into<PathBuf>) -> Result<DB> {
        let _path = path.into();
        Err(Error::NotImplemented("load".into()))
    }

    // --- Compaction ---

    /// Trigger a manual compaction of SSTable levels.
    pub fn compact(&self) -> Result<()> {
        Err(Error::NotImplemented("compact".into()))
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
    }
}
