use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use super::cache::{self, ShardedBlockCache};
use super::checksum::Checksum;
use super::error::{bytes_to_array, Error, Result};
use super::filter::KeyFilter;
use super::io::{IoBackend, IoBytes};
use super::key::Key;
use super::revision::RevisionID;
use super::value::Value;
use super::Compression;

use cache::estimate_block_size;

/// Raw entry parsed from a data block: (key_bytes, revision, expires_at_ms, value_tag, value_data).
pub(crate) type RawEntry = (Vec<u8>, u128, u64, u8, Vec<u8>);

// --- Constants ---

/// SSTable file magic bytes: "rKVS".
const MAGIC: [u8; 4] = *b"rKVS";

/// Current SSTable format version (V4 adds per-entry expires_at_ms).
const FORMAT_VERSION: u16 = 4;

/// Minimum format version this reader can handle.
const MIN_SUPPORTED_VERSION: u16 = 1;

/// V1 footer size in bytes (original format).
const V1_FOOTER_SIZE: usize = 48;

/// V2 footer size in bytes (adds features bitmask + reserved).
pub(crate) const V2_FOOTER_SIZE: usize = 56;

/// Feature flag: blocks contain restart point trailers for binary search.
const FEATURE_RESTART_POINTS: u32 = 0x01;

/// Known feature flags bitmask. Unknown bits trigger a reject.
const KNOWN_FEATURES: u32 = FEATURE_RESTART_POINTS;

/// Number of entries between restart points within a data block.
const RESTART_INTERVAL: u32 = 16;

/// Compression tag stored per block on disk.
const COMPRESS_NONE: u8 = 0x00;
const COMPRESS_LZ4: u8 = 0x01;
const COMPRESS_ZSTD: u8 = 0x02;

// --- SSTableWriter ---

/// Writes sorted key-value entries to an SSTable file.
///
/// Usage: call `add()` for each entry in sorted key order, then `finish()`
/// to flush the final block and write the index + footer.
pub(crate) struct SSTableWriter {
    /// File path for cleanup on write failure.
    path: PathBuf,
    /// Target file handle.
    file: fs::File,
    /// Block size threshold in bytes.
    block_size: usize,
    /// Compression algorithm for data blocks.
    compression: Compression,

    /// Accumulated entries for the current data block (uncompressed).
    block_buf: Vec<u8>,
    /// Last key added to the current block (for index).
    block_last_key: Option<Vec<u8>>,
    /// Number of entries in the current block.
    block_entry_count: u32,

    /// Current write offset in the file.
    offset: u64,
    /// Index entries: (last_key_bytes, block_offset, block_size_on_disk).
    index: Vec<(Vec<u8>, u64, u32)>,
    /// Total entry count across all blocks.
    entry_count: u64,
    /// Key filter builder (collects key hashes during writes).
    filter: KeyFilter,
    /// Prefix filter builder (collects prefix hashes during writes).
    prefix_filter: Option<KeyFilter>,
    /// Prefix length for prefix filter (0 = disabled).
    filter_prefix_len: usize,
    /// Restart point byte offsets within the current block.
    restarts: Vec<u32>,
}

impl SSTableWriter {
    /// Create a new SSTable writer at the given path.
    ///
    /// `bloom_bits` controls the filter: 10 = ~1% FPR, 0 = disabled.
    /// `bloom_prefix_len` controls the prefix filter (0 = disabled).
    /// `filter_policy` selects Bloom or Ribbon.
    pub(crate) fn new(
        path: &Path,
        block_size: usize,
        compression: Compression,
        bloom_bits: usize,
        bloom_prefix_len: usize,
        filter_policy: super::FilterPolicy,
        io: &dyn IoBackend,
    ) -> Result<Self> {
        let file = io.create_file(path)?;
        let make_filter = |bits: usize| match filter_policy {
            super::FilterPolicy::Bloom => KeyFilter::bloom(bits),
            super::FilterPolicy::Ribbon => KeyFilter::ribbon(bits),
        };
        let prefix_filter = if bloom_prefix_len > 0 && bloom_bits > 0 {
            Some(make_filter(bloom_bits))
        } else {
            None
        };
        Ok(Self {
            path: path.to_path_buf(),
            file,
            block_size,
            compression,
            block_buf: Vec::new(),
            block_last_key: None,
            block_entry_count: 0,
            offset: 0,
            index: Vec::new(),
            entry_count: 0,
            filter: make_filter(bloom_bits),
            prefix_filter,
            filter_prefix_len: bloom_prefix_len,
            restarts: Vec::new(),
        })
    }

    /// Add a key-value entry. Keys MUST be added in sorted order.
    ///
    /// `expires_at_ms` is the absolute epoch time in milliseconds when this
    /// entry expires, or 0 for no expiration.
    pub(crate) fn add(
        &mut self,
        key: &Key,
        value: &Value,
        revision: RevisionID,
        expires_at_ms: u64,
    ) -> Result<()> {
        let key_bytes = key.to_bytes();
        self.filter.insert(&key_bytes);

        // Insert prefix into prefix filter if enabled
        if let Some(ref mut pf) = self.prefix_filter {
            let prefix_len = self.filter_prefix_len.min(key_bytes.len());
            pf.insert(&key_bytes[..prefix_len]);
        }

        let value_data = value_to_data(value);
        let value_tag = value.to_tag();

        // Restart points enable O(log N + 16) binary search within a block.
        // Every RESTART_INTERVAL entries, we record the byte offset of the
        // next entry. Point lookups binary-search the restart offsets to find
        // the right 16-entry window, then linear-scan within it.
        if self.block_entry_count.is_multiple_of(RESTART_INTERVAL) {
            self.restarts.push(self.block_buf.len() as u32);
        }

        // V4 entry: [key_len: u16 BE][key_bytes][revision: u128 BE][expires_at_ms: u64 BE][value_tag: u8][value_len: u32 BE][value_data]
        let key_len = key_bytes.len() as u16;
        let value_len = value_data.len() as u32;
        self.block_buf.extend_from_slice(&key_len.to_be_bytes());
        self.block_buf.extend_from_slice(&key_bytes);
        self.block_buf
            .extend_from_slice(&revision.as_u128().to_be_bytes());
        self.block_buf
            .extend_from_slice(&expires_at_ms.to_be_bytes());
        self.block_buf.push(value_tag);
        self.block_buf.extend_from_slice(&value_len.to_be_bytes());
        self.block_buf.extend_from_slice(&value_data);

        self.block_last_key = Some(key_bytes);
        self.block_entry_count += 1;
        self.entry_count += 1;

        // Flush block if it exceeds the threshold
        if self.block_buf.len() >= self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Finish writing: flush any remaining entries, write filter, index, and footer.
    /// On failure, removes the partial file to avoid leaving corrupted state on disk.
    pub(crate) fn finish(mut self) -> Result<()> {
        let result = self.finish_inner();
        if result.is_err() {
            let _ = fs::remove_file(&self.path);
        }
        result
    }

    fn finish_inner(&mut self) -> Result<()> {
        // Flush remaining entries
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }

        // Build filter block
        let key_filter_data = self.filter.build();
        let prefix_filter_data = self
            .prefix_filter
            .as_mut()
            .map(|pf| pf.build())
            .unwrap_or_default();

        // Write filter block:
        //   Legacy (0x00): filter block = key filter only
        //   Compound (0x01): [key_filter_len: u32 LE][key_filter_data][prefix_filter_data]
        let filter_offset = self.offset;
        let has_prefix = !prefix_filter_data.is_empty();
        let filter_data = if has_prefix {
            // Compound format:
            // [key_filter_len: u32 LE][key_filter_data][prefix_len: u8][prefix_filter_data]
            let mut buf = Vec::new();
            buf.extend_from_slice(&(key_filter_data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&key_filter_data);
            buf.push(self.filter_prefix_len as u8);
            buf.extend_from_slice(&prefix_filter_data);
            buf
        } else {
            key_filter_data
        };
        let filter_size = filter_data.len() as u32;
        if !filter_data.is_empty() {
            self.file.write_all(&filter_data)?;
            self.offset += filter_data.len() as u64;
        }

        // Write index block
        let index_offset = self.offset;
        let index_data = self.encode_index();
        self.file.write_all(&index_data)?;
        let index_size = index_data.len() as u32;
        self.offset += index_data.len() as u64;

        // Write footer
        let filter_format = if has_prefix { 0x01 } else { 0x00 };
        let footer = self.encode_footer(
            index_offset,
            index_size,
            filter_offset,
            filter_size,
            filter_format,
            FEATURE_RESTART_POINTS,
        );
        self.file.write_all(&footer)?;

        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Flush the current block to disk: compress, checksum, write.
    fn flush_block(&mut self) -> Result<()> {
        let block_offset = self.offset;

        // Append restart point trailer: [restart_0: u32 LE]...[num_restarts: u32 LE]
        for &offset in &self.restarts {
            self.block_buf.extend_from_slice(&offset.to_le_bytes());
        }
        self.block_buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.restarts.clear();

        let (tag, payload) = compress_block(&self.block_buf, &self.compression)?;

        // On-disk format: [compression_tag: u8][payload][checksum: 5B]
        let mut block_on_disk = Vec::with_capacity(1 + payload.len() + Checksum::encoded_size());
        block_on_disk.push(tag);
        block_on_disk.extend_from_slice(&payload);

        let checksum = Checksum::compute(&block_on_disk);
        block_on_disk.extend_from_slice(&checksum.to_bytes());

        self.file.write_all(&block_on_disk)?;

        let block_size = block_on_disk.len() as u32;
        self.offset += block_size as u64;

        // Record index entry
        let last_key = self.block_last_key.take().ok_or_else(|| {
            Error::Corruption("SSTable flush_block called with no entries".into())
        })?;
        self.index.push((last_key, block_offset, block_size));

        // Reset block state
        self.block_buf.clear();
        self.block_entry_count = 0;

        Ok(())
    }

    /// Encode the index block.
    ///
    /// Format: repeated [key_len: u16 BE][key_bytes][offset: u64 BE][size: u32 BE]
    fn encode_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key_bytes, offset, size) in &self.index {
            let key_len = key_bytes.len() as u16;
            buf.extend_from_slice(&key_len.to_be_bytes());
            buf.extend_from_slice(key_bytes);
            buf.extend_from_slice(&offset.to_be_bytes());
            buf.extend_from_slice(&size.to_be_bytes());
        }
        buf
    }

    /// Encode the V2 footer (56 bytes).
    ///
    /// Layout:
    /// ```text
    /// [magic: 4B][version: u16 BE][entry_count: u64 BE]
    /// [index_offset: u64 BE][index_size: u32 BE]
    /// [data_blocks: u32 BE][filter_offset: u64 BE][filter_size: u32 BE]
    /// [filter_format: 1B][features: u32 BE][reserved: 4B]
    /// [checksum: 5B CRC32C]
    /// ```
    fn encode_footer(
        &self,
        index_offset: u64,
        index_size: u32,
        filter_offset: u64,
        filter_size: u32,
        filter_format: u8,
        features: u32,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(V2_FOOTER_SIZE);
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        buf.extend_from_slice(&self.entry_count.to_be_bytes());
        buf.extend_from_slice(&index_offset.to_be_bytes());
        buf.extend_from_slice(&index_size.to_be_bytes());
        buf.extend_from_slice(&(self.index.len() as u32).to_be_bytes());
        buf.extend_from_slice(&filter_offset.to_be_bytes());
        buf.extend_from_slice(&filter_size.to_be_bytes());
        buf.push(filter_format);
        // V2 fields: features bitmask + 4 reserved bytes
        buf.extend_from_slice(&features.to_be_bytes());
        buf.extend_from_slice(&[0u8; 4]); // reserved
                                          // Checksum covers first 51 bytes (56 − 5)
        let checksum = Checksum::compute(&buf);
        buf.extend_from_slice(&checksum.to_bytes());
        debug_assert_eq!(buf.len(), V2_FOOTER_SIZE);
        buf
    }
}

// --- Helpers ---

/// Extract the raw data bytes for on-disk value encoding.
fn value_to_data(value: &Value) -> Vec<u8> {
    match value {
        Value::Data(bytes) => bytes.clone(),
        Value::Null | Value::Tombstone => Vec::new(),
        Value::Pointer(vp) => vp.to_bytes(),
    }
}

/// Compress a block payload, returning (compression_tag, compressed_bytes).
fn compress_block(data: &[u8], compression: &Compression) -> Result<(u8, Vec<u8>)> {
    match compression {
        Compression::None => Ok((COMPRESS_NONE, data.to_vec())),
        Compression::LZ4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            Ok((COMPRESS_LZ4, compressed))
        }
        Compression::Zstd => {
            let compressed =
                zstd::encode_all(data, 3).map_err(|e| Error::Io(std::io::Error::other(e)))?;
            Ok((COMPRESS_ZSTD, compressed))
        }
    }
}

/// Decompress a block payload given its compression tag.
pub(super) fn decompress_block(tag: u8, data: &[u8]) -> Result<Vec<u8>> {
    match tag {
        COMPRESS_NONE => Ok(data.to_vec()),
        COMPRESS_LZ4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| Error::Corruption(format!("LZ4 decompression failed: {e}"))),
        COMPRESS_ZSTD => zstd::decode_all(data)
            .map_err(|e| Error::Corruption(format!("Zstd decompression failed: {e}"))),
        _ => Err(Error::Corruption(format!(
            "unknown compression tag: 0x{tag:02x}"
        ))),
    }
}

// --- SSTableReader ---

/// Index entry parsed from an SSTable's index block.
#[derive(Clone)]
pub(crate) struct IndexEntry {
    /// Last key in this data block (serialized bytes).
    pub(crate) last_key: Vec<u8>,
    /// Byte offset of the data block in the file.
    pub(crate) offset: u64,
    /// Size of the data block on disk (including compression tag + checksum).
    pub(crate) size: u32,
}

/// Lazily-parsed SSTable metadata: index, filters, and first key.
///
/// Stored inside `OnceLock` and initialized on first access.
struct LazyMeta {
    index: Vec<IndexEntry>,
    filter: KeyFilter,
    prefix_filter: Option<KeyFilter>,
    filter_prefix_len: usize,
    first_key: Option<Vec<u8>>,
}

/// Offsets extracted from the footer for deferred index/filter parsing.
struct FooterOffsets {
    index_offset: usize,
    index_size: usize,
    filter_offset: usize,
    filter_size: usize,
    filter_format: u8,
}

/// Zero-copy view over the restart point trailer in a decompressed block.
///
/// The trailer is a contiguous array of little-endian `u32` offsets.
/// This avoids allocating a `Vec<u32>` on every block access.
pub(crate) struct RestartIndex<'a>(&'a [u8]);

impl<'a> RestartIndex<'a> {
    /// Number of restart points.
    fn len(&self) -> usize {
        self.0.len() / 4
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the restart offset at index `i`.
    fn get(&self, i: usize) -> u32 {
        let off = i * 4;
        u32::from_le_bytes([
            self.0[off],
            self.0[off + 1],
            self.0[off + 2],
            self.0[off + 3],
        ])
    }
}

/// Reads key-value entries from an SSTable file.
///
/// Opens a file, parses the footer eagerly (for validation), then defers
/// index, bloom filter, and first-key parsing to first access via `OnceLock`.
pub(crate) struct SSTableReader {
    /// Raw file contents (read into memory or memory-mapped).
    data: Arc<IoBytes>,
    /// Total entry count from the footer.
    entry_count: u64,
    /// Unique SSTable identifier (sequence number from file naming).
    sst_id: u64,
    /// Shared LRU block cache for decompressed data blocks.
    cache: Option<Arc<ShardedBlockCache>>,
    /// Feature flags bitmask from the footer (0 for V1 files).
    #[allow(dead_code)] // accessed via #[cfg(test)] features() method
    features: u32,
    /// Format version from the footer (1, 2, or 3).
    version: u16,
    /// Whether blocks contain restart point trailers.
    has_restarts: bool,
    /// Footer offsets for deferred index/filter parsing.
    footer_offsets: FooterOffsets,
    /// Lazily-parsed metadata (index, bloom, first key).
    lazy_meta: OnceLock<std::result::Result<LazyMeta, String>>,
    /// Shared metrics for profiling instrumentation.
    #[cfg(feature = "profiling")]
    metrics: Option<Arc<super::metrics::Metrics>>,
}

impl SSTableReader {
    /// Open an SSTable file and parse its footer and index.
    pub(crate) fn open(
        path: &Path,
        sst_id: u64,
        cache: Option<Arc<ShardedBlockCache>>,
        io: &dyn IoBackend,
    ) -> Result<Self> {
        let data = Arc::new(io.read_file(path)?);

        // Detect footer version by probing: try V2 (56 bytes) first, then V1 (48 bytes).
        let (footer_start, footer_size) = if data.len() >= V2_FOOTER_SIZE {
            let candidate = data.len() - V2_FOOTER_SIZE;
            if data[candidate..candidate + 4] == MAGIC {
                (candidate, V2_FOOTER_SIZE)
            } else if data.len() >= V1_FOOTER_SIZE {
                let candidate = data.len() - V1_FOOTER_SIZE;
                if data[candidate..candidate + 4] == MAGIC {
                    (candidate, V1_FOOTER_SIZE)
                } else {
                    return Err(Error::Corruption(format!(
                        "SSTable bad magic: expected {MAGIC:?}, got {:?}",
                        &data[candidate..candidate + 4]
                    )));
                }
            } else {
                return Err(Error::Corruption(format!(
                    "SSTable too small: {} bytes (minimum {V1_FOOTER_SIZE})",
                    data.len()
                )));
            }
        } else if data.len() >= V1_FOOTER_SIZE {
            let candidate = data.len() - V1_FOOTER_SIZE;
            if data[candidate..candidate + 4] == MAGIC {
                (candidate, V1_FOOTER_SIZE)
            } else {
                return Err(Error::Corruption(format!(
                    "SSTable bad magic: expected {MAGIC:?}, got {:?}",
                    &data[candidate..candidate + 4]
                )));
            }
        } else {
            return Err(Error::Corruption(format!(
                "SSTable too small: {} bytes (minimum {V1_FOOTER_SIZE})",
                data.len()
            )));
        };

        let footer = &data[footer_start..footer_start + footer_size];

        // Verify footer checksum (last 5 bytes)
        let cksum_offset = footer_size - Checksum::encoded_size();
        let footer_checksum = Checksum::from_bytes(&footer[cksum_offset..])?;
        footer_checksum.verify(&footer[..cksum_offset])?;

        // Parse version
        let version = u16::from_be_bytes(bytes_to_array(&footer[4..6], "SSTable footer version")?);
        if !(MIN_SUPPORTED_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(Error::Corruption(format!(
                "SSTable unsupported version: {version} (supported: {MIN_SUPPORTED_VERSION}..{FORMAT_VERSION})"
            )));
        }

        // Parse features (V2+), reject unknown bits
        let features = if footer_size == V2_FOOTER_SIZE {
            let f = u32::from_be_bytes(bytes_to_array(&footer[43..47], "SSTable features")?);
            let unknown = f & !KNOWN_FEATURES;
            if unknown != 0 {
                return Err(Error::Corruption(format!(
                    "SSTable requires features 0x{unknown:08x} not supported by this version of rKV"
                )));
            }
            f
        } else {
            0
        };

        let entry_count =
            u64::from_be_bytes(bytes_to_array(&footer[6..14], "SSTable entry count")?);
        let index_offset =
            u64::from_be_bytes(bytes_to_array(&footer[14..22], "SSTable index offset")?) as usize;
        let index_size =
            u32::from_be_bytes(bytes_to_array(&footer[22..26], "SSTable index size")?) as usize;

        // Bounds-check the index block
        if index_offset + index_size > footer_start {
            return Err(Error::Corruption(format!(
                "SSTable index out of bounds: offset={index_offset}, size={index_size}, data_end={footer_start}"
            )));
        }

        // Parse filter metadata offsets (bytes 30..43 — same layout in V1 and V2)
        let filter_offset =
            u64::from_be_bytes(bytes_to_array(&footer[30..38], "SSTable filter offset")?) as usize;
        let filter_size =
            u32::from_be_bytes(bytes_to_array(&footer[38..42], "SSTable filter size")?) as usize;
        let filter_format = footer[42];

        let has_restarts = features & FEATURE_RESTART_POINTS != 0;

        let footer_offsets = FooterOffsets {
            index_offset,
            index_size,
            filter_offset,
            filter_size,
            filter_format,
        };

        Ok(Self {
            data,
            entry_count,
            sst_id,
            cache,
            features,
            version,
            has_restarts,
            footer_offsets,
            lazy_meta: OnceLock::new(),
            #[cfg(feature = "profiling")]
            metrics: None,
        })
    }

    /// Ensure lazy metadata is initialized, returning a reference to it.
    ///
    /// Parses the index, bloom filters, and first key on first call.
    /// Subsequent calls return the cached result. Corruption errors are
    /// permanently cached (corrupt SSTables don't self-heal).
    fn ensure_meta(&self) -> Result<&LazyMeta> {
        self.lazy_meta
            .get_or_init(|| Self::parse_lazy_meta(&self.data, &self.footer_offsets, self.version))
            .as_ref()
            .map_err(|msg| Error::Corruption(msg.clone()))
    }

    /// Parse all deferred metadata: index, bloom filters, first key.
    fn parse_lazy_meta(
        data: &[u8],
        fo: &FooterOffsets,
        version: u16,
    ) -> std::result::Result<LazyMeta, String> {
        // Bounds-check (defensive — open() already validated against footer_start)
        if fo.index_offset + fo.index_size > data.len() {
            return Err(format!(
                "SSTable index out of bounds: offset={}, size={}, data_len={}",
                fo.index_offset,
                fo.index_size,
                data.len()
            ));
        }

        // Parse index block
        let index_data = &data[fo.index_offset..fo.index_offset + fo.index_size];
        let index = Self::parse_index(index_data).map_err(|e| e.to_string())?;

        // Parse filters
        let (filter, prefix_filter, filter_prefix_len) =
            Self::parse_filters(data, fo).map_err(|e| e.to_string())?;

        // Extract first key from first block
        let first_key = Self::extract_first_key(data, &index, version);

        Ok(LazyMeta {
            index,
            filter,
            prefix_filter,
            filter_prefix_len,
            first_key,
        })
    }

    /// Parse filters from the filter block region.
    /// Auto-detects Bloom vs Ribbon via `KeyFilter::from_bytes`.
    fn parse_filters(
        data: &[u8],
        fo: &FooterOffsets,
    ) -> Result<(KeyFilter, Option<KeyFilter>, usize)> {
        if fo.filter_size == 0 || fo.filter_offset + fo.filter_size > data.len() {
            return Ok((KeyFilter::bloom(0), None, 0));
        }

        let filter_data = &data[fo.filter_offset..fo.filter_offset + fo.filter_size];

        match fo.filter_format {
            0x01 => {
                // Compound: [key_filter_len: u32 LE][key_filter_data]
                //           [prefix_len: u8][prefix_filter_data]
                if filter_data.len() < 4 {
                    return Ok((KeyFilter::bloom(0), None, 0));
                }
                let key_filter_len = u32::from_le_bytes(bytes_to_array(
                    &filter_data[0..4],
                    "SSTable filter key_filter_len",
                )?) as usize;
                let key_filter_end = 4 + key_filter_len;
                if key_filter_end >= filter_data.len() {
                    return Ok((KeyFilter::bloom(0), None, 0));
                }
                let key_filter = KeyFilter::from_bytes(&filter_data[4..key_filter_end])?;
                let prefix_len = filter_data[key_filter_end] as usize;
                let prefix_filter_start = key_filter_end + 1;
                let prefix_filter = if prefix_filter_start < filter_data.len() {
                    Some(KeyFilter::from_bytes(&filter_data[prefix_filter_start..])?)
                } else {
                    None
                };
                Ok((key_filter, prefix_filter, prefix_len))
            }
            _ => {
                // Legacy (0x00): filter block = key filter only
                Ok((KeyFilter::from_bytes(filter_data)?, None, 0))
            }
        }
    }

    /// Extract the first key from the first data block (best-effort).
    fn extract_first_key(data: &[u8], index: &[IndexEntry], _version: u16) -> Option<Vec<u8>> {
        let first_ie = index.first()?;
        let block_start = first_ie.offset as usize;
        let block_end = block_start + first_ie.size as usize;
        if block_end > data.len() {
            return None;
        }
        let block_on_disk = &data[block_start..block_end];
        let cksum_start = block_on_disk.len() - Checksum::encoded_size();
        let compression_tag = block_on_disk[0];
        let compressed_payload = &block_on_disk[1..cksum_start];
        let block_data = decompress_block(compression_tag, compressed_payload).ok()?;
        if block_data.len() >= 2 {
            let kl = u16::from_be_bytes(
                bytes_to_array(&block_data[0..2], "SSTable block key_len").ok()?,
            ) as usize;
            if 2 + kl <= block_data.len() {
                return Some(block_data[2..2 + kl].to_vec());
            }
        }
        None
    }

    /// Parse the index block into entries.
    fn parse_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        let mut entries = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            if pos + 2 > data.len() {
                return Err(Error::Corruption(
                    "SSTable index truncated at key_len".into(),
                ));
            }
            let key_len = u16::from_be_bytes(bytes_to_array(
                &data[pos..pos + 2],
                "SSTable index key_len",
            )?) as usize;
            pos += 2;

            if pos + key_len > data.len() {
                return Err(Error::Corruption("SSTable index truncated at key".into()));
            }
            let last_key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            if pos + 12 > data.len() {
                return Err(Error::Corruption(
                    "SSTable index truncated at offset/size".into(),
                ));
            }
            let offset =
                u64::from_be_bytes(bytes_to_array(&data[pos..pos + 8], "SSTable index offset")?);
            pos += 8;
            let size =
                u32::from_be_bytes(bytes_to_array(&data[pos..pos + 4], "SSTable index size")?);
            pos += 4;

            entries.push(IndexEntry {
                last_key,
                offset,
                size,
            });
        }
        Ok(entries)
    }

    /// Look up a key in the SSTable, returning the LATEST revision.
    ///
    /// Returns `Some((value, revision, expires_at_ms))` if found, `None` if
    /// the key is not present. When multiple entries exist for the same key
    /// (oldest-first order), returns the last match (latest revision).
    ///
    /// Uses the bloom filter for fast negative answers, binary search
    /// for block selection, and binary search within blocks via restart
    /// points (or `partition_point` on cached entries).
    #[cfg(test)]
    pub(crate) fn get(
        &self,
        key: &Key,
        verify_checksums: bool,
    ) -> Result<Option<(Value, RevisionID, u64)>> {
        let meta = self.ensure_meta()?;

        if meta.index.is_empty() {
            return Ok(None);
        }

        let key_buf = {
            super::metrics::prof_opt_timer!(self.metrics.as_deref(), prof_key_serialize);
            let mut buf = Vec::with_capacity(key.encoded_len());
            key.write_bytes_to(&mut buf);
            buf
        };

        self.get_with_key_bytes(&key_buf, verify_checksums)
    }

    /// Look up a key using pre-serialized key bytes.
    ///
    /// This avoids repeated key serialization when the same key is searched
    /// across multiple SSTables.
    pub(crate) fn get_with_key_bytes(
        &self,
        key_buf: &[u8],
        verify_checksums: bool,
    ) -> Result<Option<(Value, RevisionID, u64)>> {
        let meta = self.ensure_meta()?;

        if meta.index.is_empty() {
            return Ok(None);
        }

        #[cfg(feature = "profiling")]
        let prof_m = self.metrics.as_deref();

        // Key-range pre-filter: skip if key is outside [first_key, last_key]
        if let Some(ref first_key) = meta.first_key {
            if key_buf < first_key.as_slice() {
                return Ok(None);
            }
        }
        if let Some(last_ie) = meta.index.last() {
            if key_buf > last_ie.last_key.as_slice() {
                return Ok(None);
            }
        }

        // Bloom filter check: skip this SSTable if the key is definitely absent
        {
            super::metrics::prof_opt_timer!(prof_m, prof_sst_bloom_check);
            if !meta.filter.may_contain(key_buf) {
                return Ok(None);
            }
        }

        // Binary search: find the first block whose last_key >= target
        let block_idx = {
            super::metrics::prof_opt_timer!(prof_m, prof_sst_index_search);
            match meta
                .index
                .binary_search_by(|e| e.last_key.as_slice().cmp(key_buf))
            {
                Ok(i) => i,
                Err(i) => {
                    if i >= meta.index.len() {
                        return Ok(None); // key beyond all blocks
                    }
                    i
                }
            }
        };

        let mut last_match: Option<(Value, RevisionID, u64)> = None;

        // Scan starting block and continue to subsequent blocks that may
        // contain more entries for the same key.
        for bi in block_idx..meta.index.len() {
            let ie = &meta.index[bi];
            let found_in_block = {
                super::metrics::prof_opt_timer!(prof_m, prof_sst_block_read);
                self.get_from_block(ie, bi, key_buf, verify_checksums, &mut last_match)?
            };

            if found_in_block {
                if ie.last_key.as_slice() != key_buf {
                    return Ok(last_match);
                }
                // last_key == target → entries may continue in next block
            } else {
                break;
            }
        }

        Ok(last_match)
    }

    /// Search a single block for a key, updating `last_match` with the latest
    /// revision found. Returns `true` if any match was found in this block.
    ///
    /// Always uses restart-point binary search on raw decompressed bytes.
    /// On cache hit: uses cached decompressed bytes directly.
    /// On cache miss: decompresses, caches if cache is present, then searches.
    fn get_from_block(
        &self,
        ie: &IndexEntry,
        block_index: usize,
        key_bytes: &[u8],
        verify_checksums: bool,
        last_match: &mut Option<(Value, RevisionID, u64)>,
    ) -> Result<bool> {
        let block_data = self.get_raw_block(ie, block_index, verify_checksums)?;

        if self.has_restarts {
            let (entry_data, restarts) = Self::strip_restart_trailer(&block_data)?;
            if let Some(found) =
                Self::find_key_in_block(entry_data, &restarts, key_bytes, self.version)?
            {
                *last_match = Some(found);
                return Ok(true);
            }
            Ok(false)
        } else {
            // Legacy blocks without restart points: fall back to full parse + binary search
            let entries = Self::parse_block_entries(&block_data, self.version)?;
            Self::binary_search_entries(&entries, key_bytes, last_match)
        }
    }

    /// Binary search on parsed entries for a target key.
    ///
    /// Updates `last_match` with the latest revision (last entry) for the key.
    /// Returns `true` if any match was found.
    fn binary_search_entries(
        entries: &[RawEntry],
        key_bytes: &[u8],
        last_match: &mut Option<(Value, RevisionID, u64)>,
    ) -> Result<bool> {
        // partition_point: first index where key >= target
        let start = entries.partition_point(|e| e.0.as_slice() < key_bytes);
        let mut found = false;
        for entry in &entries[start..] {
            if entry.0.as_slice() == key_bytes {
                let value = Value::from_tag(entry.3, &entry.4)?;
                *last_match = Some((value, RevisionID::from(entry.1), entry.2));
                found = true;
            } else {
                break;
            }
        }
        Ok(found)
    }

    /// Look up ALL revisions for a key in the SSTable.
    ///
    /// Returns all matching entries in oldest-first order (the natural
    /// SSTable storage order). Uses bloom filter and binary search for
    /// fast block selection, with binary search within blocks via restart
    /// points or `partition_point` on cached entries.
    pub(crate) fn get_all_revisions(
        &self,
        key: &Key,
        verify_checksums: bool,
    ) -> Result<Vec<(Value, RevisionID, u64)>> {
        let meta = self.ensure_meta()?;

        if meta.index.is_empty() {
            return Ok(Vec::new());
        }

        let mut key_buf = Vec::with_capacity(key.encoded_len());
        key.write_bytes_to(&mut key_buf);

        // Key-range pre-filter
        if let Some(ref first_key) = meta.first_key {
            if key_buf.as_slice() < first_key.as_slice() {
                return Ok(Vec::new());
            }
        }
        if let Some(last_ie) = meta.index.last() {
            if key_buf.as_slice() > last_ie.last_key.as_slice() {
                return Ok(Vec::new());
            }
        }

        if !meta.filter.may_contain(&key_buf) {
            return Ok(Vec::new());
        }

        let block_idx = match meta
            .index
            .binary_search_by(|e| e.last_key.as_slice().cmp(&key_buf))
        {
            Ok(i) => i,
            Err(i) => {
                if i >= meta.index.len() {
                    return Ok(Vec::new());
                }
                i
            }
        };

        let mut result = Vec::new();

        for bi in block_idx..meta.index.len() {
            let ie = &meta.index[bi];
            let found_in_block =
                self.get_all_from_block(ie, bi, &key_buf, verify_checksums, &mut result)?;

            if found_in_block {
                if ie.last_key.as_slice() != key_buf.as_slice() {
                    return Ok(result);
                }
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Search a single block for all revisions of a key, appending to `result`.
    /// Returns `true` if any match was found.
    fn get_all_from_block(
        &self,
        ie: &IndexEntry,
        block_index: usize,
        key_bytes: &[u8],
        verify_checksums: bool,
        result: &mut Vec<(Value, RevisionID, u64)>,
    ) -> Result<bool> {
        let block_data = self.get_raw_block(ie, block_index, verify_checksums)?;

        if self.has_restarts {
            let (entry_data, restarts) = Self::strip_restart_trailer(&block_data)?;
            let found = Self::find_all_in_block(entry_data, &restarts, key_bytes, self.version)?;
            let any = !found.is_empty();
            result.extend(found);
            Ok(any)
        } else {
            let entries = Self::parse_block_entries(&block_data, self.version)?;
            Self::binary_search_all_entries(&entries, key_bytes, result)
        }
    }

    /// Binary search on parsed entries for all revisions of a target key.
    fn binary_search_all_entries(
        entries: &[RawEntry],
        key_bytes: &[u8],
        result: &mut Vec<(Value, RevisionID, u64)>,
    ) -> Result<bool> {
        let start = entries.partition_point(|e| e.0.as_slice() < key_bytes);
        let mut found = false;
        for entry in &entries[start..] {
            if entry.0.as_slice() == key_bytes {
                let value = Value::from_tag(entry.3, &entry.4)?;
                result.push((value, RevisionID::from(entry.1), entry.2));
                found = true;
            } else {
                break;
            }
        }
        Ok(found)
    }

    /// Parse all entries from a decompressed block.
    ///
    /// V4 format: `[key_len][key][revision: u128 BE][expires_at_ms: u64 BE][value_tag][value_len][value_data]`
    /// V3 format: `[key_len][key][revision: u128 BE][value_tag][value_len][value_data]`
    /// V1/V2 format: `[key_len][key][value_tag][value_len][value_data]` (revision = 0)
    pub(crate) fn parse_block_entries(block: &[u8], version: u16) -> Result<Vec<RawEntry>> {
        let mut entries = Vec::new();
        let mut pos = 0;
        while pos < block.len() {
            if pos + 2 > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at key_len".into(),
                ));
            }
            let kl = u16::from_be_bytes(bytes_to_array(
                &block[pos..pos + 2],
                "SSTable entry key_len",
            )?) as usize;
            pos += 2;

            if pos + kl > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at key_bytes".into(),
                ));
            }
            let key_bytes = block[pos..pos + kl].to_vec();
            pos += kl;

            // V3+: parse 16-byte revision after key
            let revision = if version >= 3 {
                if pos + 16 > block.len() {
                    return Err(Error::Corruption(
                        "SSTable entry truncated at revision".into(),
                    ));
                }
                let rev = u128::from_be_bytes(bytes_to_array(
                    &block[pos..pos + 16],
                    "SSTable entry revision",
                )?);
                pos += 16;
                rev
            } else {
                0u128
            };

            // V4+: parse 8-byte expires_at_ms after revision
            let expires_at_ms = if version >= 4 {
                if pos + 8 > block.len() {
                    return Err(Error::Corruption(
                        "SSTable entry truncated at expires_at_ms".into(),
                    ));
                }
                let ms = u64::from_be_bytes(bytes_to_array(
                    &block[pos..pos + 8],
                    "SSTable entry expires_at_ms",
                )?);
                pos += 8;
                ms
            } else {
                0u64
            };

            if pos + 1 > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at value_tag".into(),
                ));
            }
            let value_tag = block[pos];
            pos += 1;

            if pos + 4 > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at value_len".into(),
                ));
            }
            let vl = u32::from_be_bytes(bytes_to_array(
                &block[pos..pos + 4],
                "SSTable entry value_len",
            )?) as usize;
            pos += 4;

            if pos + vl > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at value_data".into(),
                ));
            }
            let value_data = block[pos..pos + vl].to_vec();
            pos += vl;

            entries.push((key_bytes, revision, expires_at_ms, value_tag, value_data));
        }
        Ok(entries)
    }

    /// Strip the restart point trailer from a decompressed block.
    ///
    /// Returns the entry data slice (without trailer) and a zero-copy
    /// `RestartIndex` over the raw restart bytes.
    /// Trailer format: `[restart_0: u32 LE]...[num_restarts: u32 LE]`
    pub(crate) fn strip_restart_trailer(block: &[u8]) -> Result<(&[u8], RestartIndex<'_>)> {
        if block.len() < 4 {
            return Err(Error::Corruption(
                "block too small for restart trailer".into(),
            ));
        }
        let num_restarts = u32::from_le_bytes(bytes_to_array(
            &block[block.len() - 4..],
            "SSTable restart num_restarts",
        )?) as usize;
        let trailer_size = (num_restarts + 1) * 4;
        if trailer_size > block.len() {
            return Err(Error::Corruption(format!(
                "restart trailer exceeds block: {num_restarts} restarts, block size {}",
                block.len()
            )));
        }
        let entry_end = block.len() - trailer_size;
        let restart_bytes = &block[entry_end..block.len() - 4];
        Ok((&block[..entry_end], RestartIndex(restart_bytes)))
    }

    /// Read the key at a given byte offset within entry data (no allocation).
    ///
    /// Returns `(key_slice, position_after_key)`.
    fn key_at_offset(entry_data: &[u8], pos: usize) -> Result<(&[u8], usize)> {
        if pos + 2 > entry_data.len() {
            return Err(Error::Corruption("restart: truncated at key_len".into()));
        }
        let kl = u16::from_be_bytes(bytes_to_array(
            &entry_data[pos..pos + 2],
            "SSTable restart key_len",
        )?) as usize;
        let key_end = pos + 2 + kl;
        if key_end > entry_data.len() {
            return Err(Error::Corruption("restart: truncated at key".into()));
        }
        Ok((&entry_data[pos + 2..key_end], key_end))
    }

    /// Skip past the value portion of an entry (after the key has been read).
    ///
    /// `pos` should point to the revision field (V3+) or value_tag (V1/V2).
    fn skip_entry_value(entry_data: &[u8], mut pos: usize, version: u16) -> Result<usize> {
        if version >= 3 {
            pos += 16; // revision: u128
        }
        if version >= 4 {
            pos += 8; // expires_at_ms: u64
        }
        pos += 1; // value_tag
        if pos + 4 > entry_data.len() {
            return Err(Error::Corruption("restart: truncated at value_len".into()));
        }
        let vl = u32::from_be_bytes(bytes_to_array(
            &entry_data[pos..pos + 4],
            "SSTable restart value_len",
        )?) as usize;
        pos += 4 + vl;
        if pos > entry_data.len() {
            return Err(Error::Corruption("restart: truncated at value_data".into()));
        }
        Ok(pos)
    }

    /// Parse only the value portion of an entry (revision, expires, tag, data).
    ///
    /// `pos` should point right after the key bytes (i.e. the `after_key`
    /// position from `key_at_offset`). Returns `(next_pos, revision,
    /// expires_at_ms, value)`.
    fn parse_entry_value(
        entry_data: &[u8],
        pos: usize,
        version: u16,
    ) -> Result<(usize, u128, u64, Value)> {
        let mut p = pos;

        let revision = if version >= 3 {
            if p + 16 > entry_data.len() {
                return Err(Error::Corruption("entry truncated at revision".into()));
            }
            let rev = u128::from_be_bytes(bytes_to_array(
                &entry_data[p..p + 16],
                "SSTable entry revision",
            )?);
            p += 16;
            rev
        } else {
            0u128
        };

        let expires_at_ms = if version >= 4 {
            if p + 8 > entry_data.len() {
                return Err(Error::Corruption("entry truncated at expires_at_ms".into()));
            }
            let ms = u64::from_be_bytes(bytes_to_array(
                &entry_data[p..p + 8],
                "SSTable entry expires_at_ms",
            )?);
            p += 8;
            ms
        } else {
            0u64
        };

        if p + 1 > entry_data.len() {
            return Err(Error::Corruption("entry truncated at value_tag".into()));
        }
        let value_tag = entry_data[p];
        p += 1;

        if p + 4 > entry_data.len() {
            return Err(Error::Corruption("entry truncated at value_len".into()));
        }
        let vl = u32::from_be_bytes(bytes_to_array(
            &entry_data[p..p + 4],
            "SSTable entry value_len",
        )?) as usize;
        p += 4;

        if p + vl > entry_data.len() {
            return Err(Error::Corruption("entry truncated at value_data".into()));
        }
        let value_data = &entry_data[p..p + vl];
        p += vl;

        let value = Value::from_tag(value_tag, value_data)?;
        Ok((p, revision, expires_at_ms, value))
    }

    /// Search restart point keys via binary search.
    ///
    /// Returns the index of the last restart point whose key <= `target`.
    fn search_restart_keys(
        entry_data: &[u8],
        restarts: &RestartIndex<'_>,
        target: &[u8],
    ) -> Result<usize> {
        let n = restarts.len();
        if n == 0 {
            return Err(Error::Corruption("no restart points in block".into()));
        }

        let mut lo = 0usize;
        let mut hi = n - 1;

        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let (key, _) = Self::key_at_offset(entry_data, restarts.get(mid) as usize)?;
            if key <= target {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        Ok(lo)
    }

    /// Find a key within a decompressed block using restart points.
    ///
    /// Binary-searches restart keys to locate the interval, then parses
    /// only entries in that interval. Returns the latest revision match
    /// (last entry with matching key, since entries are oldest-first).
    fn find_key_in_block(
        entry_data: &[u8],
        restarts: &RestartIndex<'_>,
        key_bytes: &[u8],
        version: u16,
    ) -> Result<Option<(Value, RevisionID, u64)>> {
        if restarts.is_empty() || entry_data.is_empty() {
            return Ok(None);
        }

        let ri = Self::search_restart_keys(entry_data, restarts, key_bytes)?;
        let mut pos = restarts.get(ri) as usize;
        let mut last_match: Option<(Value, RevisionID, u64)> = None;

        while pos < entry_data.len() {
            let (key, after_key) = Self::key_at_offset(entry_data, pos)?;

            if key == key_bytes {
                // Parse value portion only (key already read above — no redundant alloc)
                let (next_pos, rev, expires, value) =
                    Self::parse_entry_value(entry_data, after_key, version)?;
                last_match = Some((value, RevisionID::from(rev), expires));
                pos = next_pos;
            } else if key > key_bytes {
                break;
            } else {
                // Skip this non-matching entry (zero allocation)
                pos = Self::skip_entry_value(entry_data, after_key, version)?;
            }
        }

        Ok(last_match)
    }

    /// Find all revisions of a key within a decompressed block using restart points.
    fn find_all_in_block(
        entry_data: &[u8],
        restarts: &RestartIndex<'_>,
        key_bytes: &[u8],
        version: u16,
    ) -> Result<Vec<(Value, RevisionID, u64)>> {
        if restarts.is_empty() || entry_data.is_empty() {
            return Ok(Vec::new());
        }

        let ri = Self::search_restart_keys(entry_data, restarts, key_bytes)?;
        let mut pos = restarts.get(ri) as usize;
        let mut result = Vec::new();

        while pos < entry_data.len() {
            let (key, after_key) = Self::key_at_offset(entry_data, pos)?;

            if key == key_bytes {
                let (next_pos, rev, expires, value) =
                    Self::parse_entry_value(entry_data, after_key, version)?;
                result.push((value, RevisionID::from(rev), expires));
                pos = next_pos;
            } else if key > key_bytes {
                break;
            } else {
                pos = Self::skip_entry_value(entry_data, after_key, version)?;
            }
        }

        Ok(result)
    }

    /// Decompress a raw block from disk, verify checksum if requested.
    fn decompress_raw_block(&self, ie: &IndexEntry, verify_checksums: bool) -> Result<Vec<u8>> {
        let block_start = ie.offset as usize;
        let block_end = block_start + ie.size as usize;

        if block_end > self.data.len() {
            return Err(Error::Corruption(format!(
                "SSTable block out of bounds: {block_start}..{block_end} (file size {})",
                self.data.len()
            )));
        }

        let block_on_disk = &self.data[block_start..block_end];
        let cksum_start = block_on_disk.len() - Checksum::encoded_size();

        if verify_checksums {
            let checksum = Checksum::from_bytes(&block_on_disk[cksum_start..])?;
            checksum.verify(&block_on_disk[..cksum_start])?;
        }

        let compression_tag = block_on_disk[0];
        let compressed_payload = &block_on_disk[1..cksum_start];
        decompress_block(compression_tag, compressed_payload)
    }

    /// Get decompressed block bytes, using cache if available.
    ///
    /// On cache hit: returns cached bytes (promoted to MRU).
    /// On cache miss: decompresses from disk, inserts into cache if present.
    fn get_raw_block(
        &self,
        ie: &IndexEntry,
        block_index: usize,
        verify_checksums: bool,
    ) -> Result<Arc<Vec<u8>>> {
        // 1. Cache lookup
        if let Some(ref c) = self.cache {
            if let Some(data) = c.get(self.sst_id, block_index as u32) {
                #[cfg(feature = "profiling")]
                super::metrics::prof_opt_timer!(self.metrics.as_deref(), prof_sst_cache_hit);
                return Ok(data);
            }
        }

        #[cfg(feature = "profiling")]
        super::metrics::prof_opt_timer!(self.metrics.as_deref(), prof_sst_cache_miss);

        // 2. Decompress from disk
        let block_data = Arc::new(self.decompress_raw_block(ie, verify_checksums)?);

        // 3. Cache insert
        if let Some(ref c) = self.cache {
            let size = estimate_block_size(&block_data);
            c.insert_arc(
                self.sst_id,
                block_index as u32,
                Arc::clone(&block_data),
                size,
            );
        }

        Ok(block_data)
    }

    /// Read and decompress a single block, returning parsed entries.
    ///
    /// Uses `get_raw_block` for decompression and caching, then parses
    /// entries from the raw bytes.
    fn read_block(
        &self,
        ie: &IndexEntry,
        block_index: usize,
        verify_checksums: bool,
    ) -> Result<Vec<RawEntry>> {
        let block_data = self.get_raw_block(ie, block_index, verify_checksums)?;

        let entry_data = if self.has_restarts {
            let (entries_slice, _restarts) = Self::strip_restart_trailer(&block_data)?;
            entries_slice
        } else {
            &block_data
        };
        Self::parse_block_entries(entry_data, self.version)
    }

    /// Iterate all entries in sorted key order.
    ///
    /// Reads every data block, decompresses, and returns `(Key, Value, RevisionID)` triples.
    pub(crate) fn iter_entries(
        &self,
        verify_checksums: bool,
    ) -> Result<Vec<(Key, Value, RevisionID, u64)>> {
        let meta = self.ensure_meta()?;
        let mut result = Vec::with_capacity(self.entry_count as usize);

        for (bi, ie) in meta.index.iter().enumerate() {
            let entries = self.read_block(ie, bi, verify_checksums)?;
            for (key_bytes, revision, expires_at_ms, value_tag, value_data) in entries.iter() {
                let key = Key::from_bytes(key_bytes)?;
                let value = Value::from_tag(*value_tag, value_data)?;
                result.push((key, value, RevisionID::from(*revision), *expires_at_ms));
            }
        }

        Ok(result)
    }

    /// Return the SSTable identifier.
    pub(crate) fn sst_id(&self) -> u64 {
        self.sst_id
    }

    /// Return the feature flags bitmask (0 for V1 files).
    #[cfg(test)]
    pub(crate) fn features(&self) -> u32 {
        self.features
    }

    /// Return the total size of the SSTable data in bytes.
    pub(crate) fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Attach shared metrics for profiling instrumentation.
    #[cfg(feature = "profiling")]
    pub(crate) fn set_metrics(&mut self, metrics: Arc<super::metrics::Metrics>) {
        self.metrics = Some(metrics);
    }

    /// Return the total number of entries in this SSTable.
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Return the number of data blocks in this SSTable.
    #[cfg(test)]
    pub(crate) fn block_count(&self) -> Result<usize> {
        Ok(self.ensure_meta()?.index.len())
    }

    /// Return the Arc-wrapped raw data for iterator construction.
    pub(crate) fn data(&self) -> &Arc<IoBytes> {
        &self.data
    }

    /// Return the format version.
    pub(crate) fn version(&self) -> u16 {
        self.version
    }

    /// Return whether blocks have restart point trailers.
    pub(crate) fn has_restarts(&self) -> bool {
        self.has_restarts
    }

    /// Return cloned index entries for iterator construction.
    pub(crate) fn index_entries(&self) -> Result<Vec<IndexEntry>> {
        Ok(self.ensure_meta()?.index.clone())
    }

    /// Pre-populate the block cache with all data blocks from this SSTable.
    ///
    /// Each block is decompressed from disk and inserted into the cache.
    /// Blocks that are already cached are skipped (cache hit path).
    /// Errors on individual blocks are silently ignored — warming is
    /// best-effort and must not break the flush path.
    pub(crate) fn warm_cache(&self) {
        if self.cache.is_none() {
            return;
        }
        let meta = match self.ensure_meta() {
            Ok(m) => m,
            Err(_) => return,
        };
        for (i, ie) in meta.index.iter().enumerate() {
            let _ = self.get_raw_block(ie, i, false);
        }
    }

    /// Check prefix bloom filter for scan skip.
    pub(crate) fn may_contain_prefix_for_scan(&self, prefix_bytes: &[u8]) -> bool {
        let meta = match self.ensure_meta() {
            Ok(meta) => meta,
            Err(_) => return true,
        };
        if prefix_bytes.is_empty() {
            return true;
        }
        match &meta.prefix_filter {
            Some(pf) => {
                let query =
                    if meta.filter_prefix_len > 0 && prefix_bytes.len() >= meta.filter_prefix_len {
                        &prefix_bytes[..meta.filter_prefix_len]
                    } else {
                        prefix_bytes
                    };
                pf.may_contain(query)
            }
            None => true,
        }
    }
}

/// Decompress and parse a block from raw SSTable data without an SSTableReader.
///
/// Used by `SSTableScanIter` to read blocks lazily without holding the sstables
/// RwLock. Returns parsed `RawEntry` tuples.
pub(crate) fn read_block_from_data(
    data: &[u8],
    ie: &IndexEntry,
    version: u16,
    has_restarts: bool,
    verify_checksums: bool,
) -> Result<Vec<RawEntry>> {
    let block_start = ie.offset as usize;
    let block_end = block_start + ie.size as usize;

    if block_end > data.len() {
        return Err(Error::Corruption(format!(
            "SSTable block out of bounds: {block_start}..{block_end} (file size {})",
            data.len()
        )));
    }

    let block_on_disk = &data[block_start..block_end];
    let cksum_start = block_on_disk.len() - Checksum::encoded_size();

    if verify_checksums {
        let checksum = Checksum::from_bytes(&block_on_disk[cksum_start..])?;
        checksum.verify(&block_on_disk[..cksum_start])?;
    }

    let compression_tag = block_on_disk[0];
    let compressed_payload = &block_on_disk[1..cksum_start];
    let block_data = decompress_block(compression_tag, compressed_payload)?;

    let entry_data = if has_restarts {
        let (entries_slice, _restarts) = SSTableReader::strip_restart_trailer(&block_data)?;
        entries_slice
    } else {
        &block_data
    };
    SSTableReader::parse_block_entries(entry_data, version)
}

#[cfg(test)]
mod tests {
    use super::super::FilterPolicy;
    use super::*;

    fn io() -> super::super::io::BufferedIo {
        super::super::io::BufferedIo
    }

    #[test]
    fn compress_decompress_none() {
        let data = b"hello world";
        let (tag, compressed) = compress_block(data, &Compression::None).unwrap();
        assert_eq!(tag, COMPRESS_NONE);
        let decompressed = decompress_block(tag, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_decompress_lz4() {
        let data = b"hello world, this is a test for lz4 compression";
        let (tag, compressed) = compress_block(data, &Compression::LZ4).unwrap();
        assert_eq!(tag, COMPRESS_LZ4);
        let decompressed = decompress_block(tag, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_decompress_zstd() {
        let data = b"hello world, this is a test for zstd compression";
        let (tag, compressed) = compress_block(data, &Compression::Zstd).unwrap();
        assert_eq!(tag, COMPRESS_ZSTD);
        let decompressed = decompress_block(tag, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn decompress_unknown_tag() {
        let err = decompress_block(0xFF, b"data").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn footer_is_fixed_size() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut writer = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        writer
            .add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        writer.finish().unwrap();

        let data = fs::read(&path).unwrap();
        assert!(data.len() >= V2_FOOTER_SIZE);

        // Last 56 bytes are the V2 footer
        let footer = &data[data.len() - V2_FOOTER_SIZE..];
        assert_eq!(&footer[..4], &MAGIC);
    }

    #[test]
    fn writer_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut writer = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        writer
            .add(&Key::Int(1), &Value::from("hello"), RevisionID::ZERO, 0)
            .unwrap();
        writer
            .add(&Key::Int(2), &Value::from("world"), RevisionID::ZERO, 0)
            .unwrap();
        writer.finish().unwrap();

        assert!(path.exists());
        let data = fs::read(&path).unwrap();
        assert!(data.len() > V2_FOOTER_SIZE);
    }

    #[test]
    fn writer_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("multi.sst");
        // Very small block size to force multiple blocks
        let mut writer = SSTableWriter::new(
            &path,
            32,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..20 {
            writer
                .add(
                    &Key::Int(i),
                    &Value::from(format!("val{i}").as_str()),
                    RevisionID::ZERO,
                    0,
                )
                .unwrap();
        }
        writer.finish().unwrap();

        let data = fs::read(&path).unwrap();
        // Footer should have magic
        let footer = &data[data.len() - V2_FOOTER_SIZE..];
        assert_eq!(&footer[..4], &MAGIC);
    }

    // --- Reader: open & metadata ---

    #[test]
    fn reader_open_single_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(2), &Value::from("b"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.entry_count(), 2);
        assert_eq!(r.block_count().unwrap(), 1);
    }

    #[test]
    fn reader_open_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("multi.sst");
        let mut w = SSTableWriter::new(
            &path,
            32,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..20 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.entry_count(), 20);
        assert!(r.block_count().unwrap() > 1);
    }

    // --- Roundtrip: write then read back ---

    #[test]
    fn roundtrip_single_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(42), &Value::from("hello"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let val = r.get(&Key::Int(42), true).unwrap();
        assert_eq!(val, Some((Value::from("hello"), RevisionID::ZERO, 0)));
    }

    #[test]
    fn roundtrip_multiple_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..10 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("val{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        for i in 0..10 {
            let val = r.get(&Key::Int(i), true).unwrap();
            assert_eq!(
                val,
                Some((Value::from(format!("val{i}").as_str()), RevisionID::ZERO, 0))
            );
        }
    }

    #[test]
    fn roundtrip_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        // Small block size forces multiple blocks
        let mut w = SSTableWriter::new(
            &path,
            32,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..50 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.block_count().unwrap() > 1);
        for i in 0..50 {
            let val = r.get(&Key::Int(i), true).unwrap();
            assert_eq!(
                val,
                Some((Value::from(format!("v{i}").as_str()), RevisionID::ZERO, 0))
            );
        }
    }

    #[test]
    fn roundtrip_with_lz4() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("lz4.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::LZ4,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..10 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("val{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        for i in 0..10 {
            assert_eq!(
                r.get(&Key::Int(i), true).unwrap(),
                Some((Value::from(format!("val{i}").as_str()), RevisionID::ZERO, 0))
            );
        }
    }

    #[test]
    fn roundtrip_with_zstd() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("zstd.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::Zstd,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..10 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("val{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        for i in 0..10 {
            assert_eq!(
                r.get(&Key::Int(i), true).unwrap(),
                Some((Value::from(format!("val{i}").as_str()), RevisionID::ZERO, 0))
            );
        }
    }

    #[test]
    fn roundtrip_str_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("str.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        // Str keys must be added in sorted order
        w.add(
            &Key::from("aaa"),
            &Value::from("first"),
            RevisionID::ZERO,
            0,
        )
        .unwrap();
        w.add(
            &Key::from("bbb"),
            &Value::from("second"),
            RevisionID::ZERO,
            0,
        )
        .unwrap();
        w.add(
            &Key::from("ccc"),
            &Value::from("third"),
            RevisionID::ZERO,
            0,
        )
        .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(
            r.get(&Key::from("aaa"), true).unwrap(),
            Some((Value::from("first"), RevisionID::ZERO, 0))
        );
        assert_eq!(
            r.get(&Key::from("bbb"), true).unwrap(),
            Some((Value::from("second"), RevisionID::ZERO, 0))
        );
        assert_eq!(
            r.get(&Key::from("ccc"), true).unwrap(),
            Some((Value::from("third"), RevisionID::ZERO, 0))
        );
    }

    #[test]
    fn roundtrip_null_and_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("special.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::Null, RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(2), &Value::tombstone(), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(3), &Value::from("data"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(
            r.get(&Key::Int(1), true).unwrap(),
            Some((Value::Null, RevisionID::ZERO, 0))
        );
        assert_eq!(
            r.get(&Key::Int(2), true).unwrap(),
            Some((Value::tombstone(), RevisionID::ZERO, 0))
        );
        assert_eq!(
            r.get(&Key::Int(3), true).unwrap(),
            Some((Value::from("data"), RevisionID::ZERO, 0))
        );
    }

    // --- Key not found ---

    #[test]
    fn get_missing_key_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(3), &Value::from("c"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.get(&Key::Int(2), true).unwrap(), None);
    }

    #[test]
    fn get_key_beyond_last_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.get(&Key::Int(999), true).unwrap(), None);
    }

    #[test]
    fn get_key_before_first_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(10), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.get(&Key::Int(1), true).unwrap(), None);
    }

    // --- Corruption detection ---

    #[test]
    fn reader_rejects_too_small_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tiny.sst");
        fs::write(&path, b"too small").unwrap();

        let Err(err) = SSTableReader::open(&path, 1, None, &io()) else {
            panic!("expected error for too-small file");
        };
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_rejects_bad_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bad.sst");
        let mut data = vec![0u8; V2_FOOTER_SIZE];
        data[..4].copy_from_slice(b"XXXX");
        fs::write(&path, &data).unwrap();

        let Err(err) = SSTableReader::open(&path, 1, None, &io()) else {
            panic!("expected error for bad magic");
        };
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_detects_corrupt_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("corrupt.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("hello"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        // Corrupt one byte in the data block (byte 0 is compression tag)
        let mut data = fs::read(&path).unwrap();
        data[1] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let err = r.get(&Key::Int(1), true).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_skips_checksum_when_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("skip.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("hello"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        // Corrupt one byte in the data block
        let mut data = fs::read(&path).unwrap();
        data[1] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        // With verify_checksums=false, corruption is not detected
        // (read may return garbage or decompression error, but not a checksum error)
        let result = r.get(&Key::Int(1), false);
        // We don't assert the value—just that it doesn't return a checksum error
        if let Err(ref e) = result {
            let msg = format!("{e}");
            assert!(!msg.contains("checksum mismatch"));
        }
    }

    // --- Boundary: first and last keys ---

    #[test]
    fn get_first_and_last_key_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("boundary.sst");
        let mut w = SSTableWriter::new(
            &path,
            32,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..100 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(
            r.get(&Key::Int(0), true).unwrap(),
            Some((Value::from("v0"), RevisionID::ZERO, 0))
        );
        assert_eq!(
            r.get(&Key::Int(99), true).unwrap(),
            Some((Value::from("v99"), RevisionID::ZERO, 0))
        );
        assert_eq!(r.get(&Key::Int(100), true).unwrap(), None);
    }

    // --- iter_entries ---

    #[test]
    fn iter_entries_single_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(2), &Value::from("b"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(3), &Value::from("c"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            (Key::Int(1), Value::from("a"), RevisionID::ZERO, 0)
        );
        assert_eq!(
            entries[1],
            (Key::Int(2), Value::from("b"), RevisionID::ZERO, 0)
        );
        assert_eq!(
            entries[2],
            (Key::Int(3), Value::from("c"), RevisionID::ZERO, 0)
        );
    }

    #[test]
    fn iter_entries_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_multi.sst");
        let mut w = SSTableWriter::new(
            &path,
            32,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..20 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 20);
        for (i, (key, value, _rev, _exp)) in entries.iter().enumerate() {
            assert_eq!(*key, Key::Int(i as i64));
            assert_eq!(*value, Value::from(format!("v{i}").as_str()));
        }
    }

    #[test]
    fn iter_entries_with_tombstones() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_tomb.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("live"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(2), &Value::tombstone(), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::Int(3), &Value::Null, RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].1, Value::tombstone());
        assert_eq!(entries[2].1, Value::Null);
    }

    #[test]
    fn iter_entries_empty_sstable() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("empty.sst");
        let w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn iter_entries_with_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_lz4.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::LZ4,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..10 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("val{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 10);
        assert_eq!(
            entries[0],
            (Key::Int(0), Value::from("val0"), RevisionID::ZERO, 0)
        );
        assert_eq!(
            entries[9],
            (Key::Int(9), Value::from("val9"), RevisionID::ZERO, 0)
        );
    }

    #[test]
    fn size_bytes_nonzero() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("size.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("data"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.size_bytes() > V2_FOOTER_SIZE);
    }

    // --- parse_index truncation ---

    #[test]
    fn parse_index_truncated_at_key_len() {
        // Only 1 byte when 2 are needed for key_len
        let data = vec![0x00];
        assert!(SSTableReader::parse_index(&data).is_err());
    }

    #[test]
    fn parse_index_truncated_at_key() {
        // key_len = 5, but only 3 bytes follow
        let mut data = vec![];
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(&[0x01, 0x02, 0x03]);
        assert!(SSTableReader::parse_index(&data).is_err());
    }

    #[test]
    fn parse_index_truncated_at_offset_size() {
        // Valid key_len + key, but missing offset/size (needs 12 bytes, only 4 given)
        let key = Key::Int(1).to_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(&key);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // only 4 bytes, need 12
        assert!(SSTableReader::parse_index(&data).is_err());
    }

    #[test]
    fn parse_index_valid_entry() {
        let key = Key::Int(1).to_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(&key);
        data.extend_from_slice(&100u64.to_be_bytes()); // offset
        data.extend_from_slice(&200u32.to_be_bytes()); // size
        let entries = SSTableReader::parse_index(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].offset, 100);
        assert_eq!(entries[0].size, 200);
    }

    // --- parse_block_entries truncation ---

    #[test]
    fn parse_block_truncated_key_len() {
        let data = vec![0x00]; // only 1 byte, need 2
        let err = SSTableReader::parse_block_entries(&data, 2).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn parse_block_truncated_key() {
        let mut data = vec![];
        data.extend_from_slice(&5u16.to_be_bytes()); // key_len = 5
        data.extend_from_slice(&[0x01, 0x02]); // only 2 bytes of key
        let err = SSTableReader::parse_block_entries(&data, 2).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn parse_block_truncated_value_tag() {
        let key = Key::Int(1).to_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(&key);
        // No value_tag byte
        let err = SSTableReader::parse_block_entries(&data, 2).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn parse_block_truncated_value_len() {
        let key = Key::Int(1).to_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(&key);
        data.push(0x00); // value_tag
        data.extend_from_slice(&[0x00, 0x00]); // only 2 bytes, need 4 for value_len
        let err = SSTableReader::parse_block_entries(&data, 2).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn parse_block_truncated_value_data() {
        let key = Key::Int(1).to_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(&key);
        data.push(0x00); // value_tag
        data.extend_from_slice(&10u32.to_be_bytes()); // value_len = 10
        data.extend_from_slice(&[0x01, 0x02]); // only 2 bytes of data
        let err = SSTableReader::parse_block_entries(&data, 2).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    // --- iterator / bloom edge cases ---

    #[test]
    fn iter_entries_empty_sstable_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("empty_scan.sst");
        let w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        let entries = r.iter_entries(false).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn bloom_prefix_filter_for_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bloom_scan.sst");
        // bloom_bits=10, bloom_prefix_len=3
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            10,
            3,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::from("aaa:1"), &Value::from("v"), RevisionID::ZERO, 0)
            .unwrap();
        w.add(&Key::from("aaa:2"), &Value::from("v"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();

        // Prefix that exists — bloom should pass
        assert!(r.may_contain_prefix_for_scan(&Key::from("aaa:").to_prefix_bytes()));

        // Prefix that doesn't exist — bloom filter should reject
        assert!(!r.may_contain_prefix_for_scan(&Key::from("zzz:").to_prefix_bytes()));
    }

    // --- Format versioning ---

    #[test]
    fn v4_footer_size() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("v4.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let data = fs::read(&path).unwrap();
        let footer = &data[data.len() - V2_FOOTER_SIZE..];
        assert_eq!(&footer[..4], &MAGIC);
        let version = u16::from_be_bytes(footer[4..6].try_into().unwrap());
        assert_eq!(version, 4);
    }

    #[test]
    fn v3_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("v2rt.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..10 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.features(), FEATURE_RESTART_POINTS);
        for i in 0..10 {
            assert_eq!(
                r.get(&Key::Int(i), true).unwrap(),
                Some((Value::from(format!("v{i}").as_str()), RevisionID::ZERO, 0))
            );
        }
    }

    #[test]
    fn v1_file_readable() {
        // Construct a minimal V1 SSTable by hand: one data block + V1 footer (48 bytes).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("v1.sst");

        let mut file_data = Vec::new();

        // Build a data block with a single entry: Key::Int(1) => "hello"
        let key_bytes = Key::Int(1).to_bytes();
        let value_data = b"hello";
        let mut block_buf = Vec::new();
        block_buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        block_buf.extend_from_slice(&key_bytes);
        block_buf.push(0x00); // value tag = Data
        block_buf.extend_from_slice(&(value_data.len() as u32).to_be_bytes());
        block_buf.extend_from_slice(value_data);

        // On-disk block: [compression_tag=0x00][payload][checksum: 5B]
        let mut block_on_disk = Vec::new();
        block_on_disk.push(0x00); // no compression
        block_on_disk.extend_from_slice(&block_buf);
        let block_cksum = Checksum::compute(&block_on_disk);
        block_on_disk.extend_from_slice(&block_cksum.to_bytes());

        let block_offset = 0u64;
        let block_size = block_on_disk.len() as u32;
        file_data.extend_from_slice(&block_on_disk);

        // Filter block: empty (size=0)
        let filter_offset = file_data.len() as u64;
        let filter_size = 0u32;

        // Index block: one entry
        let index_offset = file_data.len() as u64;
        let mut index_data = Vec::new();
        index_data.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        index_data.extend_from_slice(&key_bytes);
        index_data.extend_from_slice(&block_offset.to_be_bytes());
        index_data.extend_from_slice(&block_size.to_be_bytes());
        let index_size = index_data.len() as u32;
        file_data.extend_from_slice(&index_data);

        // V1 footer (48 bytes)
        let mut footer = Vec::with_capacity(V1_FOOTER_SIZE);
        footer.extend_from_slice(&MAGIC);
        footer.extend_from_slice(&1u16.to_be_bytes()); // version = 1
        footer.extend_from_slice(&1u64.to_be_bytes()); // entry_count = 1
        footer.extend_from_slice(&index_offset.to_be_bytes());
        footer.extend_from_slice(&index_size.to_be_bytes());
        footer.extend_from_slice(&1u32.to_be_bytes()); // num_blocks = 1
        footer.extend_from_slice(&filter_offset.to_be_bytes());
        footer.extend_from_slice(&filter_size.to_be_bytes());
        footer.push(0x00); // filter_format = legacy
        let footer_cksum = Checksum::compute(&footer);
        footer.extend_from_slice(&footer_cksum.to_bytes());
        assert_eq!(footer.len(), V1_FOOTER_SIZE);
        file_data.extend_from_slice(&footer);

        fs::write(&path, &file_data).unwrap();

        // V2 reader should open V1 file transparently
        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert_eq!(r.entry_count(), 1);
        assert_eq!(r.features(), 0);
        assert_eq!(
            r.get(&Key::Int(1), true).unwrap(),
            Some((Value::from("hello"), RevisionID::ZERO, 0))
        );
    }

    #[test]
    fn unknown_features_rejected() {
        // Write a valid V2 SSTable, then patch the features field with unknown bits
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("feat.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let mut data = fs::read(&path).unwrap();
        let footer_start = data.len() - V2_FOOTER_SIZE;

        // Patch features at footer[43..47] to 0x8000_0000 (unknown bit)
        data[footer_start + 43..footer_start + 47].copy_from_slice(&0x8000_0000u32.to_be_bytes());

        // Recompute checksum over first 51 bytes of footer
        let cksum = Checksum::compute(&data[footer_start..footer_start + 51]);
        data[footer_start + 51..footer_start + 56].copy_from_slice(&cksum.to_bytes());
        fs::write(&path, &data).unwrap();

        let Err(err) = SSTableReader::open(&path, 1, None, &io()) else {
            panic!("expected error for unknown features");
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("not supported by this version of rKV"),
            "{msg}"
        );
    }

    #[test]
    fn unsupported_version_rejected() {
        // Write a valid V2 SSTable, then patch the version to 99
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("ver99.sst");
        let mut w = SSTableWriter::new(
            &path,
            4096,
            Compression::None,
            0,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(1), &Value::from("a"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let mut data = fs::read(&path).unwrap();
        let footer_start = data.len() - V2_FOOTER_SIZE;

        // Patch version at footer[4..6] to 99
        data[footer_start + 4..footer_start + 6].copy_from_slice(&99u16.to_be_bytes());

        // Recompute checksum over first 51 bytes of footer
        let cksum = Checksum::compute(&data[footer_start..footer_start + 51]);
        data[footer_start + 51..footer_start + 56].copy_from_slice(&cksum.to_bytes());
        fs::write(&path, &data).unwrap();

        let Err(err) = SSTableReader::open(&path, 1, None, &io()) else {
            panic!("expected error for unsupported version");
        };
        let msg = format!("{err}");
        assert!(msg.contains("unsupported version"), "{msg}");
    }

    #[test]
    fn restart_points_written_and_readable() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("restart.sst");
        // Small block size to get multiple blocks with restart points
        let mut w = SSTableWriter::new(
            &path,
            64,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..50 {
            w.add(
                &Key::Int(i),
                &Value::from(format!("v{i}").as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.has_restarts);
        assert_eq!(r.features(), FEATURE_RESTART_POINTS);
        assert_eq!(r.entry_count(), 50);
        assert!(r.block_count().unwrap() > 1);

        // Verify all keys are readable
        for i in 0..50 {
            let result = r.get(&Key::Int(i), true).unwrap();
            assert_eq!(
                result,
                Some((Value::from(format!("v{i}").as_str()), RevisionID::ZERO, 0)),
                "key {i} mismatch"
            );
        }
    }

    #[test]
    fn restart_trailer_roundtrip() {
        // Manually build a restart trailer and verify strip_restart_trailer
        let mut block = Vec::new();
        // Fake entry data (just some bytes)
        block.extend_from_slice(b"entry_data_here");
        let entry_end = block.len();

        // Write 3 restart offsets: 0, 5, 10
        block.extend_from_slice(&0u32.to_le_bytes());
        block.extend_from_slice(&5u32.to_le_bytes());
        block.extend_from_slice(&10u32.to_le_bytes());
        block.extend_from_slice(&3u32.to_le_bytes()); // num_restarts

        let (entries, restarts) = SSTableReader::strip_restart_trailer(&block).unwrap();
        assert_eq!(entries, &block[..entry_end]);
        assert_eq!(restarts.len(), 3);
        assert_eq!(restarts.get(0), 0);
        assert_eq!(restarts.get(1), 5);
        assert_eq!(restarts.get(2), 10);
    }

    #[test]
    fn restart_points_with_compression() {
        for compression in [Compression::LZ4, Compression::Zstd] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("compressed_restart.sst");
            let mut w =
                SSTableWriter::new(&path, 128, compression, 10, 0, FilterPolicy::Bloom, &io())
                    .unwrap();
            for i in 0..100 {
                w.add(
                    &Key::Int(i),
                    &Value::from(format!("val{i}").as_str()),
                    RevisionID::ZERO,
                    0,
                )
                .unwrap();
            }
            w.finish().unwrap();

            let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
            assert!(r.has_restarts);
            for i in 0..100 {
                let result = r.get(&Key::Int(i), true).unwrap();
                assert!(result.is_some(), "key {i} missing with {compression:?}");
            }
        }
    }

    #[test]
    fn restart_single_entry_block() {
        // A block with exactly 1 entry should have 1 restart point at offset 0
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("single.sst");
        // Very large block size so all entries fit in one block
        let mut w = SSTableWriter::new(
            &path,
            1_000_000,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        w.add(&Key::Int(42), &Value::from("only"), RevisionID::ZERO, 0)
            .unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.has_restarts);
        assert_eq!(r.block_count().unwrap(), 1);
        let result = r.get(&Key::Int(42), true).unwrap();
        assert_eq!(result, Some((Value::from("only"), RevisionID::ZERO, 0)));
        // Missing key returns None
        assert_eq!(r.get(&Key::Int(99), true).unwrap(), None);
    }

    #[test]
    fn restart_exactly_interval_entries() {
        // Exactly RESTART_INTERVAL entries in a single block — 1 restart point
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("exact.sst");
        let mut w = SSTableWriter::new(
            &path,
            1_000_000,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for i in 0..RESTART_INTERVAL {
            w.add(&Key::Int(i as i64), &Value::from("v"), RevisionID::ZERO, 0)
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.has_restarts);
        // All keys findable
        for i in 0..RESTART_INTERVAL {
            assert!(
                r.get(&Key::Int(i as i64), true).unwrap().is_some(),
                "key {i} missing"
            );
        }
    }

    #[test]
    fn restart_multi_revision_single_key() {
        // Multiple revisions of the same key in one block
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("multirev.sst");
        let mut w = SSTableWriter::new(
            &path,
            1_000_000,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        for rev in 1..=5u128 {
            w.add(
                &Key::Int(1),
                &Value::from(format!("r{rev}").as_str()),
                RevisionID::from(rev),
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        // get() returns latest (last) revision
        let result = r.get(&Key::Int(1), true).unwrap().unwrap();
        assert_eq!(result.0, Value::from("r5"));
        assert_eq!(result.1, RevisionID::from(5));

        // get_all_revisions() returns all 5
        let all = r.get_all_revisions(&Key::Int(1), true).unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].1, RevisionID::from(1));
        assert_eq!(all[4].1, RevisionID::from(5));
    }

    #[test]
    fn restart_str_keys() {
        // Verify restart points work with Str keys (variable length, different ordering)
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("strkeys.sst");
        let mut w = SSTableWriter::new(
            &path,
            128,
            Compression::None,
            10,
            0,
            FilterPolicy::Bloom,
            &io(),
        )
        .unwrap();
        let keys: Vec<String> = (0..60).map(|i| format!("key_{i:04}")).collect();
        for k in &keys {
            w.add(
                &Key::Str(k.clone()),
                &Value::from(k.as_str()),
                RevisionID::ZERO,
                0,
            )
            .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path, 1, None, &io()).unwrap();
        assert!(r.has_restarts);
        // Verify all keys readable
        for k in &keys {
            let result = r.get(&Key::Str(k.clone()), true).unwrap();
            assert!(result.is_some(), "key {k} missing");
            assert_eq!(result.unwrap().0, Value::from(k.as_str()));
        }
        // Non-existent key
        assert_eq!(r.get(&Key::Str("zzz_missing".into()), true).unwrap(), None);
    }
}
