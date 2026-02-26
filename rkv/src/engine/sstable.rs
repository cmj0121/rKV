use std::fs;
use std::io::Write;
use std::path::Path;

use super::bloom::BloomFilter;
use super::checksum::Checksum;
use super::error::{Error, Result};
use super::key::Key;
use super::value::Value;
use super::Compression;

// --- Constants ---

/// SSTable file magic bytes: "rKVS".
const MAGIC: [u8; 4] = *b"rKVS";

/// SSTable format version.
const VERSION: u16 = 1;

/// Fixed footer size in bytes.
pub(crate) const FOOTER_SIZE: usize = 48;

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
    /// Bloom filter builder (collects key hashes during writes).
    bloom: BloomFilter,
    /// Prefix bloom filter builder (collects prefix hashes during writes).
    prefix_bloom: Option<BloomFilter>,
    /// Prefix length for prefix bloom (0 = disabled).
    bloom_prefix_len: usize,
}

impl SSTableWriter {
    /// Create a new SSTable writer at the given path.
    ///
    /// `bloom_bits` controls the bloom filter: 10 = ~1% FPR, 0 = disabled.
    /// `bloom_prefix_len` controls the prefix bloom (0 = disabled).
    pub(crate) fn new(
        path: &Path,
        block_size: usize,
        compression: Compression,
        bloom_bits: usize,
        bloom_prefix_len: usize,
    ) -> Result<Self> {
        let file = fs::File::create(path)?;
        let prefix_bloom = if bloom_prefix_len > 0 && bloom_bits > 0 {
            Some(BloomFilter::new(bloom_bits))
        } else {
            None
        };
        Ok(Self {
            file,
            block_size,
            compression,
            block_buf: Vec::new(),
            block_last_key: None,
            block_entry_count: 0,
            offset: 0,
            index: Vec::new(),
            entry_count: 0,
            bloom: BloomFilter::new(bloom_bits),
            prefix_bloom,
            bloom_prefix_len,
        })
    }

    /// Add a key-value entry. Keys MUST be added in sorted order.
    pub(crate) fn add(&mut self, key: &Key, value: &Value) -> Result<()> {
        let key_bytes = key.to_bytes();
        self.bloom.insert(&key_bytes);

        // Insert prefix into prefix bloom if enabled
        if let Some(ref mut pf) = self.prefix_bloom {
            let prefix_len = self.bloom_prefix_len.min(key_bytes.len());
            pf.insert(&key_bytes[..prefix_len]);
        }

        let value_data = value_to_data(value);
        let value_tag = value.to_tag();

        // Encode entry: [key_len: u16 BE][key_bytes][value_tag: u8][value_len: u32 BE][value_data]
        let key_len = key_bytes.len() as u16;
        let value_len = value_data.len() as u32;
        self.block_buf.extend_from_slice(&key_len.to_be_bytes());
        self.block_buf.extend_from_slice(&key_bytes);
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
    pub(crate) fn finish(mut self) -> Result<()> {
        // Flush remaining entries
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }

        // Build filter block
        let key_bloom_data = self.bloom.build();
        let prefix_bloom_data = self
            .prefix_bloom
            .as_mut()
            .map(|pf| pf.build())
            .unwrap_or_default();

        // Write filter block:
        //   Legacy (0x00): filter block = key bloom only
        //   Compound (0x01): [key_bloom_len: u32 LE][key_bloom_data][prefix_bloom_data]
        let filter_offset = self.offset;
        let has_prefix = !prefix_bloom_data.is_empty();
        let filter_data = if has_prefix {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(key_bloom_data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&key_bloom_data);
            buf.extend_from_slice(&prefix_bloom_data);
            buf
        } else {
            key_bloom_data
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
        );
        self.file.write_all(&footer)?;

        self.file.flush()?;
        Ok(())
    }

    /// Flush the current block to disk: compress, checksum, write.
    fn flush_block(&mut self) -> Result<()> {
        let block_offset = self.offset;

        let (tag, payload) = compress_block(&self.block_buf, &self.compression);

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
        let last_key = self.block_last_key.take().unwrap();
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

    /// Encode the fixed-size footer (48 bytes).
    ///
    /// Layout:
    /// ```text
    /// [magic: 4B][version: u16 BE][entry_count: u64 BE]
    /// [index_offset: u64 BE][index_size: u32 BE]
    /// [data_blocks: u32 BE][filter_offset: u64 BE][filter_size: u32 BE]
    /// [reserved: 1B][checksum: 5B CRC32C]
    /// ```
    fn encode_footer(
        &self,
        index_offset: u64,
        index_size: u32,
        filter_offset: u64,
        filter_size: u32,
        filter_format: u8,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&VERSION.to_be_bytes());
        buf.extend_from_slice(&self.entry_count.to_be_bytes());
        buf.extend_from_slice(&index_offset.to_be_bytes());
        buf.extend_from_slice(&index_size.to_be_bytes());
        buf.extend_from_slice(&(self.index.len() as u32).to_be_bytes());
        buf.extend_from_slice(&filter_offset.to_be_bytes());
        buf.extend_from_slice(&filter_size.to_be_bytes());
        // Filter format byte (0x00 = legacy key-only, 0x01 = compound)
        buf.push(filter_format);
        // Checksum over the preceding 43 bytes
        let checksum = Checksum::compute(&buf);
        buf.extend_from_slice(&checksum.to_bytes());
        debug_assert_eq!(buf.len(), FOOTER_SIZE);
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
fn compress_block(data: &[u8], compression: &Compression) -> (u8, Vec<u8>) {
    match compression {
        Compression::None => (COMPRESS_NONE, data.to_vec()),
        Compression::LZ4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            (COMPRESS_LZ4, compressed)
        }
        Compression::Zstd => {
            let compressed = zstd::encode_all(data, 3).expect("zstd compression failed");
            (COMPRESS_ZSTD, compressed)
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

/// Raw entry parsed from a data block: (key_bytes, value_tag, value_data).
type RawEntry = (Vec<u8>, u8, Vec<u8>);

/// Index entry parsed from an SSTable's index block.
struct IndexEntry {
    /// Last key in this data block (serialized bytes).
    last_key: Vec<u8>,
    /// Byte offset of the data block in the file.
    offset: u64,
    /// Size of the data block on disk (including compression tag + checksum).
    size: u32,
}

/// Reads key-value entries from an SSTable file.
///
/// Opens a file, parses the footer and index, then serves point lookups
/// via binary search on the block index.
pub(crate) struct SSTableReader {
    /// Raw file contents (read into memory).
    data: Vec<u8>,
    /// Parsed index entries, sorted by last_key.
    index: Vec<IndexEntry>,
    /// Total entry count from the footer.
    entry_count: u64,
    /// Bloom filter for probabilistic key membership testing.
    bloom: BloomFilter,
    /// Prefix bloom filter for scan optimization.
    prefix_bloom: Option<BloomFilter>,
    /// First key in the SSTable (serialized bytes), for range filtering.
    first_key: Option<Vec<u8>>,
}

impl SSTableReader {
    /// Open an SSTable file and parse its footer and index.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path)?;
        if data.len() < FOOTER_SIZE {
            return Err(Error::Corruption(format!(
                "SSTable too small: {} bytes (minimum {FOOTER_SIZE})",
                data.len()
            )));
        }

        // Parse footer (last 48 bytes)
        let footer_start = data.len() - FOOTER_SIZE;
        let footer = &data[footer_start..];

        // Verify magic
        if footer[..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "SSTable bad magic: expected {MAGIC:?}, got {:?}",
                &footer[..4]
            )));
        }

        // Verify footer checksum (covers first 43 bytes)
        let cksum_offset = FOOTER_SIZE - Checksum::encoded_size();
        let footer_checksum = Checksum::from_bytes(&footer[cksum_offset..])?;
        footer_checksum.verify(&footer[..cksum_offset])?;

        // Parse footer fields
        let version = u16::from_be_bytes(footer[4..6].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "SSTable unsupported version: {version}"
            )));
        }

        let entry_count = u64::from_be_bytes(footer[6..14].try_into().unwrap());
        let index_offset = u64::from_be_bytes(footer[14..22].try_into().unwrap()) as usize;
        let index_size = u32::from_be_bytes(footer[22..26].try_into().unwrap()) as usize;

        // Bounds-check the index block
        if index_offset + index_size > footer_start {
            return Err(Error::Corruption(format!(
                "SSTable index out of bounds: offset={index_offset}, size={index_size}, data_end={footer_start}"
            )));
        }

        let index_data = &data[index_offset..index_offset + index_size];
        let index = Self::parse_index(index_data)?;

        // Parse filter metadata from footer (bytes 30-42)
        let filter_offset = u64::from_be_bytes(footer[30..38].try_into().unwrap()) as usize;
        let filter_size = u32::from_be_bytes(footer[38..42].try_into().unwrap()) as usize;
        let filter_format = footer[42];

        let (bloom, prefix_bloom) =
            if filter_size > 0 && filter_offset + filter_size <= footer_start {
                let filter_data = &data[filter_offset..filter_offset + filter_size];

                match filter_format {
                    0x01 => {
                        // Compound: [key_bloom_len: u32 LE][key_bloom_data][prefix_bloom_data]
                        if filter_data.len() < 4 {
                            (BloomFilter::new(0), None)
                        } else {
                            let key_bloom_len =
                                u32::from_le_bytes(filter_data[0..4].try_into().unwrap()) as usize;
                            let key_bloom_end = 4 + key_bloom_len;
                            if key_bloom_end > filter_data.len() {
                                (BloomFilter::new(0), None)
                            } else {
                                let key_bloom =
                                    BloomFilter::from_bytes(&filter_data[4..key_bloom_end])?;
                                let prefix_bloom = if key_bloom_end < filter_data.len() {
                                    Some(BloomFilter::from_bytes(&filter_data[key_bloom_end..])?)
                                } else {
                                    None
                                };
                                (key_bloom, prefix_bloom)
                            }
                        }
                    }
                    _ => {
                        // Legacy (0x00): filter block = key bloom only
                        (BloomFilter::from_bytes(filter_data)?, None)
                    }
                }
            } else {
                (BloomFilter::new(0), None) // no filter
            };

        // Extract first key from the first block for range filtering
        let first_key = if let Some(first_ie) = index.first() {
            let block_start = first_ie.offset as usize;
            let block_end = block_start + first_ie.size as usize;
            if block_end <= data.len() {
                let block_on_disk = &data[block_start..block_end];
                let cksum_start = block_on_disk.len() - Checksum::encoded_size();
                let compression_tag = block_on_disk[0];
                let compressed_payload = &block_on_disk[1..cksum_start];
                if let Ok(block_data) = decompress_block(compression_tag, compressed_payload) {
                    // Parse just the first entry's key
                    if block_data.len() >= 2 {
                        let kl = u16::from_be_bytes(block_data[0..2].try_into().unwrap()) as usize;
                        if 2 + kl <= block_data.len() {
                            Some(block_data[2..2 + kl].to_vec())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            data,
            index,
            entry_count,
            bloom,
            prefix_bloom,
            first_key,
        })
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
            let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
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
            let offset = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let size = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            entries.push(IndexEntry {
                last_key,
                offset,
                size,
            });
        }
        Ok(entries)
    }

    /// Look up a key in the SSTable.
    ///
    /// Returns `Some(value)` if found, `None` if the key is not present.
    /// When `verify_checksums` is true, each data block is verified before
    /// decoding.
    pub(crate) fn get(&self, key: &Key, verify_checksums: bool) -> Result<Option<Value>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let key_bytes = key.to_bytes();

        // Bloom filter check: skip this SSTable if the key is definitely absent
        if !self.bloom.may_contain(&key_bytes) {
            return Ok(None);
        }

        // Binary search: find the first block whose last_key >= target
        let block_idx = match self
            .index
            .binary_search_by(|e| e.last_key.as_slice().cmp(&key_bytes))
        {
            Ok(i) => i,
            Err(i) => {
                if i >= self.index.len() {
                    return Ok(None); // key beyond all blocks
                }
                i
            }
        };

        let ie = &self.index[block_idx];
        let entries = self.read_block(ie, verify_checksums)?;

        // Linear scan entries within the block
        for (kb, value_tag, value_data) in entries {
            if kb == key_bytes {
                return Ok(Some(Value::from_tag(value_tag, &value_data)?));
            }
        }
        Ok(None)
    }

    /// Parse all entries from a decompressed block.
    fn parse_block_entries(block: &[u8]) -> Result<Vec<RawEntry>> {
        let mut entries = Vec::new();
        let mut pos = 0;
        while pos < block.len() {
            // [key_len: u16 BE][key_bytes][value_tag: u8][value_len: u32 BE][value_data]
            if pos + 2 > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at key_len".into(),
                ));
            }
            let kl = u16::from_be_bytes(block[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if pos + kl > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at key_bytes".into(),
                ));
            }
            let key_bytes = block[pos..pos + kl].to_vec();
            pos += kl;

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
            let vl = u32::from_be_bytes(block[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + vl > block.len() {
                return Err(Error::Corruption(
                    "SSTable entry truncated at value_data".into(),
                ));
            }
            let value_data = block[pos..pos + vl].to_vec();
            pos += vl;

            entries.push((key_bytes, value_tag, value_data));
        }
        Ok(entries)
    }

    /// Scan entries matching a prefix/range.
    ///
    /// Uses the block index to skip blocks that cannot contain matching keys.
    /// Returns `(Key, Value)` pairs in sorted order, including tombstones.
    ///
    /// - `prefix_bytes`: serialized key prefix to match against.
    /// - `ordered_mode`: if true, scan from prefix forward (range scan);
    ///   if false, check all blocks for string prefix matching.
    pub(crate) fn scan_entries(
        &self,
        prefix_bytes: &[u8],
        ordered_mode: bool,
        verify_checksums: bool,
    ) -> Result<Vec<(Key, Value)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }

        // Prefix bloom check: skip this SSTable if it definitely doesn't
        // contain keys with this prefix.
        if !self.may_contain_prefix(prefix_bytes) {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        if ordered_mode {
            // Range scan: find the first block whose last_key >= prefix_bytes,
            // then read blocks forward until keys no longer match.
            let start_block = match self
                .index
                .binary_search_by(|e| e.last_key.as_slice().cmp(prefix_bytes))
            {
                Ok(i) => i,
                Err(i) => {
                    if i >= self.index.len() {
                        return Ok(Vec::new());
                    }
                    i
                }
            };

            for ie in &self.index[start_block..] {
                let entries = self.read_block(ie, verify_checksums)?;
                for (key_bytes, value_tag, value_data) in entries {
                    if key_bytes.as_slice() >= prefix_bytes {
                        let key = Key::from_bytes(&key_bytes)?;
                        let value = Value::from_tag(value_tag, &value_data)?;
                        result.push((key, value));
                    }
                }
            }
        } else {
            // Prefix matching: scan all blocks, filter by prefix.
            for ie in &self.index {
                let entries = self.read_block(ie, verify_checksums)?;
                for (key_bytes, value_tag, value_data) in entries {
                    if key_bytes.starts_with(prefix_bytes) {
                        let key = Key::from_bytes(&key_bytes)?;
                        let value = Value::from_tag(value_tag, &value_data)?;
                        result.push((key, value));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Reverse-scan entries matching a prefix/range.
    ///
    /// For ordered mode: returns entries with keys <= prefix_bytes.
    /// For unordered mode: same as scan_entries (prefix matching).
    pub(crate) fn rscan_entries(
        &self,
        prefix_bytes: &[u8],
        ordered_mode: bool,
        verify_checksums: bool,
    ) -> Result<Vec<(Key, Value)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }

        // Prefix bloom check for unordered mode (prefix matching).
        // For ordered mode, prefix_bytes is a range bound, not a prefix.
        if !ordered_mode && !self.may_contain_prefix(prefix_bytes) {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        if ordered_mode {
            // Range scan: find blocks that may contain keys <= prefix_bytes.
            // We need all blocks from the beginning up to the block whose
            // last_key >= prefix_bytes.
            let end_block = match self
                .index
                .binary_search_by(|e| e.last_key.as_slice().cmp(prefix_bytes))
            {
                Ok(i) => i,
                Err(i) => {
                    if i == 0 {
                        // All blocks have last_key < prefix, so check if
                        // any keys exist <= prefix. Process all blocks.
                        // Actually, if i == 0, the first block's last_key < prefix,
                        // meaning all keys in block 0 could be <= prefix.
                        // We need to read up to block i (exclusive would miss keys).
                        // Let's just include block 0 if it has any keys <= prefix.
                    }
                    if i > 0 {
                        i - 1
                    } else {
                        0
                    }
                }
            };

            // Read from block 0 up to end_block inclusive
            for ie in &self.index[..=end_block] {
                let entries = self.read_block(ie, verify_checksums)?;
                for (key_bytes, value_tag, value_data) in entries {
                    if key_bytes.as_slice() <= prefix_bytes {
                        let key = Key::from_bytes(&key_bytes)?;
                        let value = Value::from_tag(value_tag, &value_data)?;
                        result.push((key, value));
                    }
                }
            }
        } else {
            // Prefix matching: same as forward scan
            return self.scan_entries(prefix_bytes, ordered_mode, verify_checksums);
        }

        Ok(result)
    }

    /// Test whether the prefix bloom filter may contain the given prefix.
    ///
    /// Returns `true` if the prefix might be present (or no prefix bloom exists),
    /// `false` if the prefix is definitely absent.
    pub(crate) fn may_contain_prefix(&self, prefix_bytes: &[u8]) -> bool {
        match &self.prefix_bloom {
            Some(pf) => pf.may_contain(prefix_bytes),
            None => true, // no prefix bloom — conservative
        }
    }

    /// Return the first key in this SSTable (serialized bytes), if any.
    #[allow(dead_code)]
    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        self.first_key.as_deref()
    }

    /// Read and decompress a single block, returning raw entries.
    fn read_block(&self, ie: &IndexEntry, verify_checksums: bool) -> Result<Vec<RawEntry>> {
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
        let block_data = decompress_block(compression_tag, compressed_payload)?;

        Self::parse_block_entries(&block_data)
    }

    /// Iterate all entries in sorted key order.
    ///
    /// Reads every data block, decompresses, and returns `(Key, Value)` pairs.
    pub(crate) fn iter_entries(&self, verify_checksums: bool) -> Result<Vec<(Key, Value)>> {
        let mut result = Vec::with_capacity(self.entry_count as usize);

        for ie in &self.index {
            for (key_bytes, value_tag, value_data) in self.read_block(ie, verify_checksums)? {
                let key = Key::from_bytes(&key_bytes)?;
                let value = Value::from_tag(value_tag, &value_data)?;
                result.push((key, value));
            }
        }

        Ok(result)
    }

    /// Return the total size of the SSTable data in bytes.
    pub(crate) fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Return the total number of entries in this SSTable.
    #[allow(dead_code)]
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Return the number of data blocks in this SSTable.
    #[allow(dead_code)]
    pub(crate) fn block_count(&self) -> usize {
        self.index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_none() {
        let data = b"hello world";
        let (tag, compressed) = compress_block(data, &Compression::None);
        assert_eq!(tag, COMPRESS_NONE);
        let decompressed = decompress_block(tag, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_decompress_lz4() {
        let data = b"hello world, this is a test for lz4 compression";
        let (tag, compressed) = compress_block(data, &Compression::LZ4);
        assert_eq!(tag, COMPRESS_LZ4);
        let decompressed = decompress_block(tag, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_decompress_zstd() {
        let data = b"hello world, this is a test for zstd compression";
        let (tag, compressed) = compress_block(data, &Compression::Zstd);
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
        let mut writer = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        writer.add(&Key::Int(1), &Value::from("a")).unwrap();
        writer.finish().unwrap();

        let data = fs::read(&path).unwrap();
        assert!(data.len() >= FOOTER_SIZE);

        // Last 48 bytes are the footer
        let footer = &data[data.len() - FOOTER_SIZE..];
        assert_eq!(&footer[..4], &MAGIC);
    }

    #[test]
    fn writer_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut writer = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        writer.add(&Key::Int(1), &Value::from("hello")).unwrap();
        writer.add(&Key::Int(2), &Value::from("world")).unwrap();
        writer.finish().unwrap();

        assert!(path.exists());
        let data = fs::read(&path).unwrap();
        assert!(data.len() > FOOTER_SIZE);
    }

    #[test]
    fn writer_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("multi.sst");
        // Very small block size to force multiple blocks
        let mut writer = SSTableWriter::new(&path, 32, Compression::None, 0, 0).unwrap();
        for i in 0..20 {
            writer
                .add(&Key::Int(i), &Value::from(format!("val{i}").as_str()))
                .unwrap();
        }
        writer.finish().unwrap();

        let data = fs::read(&path).unwrap();
        // Footer should have magic
        let footer = &data[data.len() - FOOTER_SIZE..];
        assert_eq!(&footer[..4], &MAGIC);
    }

    // --- Reader: open & metadata ---

    #[test]
    fn reader_open_single_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("a")).unwrap();
        w.add(&Key::Int(2), &Value::from("b")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.entry_count(), 2);
        assert_eq!(r.block_count(), 1);
    }

    #[test]
    fn reader_open_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("multi.sst");
        let mut w = SSTableWriter::new(&path, 32, Compression::None, 0, 0).unwrap();
        for i in 0..20 {
            w.add(&Key::Int(i), &Value::from(format!("v{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.entry_count(), 20);
        assert!(r.block_count() > 1);
    }

    // --- Roundtrip: write then read back ---

    #[test]
    fn roundtrip_single_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(42), &Value::from("hello")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let val = r.get(&Key::Int(42), true).unwrap();
        assert_eq!(val, Some(Value::from("hello")));
    }

    #[test]
    fn roundtrip_multiple_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        for i in 0..10 {
            w.add(&Key::Int(i), &Value::from(format!("val{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        for i in 0..10 {
            let val = r.get(&Key::Int(i), true).unwrap();
            assert_eq!(val, Some(Value::from(format!("val{i}").as_str())));
        }
    }

    #[test]
    fn roundtrip_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rt.sst");
        // Small block size forces multiple blocks
        let mut w = SSTableWriter::new(&path, 32, Compression::None, 0, 0).unwrap();
        for i in 0..50 {
            w.add(&Key::Int(i), &Value::from(format!("v{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert!(r.block_count() > 1);
        for i in 0..50 {
            let val = r.get(&Key::Int(i), true).unwrap();
            assert_eq!(val, Some(Value::from(format!("v{i}").as_str())));
        }
    }

    #[test]
    fn roundtrip_with_lz4() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("lz4.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::LZ4, 0, 0).unwrap();
        for i in 0..10 {
            w.add(&Key::Int(i), &Value::from(format!("val{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        for i in 0..10 {
            assert_eq!(
                r.get(&Key::Int(i), true).unwrap(),
                Some(Value::from(format!("val{i}").as_str()))
            );
        }
    }

    #[test]
    fn roundtrip_with_zstd() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("zstd.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::Zstd, 0, 0).unwrap();
        for i in 0..10 {
            w.add(&Key::Int(i), &Value::from(format!("val{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        for i in 0..10 {
            assert_eq!(
                r.get(&Key::Int(i), true).unwrap(),
                Some(Value::from(format!("val{i}").as_str()))
            );
        }
    }

    #[test]
    fn roundtrip_str_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("str.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        // Str keys must be added in sorted order
        w.add(&Key::from("aaa"), &Value::from("first")).unwrap();
        w.add(&Key::from("bbb"), &Value::from("second")).unwrap();
        w.add(&Key::from("ccc"), &Value::from("third")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(
            r.get(&Key::from("aaa"), true).unwrap(),
            Some(Value::from("first"))
        );
        assert_eq!(
            r.get(&Key::from("bbb"), true).unwrap(),
            Some(Value::from("second"))
        );
        assert_eq!(
            r.get(&Key::from("ccc"), true).unwrap(),
            Some(Value::from("third"))
        );
    }

    #[test]
    fn roundtrip_null_and_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("special.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::Null).unwrap();
        w.add(&Key::Int(2), &Value::tombstone()).unwrap();
        w.add(&Key::Int(3), &Value::from("data")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(&Key::Int(1), true).unwrap(), Some(Value::Null));
        assert_eq!(r.get(&Key::Int(2), true).unwrap(), Some(Value::tombstone()));
        assert_eq!(
            r.get(&Key::Int(3), true).unwrap(),
            Some(Value::from("data"))
        );
    }

    // --- Key not found ---

    #[test]
    fn get_missing_key_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("a")).unwrap();
        w.add(&Key::Int(3), &Value::from("c")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(&Key::Int(2), true).unwrap(), None);
    }

    #[test]
    fn get_key_beyond_last_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("a")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(&Key::Int(999), true).unwrap(), None);
    }

    #[test]
    fn get_key_before_first_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(10), &Value::from("a")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(&Key::Int(1), true).unwrap(), None);
    }

    // --- Corruption detection ---

    #[test]
    fn reader_rejects_too_small_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tiny.sst");
        fs::write(&path, b"too small").unwrap();

        let Err(err) = SSTableReader::open(&path) else {
            panic!("expected error for too-small file");
        };
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_rejects_bad_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bad.sst");
        let mut data = vec![0u8; FOOTER_SIZE];
        data[..4].copy_from_slice(b"XXXX");
        fs::write(&path, &data).unwrap();

        let Err(err) = SSTableReader::open(&path) else {
            panic!("expected error for bad magic");
        };
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_detects_corrupt_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("corrupt.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("hello")).unwrap();
        w.finish().unwrap();

        // Corrupt one byte in the data block (byte 0 is compression tag)
        let mut data = fs::read(&path).unwrap();
        data[1] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let err = r.get(&Key::Int(1), true).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_skips_checksum_when_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("skip.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("hello")).unwrap();
        w.finish().unwrap();

        // Corrupt one byte in the data block
        let mut data = fs::read(&path).unwrap();
        data[1] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let r = SSTableReader::open(&path).unwrap();
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
        let mut w = SSTableWriter::new(&path, 32, Compression::None, 0, 0).unwrap();
        for i in 0..100 {
            w.add(&Key::Int(i), &Value::from(format!("v{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(&Key::Int(0), true).unwrap(), Some(Value::from("v0")));
        assert_eq!(
            r.get(&Key::Int(99), true).unwrap(),
            Some(Value::from("v99"))
        );
        assert_eq!(r.get(&Key::Int(100), true).unwrap(), None);
    }

    // --- iter_entries ---

    #[test]
    fn iter_entries_single_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("a")).unwrap();
        w.add(&Key::Int(2), &Value::from("b")).unwrap();
        w.add(&Key::Int(3), &Value::from("c")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (Key::Int(1), Value::from("a")));
        assert_eq!(entries[1], (Key::Int(2), Value::from("b")));
        assert_eq!(entries[2], (Key::Int(3), Value::from("c")));
    }

    #[test]
    fn iter_entries_multi_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_multi.sst");
        let mut w = SSTableWriter::new(&path, 32, Compression::None, 0, 0).unwrap();
        for i in 0..20 {
            w.add(&Key::Int(i), &Value::from(format!("v{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 20);
        for (i, (key, value)) in entries.iter().enumerate() {
            assert_eq!(*key, Key::Int(i as i64));
            assert_eq!(*value, Value::from(format!("v{i}").as_str()));
        }
    }

    #[test]
    fn iter_entries_with_tombstones() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_tomb.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("live")).unwrap();
        w.add(&Key::Int(2), &Value::tombstone()).unwrap();
        w.add(&Key::Int(3), &Value::Null).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].1, Value::tombstone());
        assert_eq!(entries[2].1, Value::Null);
    }

    #[test]
    fn iter_entries_empty_sstable() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("empty.sst");
        let w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn iter_entries_with_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iter_lz4.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::LZ4, 0, 0).unwrap();
        for i in 0..10 {
            w.add(&Key::Int(i), &Value::from(format!("val{i}").as_str()))
                .unwrap();
        }
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        let entries = r.iter_entries(true).unwrap();
        assert_eq!(entries.len(), 10);
        assert_eq!(entries[0], (Key::Int(0), Value::from("val0")));
        assert_eq!(entries[9], (Key::Int(9), Value::from("val9")));
    }

    #[test]
    fn size_bytes_nonzero() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("size.sst");
        let mut w = SSTableWriter::new(&path, 4096, Compression::None, 0, 0).unwrap();
        w.add(&Key::Int(1), &Value::from("data")).unwrap();
        w.finish().unwrap();

        let r = SSTableReader::open(&path).unwrap();
        assert!(r.size_bytes() > FOOTER_SIZE);
    }
}
