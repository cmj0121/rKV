use std::fs;
use std::io::Write;
use std::path::Path;

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
}

impl SSTableWriter {
    /// Create a new SSTable writer at the given path.
    pub(crate) fn new(path: &Path, block_size: usize, compression: Compression) -> Result<Self> {
        let file = fs::File::create(path)?;
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
        })
    }

    /// Add a key-value entry. Keys MUST be added in sorted order.
    pub(crate) fn add(&mut self, key: &Key, value: &Value) -> Result<()> {
        let key_bytes = key.to_bytes();
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

    /// Finish writing: flush any remaining entries, write index and footer.
    pub(crate) fn finish(mut self) -> Result<()> {
        // Flush remaining entries
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }

        // Write index block
        let index_offset = self.offset;
        let index_data = self.encode_index();
        self.file.write_all(&index_data)?;
        let index_size = index_data.len() as u32;
        self.offset += index_data.len() as u64;

        // Write footer
        let footer = self.encode_footer(index_offset, index_size);
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
    /// [data_blocks: u32 BE][reserved: 13B][checksum: 5B CRC32C]
    /// ```
    fn encode_footer(&self, index_offset: u64, index_size: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&VERSION.to_be_bytes());
        buf.extend_from_slice(&self.entry_count.to_be_bytes());
        buf.extend_from_slice(&index_offset.to_be_bytes());
        buf.extend_from_slice(&index_size.to_be_bytes());
        buf.extend_from_slice(&(self.index.len() as u32).to_be_bytes());
        // Reserved padding (13 bytes)
        buf.extend_from_slice(&[0u8; 13]);
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
        let mut writer = SSTableWriter::new(&path, 4096, Compression::None).unwrap();
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
        let mut writer = SSTableWriter::new(&path, 4096, Compression::None).unwrap();
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
        let mut writer = SSTableWriter::new(&path, 32, Compression::None).unwrap();
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
}
