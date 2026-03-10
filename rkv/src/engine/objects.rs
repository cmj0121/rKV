use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::checksum::Checksum;
use super::error::{bytes_to_array, Error, Result};
use super::io::IoBackend;
use super::value::ValuePointer;

/// Object file flags — bit 0 indicates LZ4 compression.
const FLAG_LZ4: u8 = 0x01;

/// Pack file magic bytes: "rKVO" (rKV Objects).
const PACK_MAGIC: [u8; 4] = *b"rKVO";

/// Pack file format version.
const PACK_VERSION: u16 = 1;

/// Pack file header size in bytes: magic (4) + version (2).
const PACK_HEADER_SIZE: usize = 6;

/// Pack record header size: hash (32) + original_size (4) + flags (1) + data_len (4).
const PACK_RECORD_HEADER_SIZE: usize = 41;

/// Maximum pack file size before rotation (256 MB).
const PACK_MAX_SIZE: u64 = 256 * 1024 * 1024;

// --- Pack index structures ---

/// Location of an object within a pack file.
#[derive(Clone, Debug)]
struct PackEntry {
    pack_seq: u64,
    offset: u64,
    data_len: u32,
    #[allow(dead_code)] // preserved in pack format for future use
    original_size: u32,
    flags: u8,
}

/// Mutable pack state behind a Mutex.
struct PackState {
    /// Hash → pack entry mapping for all packed objects.
    index: HashMap<[u8; 32], PackEntry>,
    /// Active pack file writer (lazy-created on first write).
    writer: Option<PackWriter>,
    /// Next pack sequence number.
    next_seq: u64,
}

/// Active pack file writer — appends records to a single pack file.
struct PackWriter {
    file: std::io::BufWriter<fs::File>,
    seq: u64,
    offset: u64,
    records_since_sync: usize,
    sync_interval: usize,
}

impl PackWriter {
    /// Create a new pack file and write the header.
    fn create(path: &Path, seq: u64, sync_interval: usize) -> Result<Self> {
        let file = fs::File::create(path)?;
        let mut bw = std::io::BufWriter::new(file);
        bw.write_all(&PACK_MAGIC)?;
        bw.write_all(&PACK_VERSION.to_be_bytes())?;
        Ok(Self {
            file: bw,
            seq,
            offset: PACK_HEADER_SIZE as u64,
            records_since_sync: 0,
            sync_interval,
        })
    }

    /// Append a record to the pack file, returning its index entry.
    fn append(
        &mut self,
        hash: &[u8; 32],
        original_size: u32,
        flags: u8,
        data: &[u8],
    ) -> Result<PackEntry> {
        let record_offset = self.offset;
        let data_len = data.len() as u32;
        let original_size_be = original_size.to_be_bytes();
        let data_len_be = data_len.to_be_bytes();

        // Compute checksum over header+data without allocating a concatenation Vec.
        let checksum =
            Checksum::compute_slices(&[hash, &original_size_be, &[flags], &data_len_be, data]);

        // Write header fields + data + checksum directly to BufWriter.
        self.file.write_all(hash)?;
        self.file.write_all(&original_size_be)?;
        self.file.write_all(&[flags])?;
        self.file.write_all(&data_len_be)?;
        self.file.write_all(data)?;
        self.file.write_all(&checksum.to_bytes())?;

        self.records_since_sync += 1;
        if self.sync_interval == 0 || self.records_since_sync >= self.sync_interval {
            self.file.flush()?;
            self.file.get_ref().sync_all()?;
            self.records_since_sync = 0;
        }

        let record_len = PACK_RECORD_HEADER_SIZE + data.len() + Checksum::encoded_size();
        self.offset += record_len as u64;

        Ok(PackEntry {
            pack_seq: self.seq,
            offset: record_offset,
            data_len,
            original_size,
            flags,
        })
    }
}

/// Scan a pack file and return all valid entries for index rebuild.
///
/// Stops at EOF or the first corrupted/truncated record (crash recovery).
fn scan_pack_file(path: &Path, seq: u64) -> Result<Vec<([u8; 32], PackEntry)>> {
    let data = fs::read(path)?;
    if data.len() < PACK_HEADER_SIZE {
        return Err(Error::Corruption(format!(
            "pack file too small: {}",
            path.display()
        )));
    }

    // Verify header
    if data[..4] != PACK_MAGIC {
        return Err(Error::Corruption(format!(
            "pack file bad magic: {}",
            path.display()
        )));
    }
    let version = u16::from_be_bytes(bytes_to_array(&data[4..6], "pack file version")?);
    if version != PACK_VERSION {
        return Err(Error::Corruption(format!(
            "pack file unsupported version {version}: {}",
            path.display()
        )));
    }

    let mut entries = Vec::new();
    let mut pos = PACK_HEADER_SIZE;

    while pos < data.len() {
        // Need at least header + checksum
        let min_record = PACK_RECORD_HEADER_SIZE + Checksum::encoded_size();
        if pos + min_record > data.len() {
            break; // truncated tail — stop
        }

        // Parse record header
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[pos..pos + 32]);
        let original_size = u32::from_be_bytes(bytes_to_array(
            &data[pos + 32..pos + 36],
            "pack record original_size",
        )?);
        let flags = data[pos + 36];
        let data_len = u32::from_be_bytes(bytes_to_array(
            &data[pos + 37..pos + 41],
            "pack record data_len",
        )?);

        let record_end = pos + PACK_RECORD_HEADER_SIZE + data_len as usize;
        let checksum_end = record_end + Checksum::encoded_size();

        if checksum_end > data.len() {
            break; // truncated record — stop
        }

        // Verify checksum
        let payload = &data[pos..record_end];
        let checksum = Checksum::from_bytes(&data[record_end..checksum_end])?;
        if checksum.verify(payload).is_err() {
            break; // corrupted record — stop
        }

        entries.push((
            hash,
            PackEntry {
                pack_seq: seq,
                offset: pos as u64,
                data_len,
                original_size,
                flags,
            },
        ));

        pos = checksum_end;
    }

    Ok(entries)
}

/// Read a single packed object from a pack file by offset.
fn read_pack_record(path: &Path, entry: &PackEntry) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(entry.offset))?;

    let total = PACK_RECORD_HEADER_SIZE + entry.data_len as usize + Checksum::encoded_size();
    let mut buf = vec![0u8; total];
    file.read_exact(&mut buf)?;

    // Verify checksum
    let payload_end = PACK_RECORD_HEADER_SIZE + entry.data_len as usize;
    let checksum = Checksum::from_bytes(&buf[payload_end..])?;
    checksum.verify(&buf[..payload_end])?;

    // Extract data payload (after the 41-byte header)
    Ok(buf[PACK_RECORD_HEADER_SIZE..payload_end].to_vec())
}

// --- ObjectStore ---

/// Content-addressable object store for bin objects.
///
/// Objects can be stored as:
/// 1. **Pack files** — multiple objects batched into append-only pack files
///    for reduced I/O syscalls (default for new writes).
/// 2. **Loose files** — one file per object in fan-out directories
///    (legacy format, still readable).
pub(crate) struct ObjectStore {
    base: PathBuf,
    io: Arc<dyn IoBackend>,
    packs: Mutex<PackState>,
    sync_interval: usize,
}

impl ObjectStore {
    /// Open (or create) the object store directory for a namespace under `db_dir`.
    ///
    /// Scans existing pack files to rebuild the in-memory index.
    pub(crate) fn open(
        db_dir: &Path,
        ns: &str,
        io: Arc<dyn IoBackend>,
        sync_interval: usize,
    ) -> Result<Self> {
        let base = db_dir.join("objects").join(ns);
        fs::create_dir_all(&base)?;

        // Scan for existing pack files and build index
        let mut index = HashMap::new();
        let mut max_seq = 0u64;

        let mut pack_files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&base)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(seq) = parse_pack_filename(&name) {
                pack_files.push((seq, entry.path()));
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }

        // Sort by sequence so later packs overwrite earlier entries (if any dups)
        pack_files.sort_by_key(|(seq, _)| *seq);

        for (seq, path) in &pack_files {
            match scan_pack_file(path, *seq) {
                Ok(entries) => {
                    for (hash, pack_entry) in entries {
                        index.insert(hash, pack_entry);
                    }
                }
                Err(_) => {
                    // Skip corrupted pack files — repair will handle them
                }
            }
        }

        let next_seq = if pack_files.is_empty() {
            1
        } else {
            max_seq + 1
        };

        Ok(Self {
            base,
            io,
            packs: Mutex::new(PackState {
                index,
                writer: None,
                next_seq,
            }),
            sync_interval,
        })
    }

    /// Write a value to the object store, returning a `ValuePointer`.
    ///
    /// New objects are appended to a pack file. If an object with the same
    /// hash already exists (dedup), the write is skipped.
    pub(crate) fn write(&self, data: &[u8], compress: bool) -> Result<ValuePointer> {
        if data.len() > u32::MAX as usize {
            return Err(Error::Corruption(
                "object too large for pack format (>4 GB)".into(),
            ));
        }

        let hash: [u8; 32] = blake3::hash(data).into();
        let size = data.len() as u32;
        let vp = ValuePointer::new(hash, size);

        let mut state = self.packs.lock().unwrap_or_else(|e| e.into_inner());

        // Dedup: skip if already in pack index
        if state.index.contains_key(&hash) {
            return Ok(vp);
        }

        // Also check loose files for dedup (backward compat)
        let loose_path = self.loose_object_path(&vp);
        if loose_path.exists() {
            return Ok(vp);
        }

        // Compress if requested — avoid cloning when uncompressed
        let compressed;
        let (flags, payload): (u8, &[u8]) = if compress {
            compressed = lz4_flex::compress_prepend_size(data);
            if compressed.len() > u32::MAX as usize {
                return Err(Error::Corruption(
                    "compressed payload too large for pack format (>4 GB)".into(),
                ));
            }
            (FLAG_LZ4, &compressed)
        } else {
            (0u8, data)
        };

        // Rotate pack file if current one exceeds size threshold
        if let Some(ref mut w) = state.writer {
            if w.offset >= PACK_MAX_SIZE {
                // Sync any buffered records before dropping the old writer
                if w.records_since_sync > 0 {
                    w.file.get_ref().sync_all()?;
                }
                state.writer = None;
            }
        }

        // Ensure writer is open
        if state.writer.is_none() {
            let seq = state.next_seq;
            state.next_seq += 1;
            let path = self.pack_path(seq);
            state.writer = Some(PackWriter::create(&path, seq, self.sync_interval)?);
        }

        let writer = state.writer.as_mut().ok_or_else(|| {
            Error::Corruption("pack writer unexpectedly None after initialization".into())
        })?;
        let entry = writer.append(&hash, size, flags, payload)?;
        // Flush BufWriter before exposing the entry in the index, so
        // concurrent readers that open the file directly can see the data.
        writer.file.flush()?;
        state.index.insert(hash, entry);

        Ok(vp)
    }

    /// Read a value from the object store, decompressing if needed.
    ///
    /// Checks the pack index first, then falls back to loose files.
    /// When `verify` is true, the BLAKE3 hash is recomputed and checked.
    pub(crate) fn read(&self, vp: &ValuePointer, verify: bool) -> Result<Vec<u8>> {
        // Check pack index (hold lock briefly, copy entry)
        let pack_entry = {
            let state = self.packs.lock().unwrap_or_else(|e| e.into_inner());
            state.index.get(&vp.hash).cloned()
        };

        if let Some(entry) = pack_entry {
            return self.read_from_pack(&entry, vp, verify);
        }

        // Fall back to loose file
        self.read_from_loose(vp, verify)
    }

    /// Read from a pack file at the given entry offset.
    fn read_from_pack(
        &self,
        entry: &PackEntry,
        vp: &ValuePointer,
        verify: bool,
    ) -> Result<Vec<u8>> {
        let pack_path = self.pack_path(entry.pack_seq);
        let raw_data = read_pack_record(&pack_path, entry).map_err(|e| {
            Error::Corruption(format!(
                "failed to read packed object {}: {e}",
                vp.hex_hash()
            ))
        })?;

        // Decompress
        let data = if entry.flags & FLAG_LZ4 != 0 {
            lz4_flex::decompress_size_prepended(&raw_data)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression failed: {e}")))?
        } else {
            raw_data
        };

        if verify {
            let actual_hash: [u8; 32] = blake3::hash(&data).into();
            if actual_hash != vp.hash {
                return Err(Error::Corruption(format!(
                    "object hash mismatch: expected {}, got {}",
                    vp.hex_hash(),
                    hex_encode(&actual_hash)
                )));
            }
        }

        Ok(data)
    }

    /// Read from a loose object file (legacy format).
    fn read_from_loose(&self, vp: &ValuePointer, verify: bool) -> Result<Vec<u8>> {
        let path = self.loose_object_path(vp);

        let content = self.io.read_file(&path).map_err(|e| {
            if matches!(e, Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound) {
                Error::Corruption(format!("object file missing: {}", vp.hex_hash()))
            } else {
                e
            }
        })?;

        if content.is_empty() {
            return Err(Error::Corruption(format!(
                "object file empty: {}",
                path.display()
            )));
        }

        let flags = content[0];
        let payload = &content[1..];

        let data = if flags & FLAG_LZ4 != 0 {
            lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression failed: {e}")))?
        } else {
            payload.to_vec()
        };

        if verify {
            let actual_hash: [u8; 32] = blake3::hash(&data).into();
            if actual_hash != vp.hash {
                return Err(Error::Corruption(format!(
                    "object hash mismatch: expected {}, got {}",
                    vp.hex_hash(),
                    hex_encode(&actual_hash)
                )));
            }
        }

        Ok(data)
    }

    /// Check if an object exists (in packs or loose files).
    #[cfg(test)]
    pub(crate) fn exists(&self, vp: &ValuePointer) -> bool {
        let state = self.packs.lock().unwrap_or_else(|e| e.into_inner());
        if state.index.contains_key(&vp.hash) {
            return true;
        }
        drop(state);
        self.loose_object_path(vp).exists()
    }

    /// List all object hex hashes present on disk (packs + loose files).
    pub(crate) fn list_object_hashes(&self) -> Result<HashSet<String>> {
        let mut hashes = HashSet::new();

        // Hashes from pack index
        {
            let state = self.packs.lock().unwrap_or_else(|e| e.into_inner());
            for hash in state.index.keys() {
                hashes.insert(hex_encode(hash));
            }
        }

        // Hashes from loose files
        if self.base.exists() {
            for fan_entry in fs::read_dir(&self.base)? {
                let fan_entry = fan_entry?;
                if !fan_entry.file_type()?.is_dir() {
                    continue;
                }
                for obj_entry in fs::read_dir(fan_entry.path())? {
                    let obj_entry = obj_entry?;
                    let name = obj_entry.file_name().to_string_lossy().to_string();
                    if name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                        hashes.insert(name);
                    }
                }
            }
        }

        Ok(hashes)
    }

    /// Delete an object by its hex hash string.
    ///
    /// For loose files, the file is removed immediately. For packed objects,
    /// the hash is marked dead for removal during the next GC repack.
    pub(crate) fn delete_object(&self, hex_hash: &str) -> Result<()> {
        // Try to remove from pack index
        if let Some(hash_bytes) = hex_decode_hash(hex_hash) {
            let mut state = self.packs.lock().unwrap_or_else(|e| e.into_inner());
            if state.index.remove(&hash_bytes).is_some() {
                return Ok(());
            }
        }

        // Fall back to deleting loose file
        let fan_out = &hex_hash[..2];
        let path = self.base.join(fan_out).join(hex_hash);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Repack all pack files, removing objects not in `live_hashes`.
    ///
    /// Scans pack files on disk (not the in-memory index) to find all
    /// records. Live records are written to a new pack file. Old pack
    /// files are deleted. Returns the number of dead records removed.
    pub(crate) fn repack_gc(&self, live_hashes: &HashSet<String>) -> Result<u64> {
        let mut state = self.packs.lock().unwrap_or_else(|e| e.into_inner());

        // Close the active writer so all data is on disk
        if let Some(ref mut w) = state.writer {
            if w.records_since_sync > 0 {
                w.file.get_ref().sync_all()?;
            }
        }
        state.writer = None;

        // Scan pack files on disk to find all records (not the in-memory
        // index, which may have had entries removed by delete_object)
        let old_packs: Vec<PathBuf> = self.list_pack_files();
        if old_packs.is_empty() {
            return Ok(0);
        }

        let mut keep: Vec<([u8; 32], PackEntry)> = Vec::new();
        let mut dead_count = 0u64;

        for pack_path in &old_packs {
            let name = pack_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let seq = match parse_pack_filename(&name) {
                Some(s) => s,
                None => continue,
            };
            match scan_pack_file(pack_path, seq) {
                Ok(entries) => {
                    for (hash, entry) in entries {
                        let hex = hex_encode(&hash);
                        if live_hashes.contains(&hex) {
                            keep.push((hash, entry));
                        } else {
                            dead_count += 1;
                        }
                    }
                }
                Err(_) => {
                    // Corrupted pack — will be removed
                }
            }
        }

        if dead_count == 0 {
            return Ok(0);
        }

        if keep.is_empty() {
            // No live objects — just delete all packs
            for path in &old_packs {
                let _ = fs::remove_file(path);
            }
            state.index.clear();
            state.next_seq = 1;
            return Ok(dead_count);
        }

        // Write a new pack file with only live entries
        let new_seq = state.next_seq;
        state.next_seq += 1;
        let new_path = self.pack_path(new_seq);
        let mut writer = PackWriter::create(&new_path, new_seq, self.sync_interval)?;

        let mut new_index = HashMap::with_capacity(keep.len());

        for (hash, old_entry) in &keep {
            let old_path = self.pack_path(old_entry.pack_seq);
            let raw_data = read_pack_record(&old_path, old_entry)?;
            let new_entry =
                writer.append(hash, old_entry.original_size, old_entry.flags, &raw_data)?;
            new_index.insert(*hash, new_entry);
        }

        // Update index BEFORE deleting old packs (crash safety: if we crash
        // after index update but before deletion, the old packs remain on disk
        // as harmless leftovers that get re-scanned on next open; if we deleted
        // first and crashed, live objects could be lost).
        state.index = new_index;
        // Leave writer as None — next write will create a new pack

        // Now safe to delete old packs — index already points to new pack
        for path in &old_packs {
            let _ = fs::remove_file(path);
        }

        Ok(dead_count)
    }

    /// Flush the pack writer's buffer and fsync to disk.
    pub(crate) fn flush(&self) -> Result<()> {
        let mut state = self.packs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut writer) = state.writer {
            writer.file.flush()?;
            writer.file.get_ref().sync_all()?;
            writer.records_since_sync = 0;
        }
        Ok(())
    }

    /// Number of objects in the pack index.
    #[cfg(test)]
    pub(crate) fn pack_count(&self) -> usize {
        self.packs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .index
            .len()
    }

    // --- Path helpers ---

    /// Compute the file path for a loose object: `<base>/<fan_out>/<hex_hash>`.
    fn loose_object_path(&self, vp: &ValuePointer) -> PathBuf {
        self.base.join(vp.fan_out_prefix()).join(vp.hex_hash())
    }

    /// Compute the file path for a pack file: `<base>/pack-NNNNNN.pack`.
    fn pack_path(&self, seq: u64) -> PathBuf {
        self.base.join(format!("pack-{seq:06}.pack"))
    }

    /// List all pack file paths on disk.
    fn list_pack_files(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.base) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if parse_pack_filename(&name).is_some() {
                    paths.push(entry.path());
                }
            }
        }
        paths
    }
}

/// Parse a pack filename like "pack-000001.pack" → Some(1).
fn parse_pack_filename(name: &str) -> Option<u64> {
    let name = name.strip_prefix("pack-")?.strip_suffix(".pack")?;
    name.parse().ok()
}

/// Encode a 32-byte hash as a 64-character hex string.
fn hex_encode(hash: &[u8; 32]) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a 64-character hex string to a 32-byte hash.
fn hex_decode_hash(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut hash = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        hash[i] = (hi << 4) | lo;
    }
    Some(hash)
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

    fn test_io() -> Arc<dyn IoBackend> {
        Arc::new(super::super::io::BufferedIo)
    }

    #[test]
    fn write_read_roundtrip_raw() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"hello world, this is a test value";
        let vp = store.write(data, false).unwrap();
        assert_eq!(vp.size, data.len() as u32);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn write_read_roundtrip_compressed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"hello world, this is a test value for compression";
        let vp = store.write(data, true).unwrap();
        assert_eq!(vp.size, data.len() as u32);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn dedup_same_content() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"same content";
        let vp1 = store.write(data, true).unwrap();
        let vp2 = store.write(data, true).unwrap();

        assert_eq!(vp1, vp2);
        assert_eq!(store.pack_count(), 1); // only one entry
    }

    #[test]
    fn exists_after_write() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"some data";
        let vp = store.write(data, false).unwrap();

        assert!(store.exists(&vp));
    }

    #[test]
    fn exists_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp = ValuePointer::new([0xFFu8; 32], 100);
        assert!(!store.exists(&vp));
    }

    #[test]
    fn read_missing_object_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp = ValuePointer::new([0xFFu8; 32], 100);
        let err = store.read(&vp, false).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn blake3_verification_catches_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"test data for verification";
        let vp = store.write(data, false).unwrap();

        // Corrupt the pack file data
        let pack_path = store.pack_path(1);
        let mut content = fs::read(&pack_path).unwrap();
        // Corrupt a byte in the data portion (after header + record header)
        let data_offset = PACK_HEADER_SIZE + PACK_RECORD_HEADER_SIZE;
        content[data_offset] ^= 0xFF;
        // Also fix the checksum to match the corrupted data so the record
        // checksum passes but BLAKE3 fails
        let payload_end = PACK_HEADER_SIZE + PACK_RECORD_HEADER_SIZE + data.len();
        let payload = &content[PACK_HEADER_SIZE..payload_end];
        let new_cksum = Checksum::compute(payload);
        content[payload_end..payload_end + 5].copy_from_slice(&new_cksum.to_bytes());
        fs::write(&pack_path, &content).unwrap();

        // Re-open to rebuild index
        let store2 = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        let err = store2.read(&vp, true).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn list_object_hashes_includes_packed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp1 = store.write(b"data1", false).unwrap();
        let vp2 = store.write(b"data2", false).unwrap();

        let hashes = store.list_object_hashes().unwrap();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&vp1.hex_hash()));
        assert!(hashes.contains(&vp2.hex_hash()));
    }

    #[test]
    fn list_object_hashes_nonexistent_base() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "nonexistent_ns", test_io(), 0).unwrap();
        let hashes = store.list_object_hashes().unwrap();
        assert!(hashes.is_empty());
    }

    #[test]
    fn list_object_hashes_skips_non_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        // Create a regular file in the base dir (not a dir, not a pack)
        let non_dir = store.base.join("not_a_dir");
        fs::write(&non_dir, b"data").unwrap();

        let vp = store.write(b"test data for list", false).unwrap();

        let hashes = store.list_object_hashes().unwrap();
        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&vp.hex_hash()));
    }

    #[test]
    fn large_value_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        // 64 KB of data
        let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let vp = store.write(&data, true).unwrap();
        assert_eq!(vp.size, 65536);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn pack_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();

        let vp;
        {
            let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
            vp = store.write(b"persistent data", true).unwrap();
            assert_eq!(store.pack_count(), 1);
        }

        // Re-open and verify
        let store2 = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        assert_eq!(store2.pack_count(), 1);
        let data = store2.read(&vp, true).unwrap();
        assert_eq!(data, b"persistent data");
    }

    #[test]
    fn multiple_objects_in_one_pack() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let mut vps = Vec::new();
        for i in 0..100u32 {
            let data = format!("object_{i}");
            let vp = store.write(data.as_bytes(), i % 2 == 0).unwrap();
            vps.push((vp, data));
        }

        assert_eq!(store.pack_count(), 100);

        // Verify all readable
        for (vp, expected) in &vps {
            let data = store.read(vp, true).unwrap();
            assert_eq!(data, expected.as_bytes());
        }

        // Only one pack file on disk
        let packs = store.list_pack_files();
        assert_eq!(packs.len(), 1);
    }

    #[test]
    fn delete_packed_object() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp = store.write(b"deleteme", false).unwrap();
        assert!(store.exists(&vp));

        store.delete_object(&vp.hex_hash()).unwrap();
        // Should no longer appear in hash list
        let hashes = store.list_object_hashes().unwrap();
        assert!(!hashes.contains(&vp.hex_hash()));
    }

    #[test]
    fn repack_gc_removes_dead() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp_keep = store.write(b"keep this", false).unwrap();
        let vp_dead = store.write(b"remove this", false).unwrap();
        assert_eq!(store.pack_count(), 2);

        // Only vp_keep is live
        let mut live = HashSet::new();
        live.insert(vp_keep.hex_hash());

        let removed = store.repack_gc(&live).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(store.pack_count(), 1);

        // vp_keep still readable
        let data = store.read(&vp_keep, true).unwrap();
        assert_eq!(data, b"keep this");

        // vp_dead is gone
        assert!(!store.exists(&vp_dead));
    }

    #[test]
    fn backward_compat_reads_loose_files() {
        let tmp = tempfile::tempdir().unwrap();

        // Manually create a loose object file (old format)
        let data = b"legacy loose object data";
        let hash: [u8; 32] = blake3::hash(data).into();
        let vp = ValuePointer::new(hash, data.len() as u32);

        let base = tmp.path().join("objects").join("_");
        let fan_out_dir = base.join(vp.fan_out_prefix());
        fs::create_dir_all(&fan_out_dir).unwrap();

        // Loose format: [flags: 1][payload]
        let mut content = vec![0u8]; // no compression
        content.extend_from_slice(data);
        fs::write(fan_out_dir.join(vp.hex_hash()), &content).unwrap();

        // Open store — should find the loose object
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        assert_eq!(store.pack_count(), 0); // not in pack index

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data.as_slice());

        // Should appear in list
        let hashes = store.list_object_hashes().unwrap();
        assert!(hashes.contains(&vp.hex_hash()));
    }

    #[test]
    fn dedup_across_loose_and_pack() {
        let tmp = tempfile::tempdir().unwrap();

        // Create a loose object first
        let data = b"dedup test data across formats";
        let hash: [u8; 32] = blake3::hash(data).into();
        let vp = ValuePointer::new(hash, data.len() as u32);

        let base = tmp.path().join("objects").join("_");
        let fan_out_dir = base.join(vp.fan_out_prefix());
        fs::create_dir_all(&fan_out_dir).unwrap();
        let mut content = vec![0u8];
        content.extend_from_slice(data);
        fs::write(fan_out_dir.join(vp.hex_hash()), &content).unwrap();

        // Open store and try to write same data — should dedup
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        let vp2 = store.write(data, false).unwrap();
        assert_eq!(vp, vp2);
        assert_eq!(store.pack_count(), 0); // not added to pack (dedup via loose)
    }

    #[test]
    fn scan_pack_truncated_tail_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp1 = store.write(b"first object", false).unwrap();
        let _vp2 = store.write(b"second object", false).unwrap();
        assert_eq!(store.pack_count(), 2);

        // Truncate the pack file to corrupt the second record
        let pack_path = store.pack_path(1);
        let content = fs::read(&pack_path).unwrap();
        // Keep header + first record, truncate partway through second
        let first_record_size = PACK_RECORD_HEADER_SIZE + 12 + Checksum::encoded_size();
        let truncate_at = PACK_HEADER_SIZE + first_record_size + 10;
        fs::write(&pack_path, &content[..truncate_at]).unwrap();

        // Re-open — should recover first object, skip truncated second
        let store2 = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        assert_eq!(store2.pack_count(), 1);
        let data = store2.read(&vp1, true).unwrap();
        assert_eq!(data, b"first object");
    }

    #[test]
    fn parse_pack_filename_valid() {
        assert_eq!(parse_pack_filename("pack-000001.pack"), Some(1));
        assert_eq!(parse_pack_filename("pack-000042.pack"), Some(42));
        assert_eq!(parse_pack_filename("pack-999999.pack"), Some(999999));
    }

    #[test]
    fn parse_pack_filename_invalid() {
        assert_eq!(parse_pack_filename("pack-abc.pack"), None);
        assert_eq!(parse_pack_filename("not_a_pack"), None);
        assert_eq!(parse_pack_filename("pack-000001.dat"), None);
    }

    #[test]
    fn hex_roundtrip() {
        let hash = [0xAB_u8; 32];
        let hex = hex_encode(&hash);
        let decoded = hex_decode_hash(&hex).unwrap();
        assert_eq!(hash, decoded);
    }

    #[test]
    fn empty_data_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let data = b"";
        let vp = store.write(data, false).unwrap();
        assert_eq!(vp.size, 0);

        let result = store.read(&vp, true).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn pack_crc32c_corruption_detected_on_read() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        let vp = store.write(b"checksum test", false).unwrap();

        // Corrupt the CRC32C checksum in the pack file
        let pack_path = store.pack_path(1);
        let mut content = fs::read(&pack_path).unwrap();
        // Flip a bit in the checksum (last 5 bytes of the first record)
        let cksum_offset = content.len() - 1;
        content[cksum_offset] ^= 0xFF;
        fs::write(&pack_path, &content).unwrap();

        // Re-open — scan should skip the corrupted record
        let store2 = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();
        assert_eq!(store2.pack_count(), 0);

        // Direct read should fail with corruption error
        let err = store2.read(&vp, false).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn pack_rotation_at_size_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        // Write enough data to trigger rotation.
        // Each object: ~1 KB data + 46 bytes overhead ≈ 1070 bytes.
        // PACK_MAX_SIZE = 256 MB. Use a smaller trick: override the writer offset.
        let vp1 = store.write(b"first pack object", false).unwrap();

        // Manually set offset past threshold to trigger rotation on next write
        {
            let mut state = store.packs.lock().unwrap();
            if let Some(ref mut w) = state.writer {
                w.offset = PACK_MAX_SIZE;
            }
        }

        let vp2 = store.write(b"second pack object", false).unwrap();

        // Should now have 2 pack files
        let packs = store.list_pack_files();
        assert_eq!(packs.len(), 2);

        // Both objects still readable
        assert_eq!(store.read(&vp1, true).unwrap(), b"first pack object");
        assert_eq!(store.read(&vp2, true).unwrap(), b"second pack object");
    }

    #[test]
    fn repack_gc_all_dead() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_", test_io(), 0).unwrap();

        store.write(b"obj1", false).unwrap();
        store.write(b"obj2", false).unwrap();
        assert_eq!(store.pack_count(), 2);

        // No live hashes — everything is dead
        let live = HashSet::new();
        let removed = store.repack_gc(&live).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(store.pack_count(), 0);

        // Pack files should be deleted from disk
        assert!(store.list_pack_files().is_empty());
    }
}
