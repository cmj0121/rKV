use std::fs;
use std::path::{Path, PathBuf};

use super::error::{Error, Result};
use super::value::ValuePointer;

/// Object file flags — bit 0 indicates LZ4 compression.
const FLAG_LZ4: u8 = 0x01;

/// Content-addressable object store for bin objects.
///
/// Large values are stored as standalone files identified by their BLAKE3
/// content hash. The directory layout uses a fan-out prefix (first byte of
/// the hash) to avoid excessive entries per directory.
pub(crate) struct ObjectStore {
    base: PathBuf,
}

impl ObjectStore {
    /// Open (or create) the object store directory for a namespace under `db_dir`.
    pub(crate) fn open(db_dir: &Path, ns: &str) -> Result<Self> {
        let base = db_dir.join("objects").join(ns);
        fs::create_dir_all(&base)?;
        Ok(Self { base })
    }

    /// Write a value to the object store, returning a `ValuePointer`.
    ///
    /// If an object with the same hash already exists (dedup), the write is
    /// skipped and the existing pointer is returned.
    pub(crate) fn write(&self, data: &[u8], compress: bool) -> Result<ValuePointer> {
        let hash: [u8; 32] = blake3::hash(data).into();
        let size = data.len() as u32;
        let vp = ValuePointer::new(hash, size);

        let path = self.object_path(&vp);

        // Dedup: skip if file already exists
        if path.exists() {
            return Ok(vp);
        }

        // Ensure fan-out directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Build object file: [flags: 1][payload]
        let (flags, payload) = if compress {
            let compressed = lz4_flex::compress_prepend_size(data);
            (FLAG_LZ4, compressed)
        } else {
            (0u8, data.to_vec())
        };

        let mut content = Vec::with_capacity(1 + payload.len());
        content.push(flags);
        content.extend_from_slice(&payload);

        // Atomic write: write to tmp file, then rename
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, &content)?;
        fs::rename(&tmp_path, &path)?;

        Ok(vp)
    }

    /// Read a value from the object store, decompressing if needed.
    ///
    /// When `verify` is true, the BLAKE3 hash is recomputed and checked.
    pub(crate) fn read(&self, vp: &ValuePointer, verify: bool) -> Result<Vec<u8>> {
        let path = self.object_path(vp);

        let content = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::Corruption(format!("object file missing: {}", path.display()))
            } else {
                Error::Io(e)
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
                    actual_hash
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                )));
            }
        }

        Ok(data)
    }

    /// Check if an object file exists.
    #[allow(dead_code)]
    pub(crate) fn exists(&self, vp: &ValuePointer) -> bool {
        self.object_path(vp).exists()
    }

    /// List all object hex hashes present on disk.
    ///
    /// Walks the fan-out directories and returns the set of 64-char hex
    /// hash strings for every object file found.
    pub(crate) fn list_object_hashes(&self) -> Result<std::collections::HashSet<String>> {
        let mut hashes = std::collections::HashSet::new();
        if !self.base.exists() {
            return Ok(hashes);
        }
        for fan_entry in fs::read_dir(&self.base)? {
            let fan_entry = fan_entry?;
            if !fan_entry.file_type()?.is_dir() {
                continue;
            }
            for obj_entry in fs::read_dir(fan_entry.path())? {
                let obj_entry = obj_entry?;
                let name = obj_entry.file_name().to_string_lossy().to_string();
                // Object files are 64-char hex hashes (no extension)
                if name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                    hashes.insert(name);
                }
            }
        }
        Ok(hashes)
    }

    /// Delete an object file by its hex hash string.
    pub(crate) fn delete_object(&self, hex_hash: &str) -> Result<()> {
        let fan_out = &hex_hash[..2];
        let path = self.base.join(fan_out).join(hex_hash);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Compute the file path for an object: `<base>/<fan_out>/<hex_hash>`.
    fn object_path(&self, vp: &ValuePointer) -> PathBuf {
        self.base.join(vp.fan_out_prefix()).join(vp.hex_hash())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_roundtrip_raw() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"hello world, this is a test value";
        let vp = store.write(data, false).unwrap();
        assert_eq!(vp.size, data.len() as u32);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn write_read_roundtrip_compressed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"hello world, this is a test value for compression";
        let vp = store.write(data, true).unwrap();
        assert_eq!(vp.size, data.len() as u32);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn dedup_same_content() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"same content";
        let vp1 = store.write(data, true).unwrap();
        let vp2 = store.write(data, true).unwrap();

        assert_eq!(vp1, vp2);
    }

    #[test]
    fn exists_after_write() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"some data";
        let vp = store.write(data, false).unwrap();

        assert!(store.exists(&vp));
    }

    #[test]
    fn exists_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let vp = ValuePointer::new([0xFFu8; 32], 100);
        assert!(!store.exists(&vp));
    }

    #[test]
    fn read_missing_file_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let vp = ValuePointer::new([0xFFu8; 32], 100);
        let err = store.read(&vp, false).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn read_empty_file_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        // Create an empty file at the object path
        let vp = ValuePointer::new([0xAAu8; 32], 100);
        let path = store.object_path(&vp);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &[]).unwrap();

        let err = store.read(&vp, false).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn blake3_verification_catches_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"test data for verification";
        let vp = store.write(data, false).unwrap();

        // Corrupt the file content (but keep the flags byte valid)
        let path = store.object_path(&vp);
        let mut content = fs::read(&path).unwrap();
        content[1] ^= 0xFF;
        fs::write(&path, &content).unwrap();

        let err = store.read(&vp, true).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn fan_out_directory_created() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        let data = b"test data";
        let vp = store.write(data, false).unwrap();

        let fan_out_dir = tmp
            .path()
            .join("objects")
            .join("_")
            .join(vp.fan_out_prefix());
        assert!(fan_out_dir.is_dir());
    }

    #[test]
    fn large_value_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(tmp.path(), "_").unwrap();

        // 64 KB of data
        let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let vp = store.write(&data, true).unwrap();
        assert_eq!(vp.size, 65536);

        let result = store.read(&vp, true).unwrap();
        assert_eq!(result, data);
    }
}
