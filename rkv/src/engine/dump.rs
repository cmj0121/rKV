use std::fs;
use std::io::Write;
use std::path::Path;

use super::checksum::Checksum;
use super::error::{Error, Result};
use super::key::Key;
use super::value::Value;

/// Dump file magic bytes: "rKVD".
const MAGIC: [u8; 4] = *b"rKVD";

/// Dump format version.
const VERSION: u16 = 1;

// --- DumpWriter ---

/// Writes database records to a portable dump file.
///
/// Usage: call `write_header()` once, then `write_record()` for each entry,
/// then `finish()` to write the EOF sentinel.
pub(crate) struct DumpWriter {
    file: fs::File,
}

impl DumpWriter {
    /// Create a new dump writer at the given path.
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let file = fs::File::create(path)?;
        Ok(Self { file })
    }

    /// Write the dump file header: magic, version, and original DB path.
    pub(crate) fn write_header(&mut self, db_path: &Path) -> Result<()> {
        let path_bytes = db_path.to_string_lossy().as_bytes().to_vec();
        let path_len = path_bytes.len() as u16;

        self.file.write_all(&MAGIC)?;
        self.file.write_all(&VERSION.to_be_bytes())?;
        self.file.write_all(&path_len.to_be_bytes())?;
        self.file.write_all(&path_bytes)?;
        Ok(())
    }

    /// Write a single data record with checksum.
    pub(crate) fn write_record(
        &mut self,
        namespace: &str,
        key: &Key,
        value: &Value,
        expires_at_ms: u64,
    ) -> Result<()> {
        let payload = encode_payload(namespace, key, value, expires_at_ms);
        let payload_len = payload.len() as u32;

        self.file.write_all(&payload_len.to_be_bytes())?;
        self.file.write_all(&payload)?;
        let checksum = Checksum::compute(&payload);
        self.file.write_all(&checksum.to_bytes())?;
        Ok(())
    }

    /// Write the EOF sentinel and flush.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.file.write_all(&0u32.to_be_bytes())?;
        self.file.flush()?;
        Ok(())
    }
}

// --- DumpReader ---

/// A single record parsed from a dump file.
pub(crate) struct DumpRecord {
    pub namespace: String,
    pub key: Key,
    pub value: Value,
    #[allow(dead_code)] // part of format spec, consumed when TTL support is added
    pub expires_at_ms: u64,
}

/// Header parsed from a dump file.
#[derive(Debug)]
pub(crate) struct DumpHeader {
    pub db_path: String,
}

/// Reads database records from a dump file.
pub(crate) struct DumpReader {
    data: Vec<u8>,
    pos: usize,
}

impl DumpReader {
    /// Open a dump file for reading.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path)?;
        Ok(Self { data, pos: 0 })
    }

    /// Read and verify the dump file header.
    pub(crate) fn read_header(&mut self) -> Result<DumpHeader> {
        // Minimum header: 4 (magic) + 2 (version) + 2 (path_len) = 8 bytes
        if self.data.len() < 8 {
            return Err(Error::Corruption("dump file too small".into()));
        }

        // Verify magic
        if self.data[..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "dump file bad magic: expected {MAGIC:?}, got {:?}",
                &self.data[..4]
            )));
        }

        let version = u16::from_be_bytes(self.data[4..6].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "dump file unsupported version: {version}"
            )));
        }

        let path_len = u16::from_be_bytes(self.data[6..8].try_into().unwrap()) as usize;
        if 8 + path_len > self.data.len() {
            return Err(Error::Corruption("dump file header truncated".into()));
        }

        let db_path = String::from_utf8(self.data[8..8 + path_len].to_vec())
            .map_err(|e| Error::Corruption(format!("dump file path not UTF-8: {e}")))?;

        self.pos = 8 + path_len;
        Ok(DumpHeader { db_path })
    }

    /// Read the next record. Returns `None` at EOF sentinel.
    pub(crate) fn read_record(&mut self, verify_checksums: bool) -> Result<Option<DumpRecord>> {
        if self.pos + 4 > self.data.len() {
            return Err(Error::Corruption(
                "dump file truncated at payload_len".into(),
            ));
        }

        let payload_len =
            u32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        self.pos += 4;

        // EOF sentinel
        if payload_len == 0 {
            return Ok(None);
        }

        let cksum_size = Checksum::encoded_size();
        if self.pos + payload_len + cksum_size > self.data.len() {
            return Err(Error::Corruption(
                "dump file truncated at record payload".into(),
            ));
        }

        let payload = &self.data[self.pos..self.pos + payload_len];

        // Verify checksum
        if verify_checksums {
            let cksum_start = self.pos + payload_len;
            let checksum = Checksum::from_bytes(&self.data[cksum_start..cksum_start + cksum_size])?;
            checksum.verify(payload)?;
        }

        let record = decode_payload(payload)?;
        self.pos += payload_len + cksum_size;

        Ok(Some(record))
    }
}

// --- Encoding / Decoding ---

/// Encode a record payload.
///
/// Format:
/// ```text
/// [ns_len: 2B BE][namespace][key_len: 2B BE][key_bytes]
/// [value_tag: 1B][value_data_len: 4B BE][value_data]
/// [expires_at_ms: 8B BE]
/// ```
fn encode_payload(namespace: &str, key: &Key, value: &Value, expires_at_ms: u64) -> Vec<u8> {
    let ns_bytes = namespace.as_bytes();
    let key_bytes = key.to_bytes();
    let value_tag = value.to_tag();
    let value_data = value_to_data(value);

    let mut buf = Vec::new();
    buf.extend_from_slice(&(ns_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(ns_bytes);
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(&key_bytes);
    buf.push(value_tag);
    buf.extend_from_slice(&(value_data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&value_data);
    buf.extend_from_slice(&expires_at_ms.to_be_bytes());
    buf
}

/// Decode a record payload into a DumpRecord.
fn decode_payload(data: &[u8]) -> Result<DumpRecord> {
    let mut pos = 0;

    // Namespace
    if pos + 2 > data.len() {
        return Err(Error::Corruption("dump record truncated at ns_len".into()));
    }
    let ns_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    if pos + ns_len > data.len() {
        return Err(Error::Corruption(
            "dump record truncated at namespace".into(),
        ));
    }
    let namespace = String::from_utf8(data[pos..pos + ns_len].to_vec())
        .map_err(|e| Error::Corruption(format!("dump record namespace not UTF-8: {e}")))?;
    pos += ns_len;

    // Key
    if pos + 2 > data.len() {
        return Err(Error::Corruption("dump record truncated at key_len".into()));
    }
    let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    if pos + key_len > data.len() {
        return Err(Error::Corruption("dump record truncated at key".into()));
    }
    let key = Key::from_bytes(&data[pos..pos + key_len])?;
    pos += key_len;

    // Value tag
    if pos + 1 > data.len() {
        return Err(Error::Corruption(
            "dump record truncated at value_tag".into(),
        ));
    }
    let value_tag = data[pos];
    pos += 1;

    // Value data
    if pos + 4 > data.len() {
        return Err(Error::Corruption(
            "dump record truncated at value_data_len".into(),
        ));
    }
    let value_data_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + value_data_len > data.len() {
        return Err(Error::Corruption(
            "dump record truncated at value_data".into(),
        ));
    }
    let value = Value::from_tag(value_tag, &data[pos..pos + value_data_len])?;
    pos += value_data_len;

    // Expires at
    if pos + 8 > data.len() {
        return Err(Error::Corruption(
            "dump record truncated at expires_at_ms".into(),
        ));
    }
    let expires_at_ms = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());

    Ok(DumpRecord {
        namespace,
        key,
        value,
        expires_at_ms,
    })
}

/// Extract raw data bytes for value encoding (same pattern as sstable.rs).
fn value_to_data(value: &Value) -> Vec<u8> {
    match value {
        Value::Data(bytes) => bytes.clone(),
        Value::Null | Value::Tombstone => Vec::new(),
        Value::Pointer(vp) => vp.to_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_record_encoding() {
        let payload = encode_payload("myns", &Key::Int(42), &Value::from("hello"), 0);
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.namespace, "myns");
        assert_eq!(record.key, Key::Int(42));
        assert_eq!(record.value, Value::from("hello"));
        assert_eq!(record.expires_at_ms, 0);
    }

    #[test]
    fn roundtrip_with_ttl() {
        let payload = encode_payload("ns", &Key::from("key"), &Value::Null, 1700000000000);
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.namespace, "ns");
        assert_eq!(record.key, Key::from("key"));
        assert_eq!(record.value, Value::Null);
        assert_eq!(record.expires_at_ms, 1700000000000);
    }

    #[test]
    fn writer_reader_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dump_path = tmp.path().join("test.rkv");

        {
            let mut w = DumpWriter::new(&dump_path).unwrap();
            w.write_header(Path::new("/tmp/mydb")).unwrap();
            w.write_record("_", &Key::Int(1), &Value::from("a"), 0)
                .unwrap();
            w.write_record("ns2", &Key::from("k"), &Value::Null, 12345)
                .unwrap();
            w.finish().unwrap();
        }

        let mut r = DumpReader::open(&dump_path).unwrap();
        let header = r.read_header().unwrap();
        assert_eq!(header.db_path, "/tmp/mydb");

        let rec1 = r.read_record(true).unwrap().unwrap();
        assert_eq!(rec1.namespace, "_");
        assert_eq!(rec1.key, Key::Int(1));
        assert_eq!(rec1.value, Value::from("a"));
        assert_eq!(rec1.expires_at_ms, 0);

        let rec2 = r.read_record(true).unwrap().unwrap();
        assert_eq!(rec2.namespace, "ns2");
        assert_eq!(rec2.key, Key::from("k"));
        assert_eq!(rec2.value, Value::Null);
        assert_eq!(rec2.expires_at_ms, 12345);

        assert!(r.read_record(true).unwrap().is_none()); // EOF
    }

    #[test]
    fn reader_rejects_bad_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bad.rkv");
        fs::write(&path, b"XXXXrest").unwrap();

        let mut r = DumpReader::open(&path).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_rejects_truncated_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tiny.rkv");
        fs::write(&path, b"rKV").unwrap(); // too small

        let mut r = DumpReader::open(&path).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }
}
