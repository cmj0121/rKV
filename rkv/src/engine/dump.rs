use std::fs;
use std::io::Write;
use std::path::Path;

use super::checksum::Checksum;
use super::error::{Error, Result};
use super::io::{IoBackend, IoBytes};
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
    pub(crate) fn new(path: &Path, io: &dyn IoBackend) -> Result<Self> {
        let file = io.create_file(path)?;
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
    data: IoBytes,
    pos: usize,
}

impl DumpReader {
    /// Open a dump file for reading.
    pub(crate) fn open(path: &Path, io: &dyn IoBackend) -> Result<Self> {
        let data = io.read_file(path)?;
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

        // SAFETY: data.len() >= 8 checked above — slices are exactly 2 bytes
        let version = u16::from_be_bytes(self.data[4..6].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "dump file unsupported version: {version}"
            )));
        }

        // SAFETY: data.len() >= 8 checked above — slice is exactly 2 bytes
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

        // SAFETY: bounds checked above — slice is exactly 4 bytes
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

/// Maximum namespace name length in bytes (sanity limit for untrusted data).
const MAX_NAMESPACE_LEN: usize = 256;

/// Decode a record payload into a DumpRecord.
fn decode_payload(data: &[u8]) -> Result<DumpRecord> {
    let mut pos = 0;

    // Namespace
    if pos + 2 > data.len() {
        return Err(Error::Corruption("dump record truncated at ns_len".into()));
    }
    // SAFETY: bounds checked above — slice is exactly 2 bytes
    let ns_len = u16::from_be_bytes(
        data[pos..pos + 2]
            .try_into()
            .map_err(|_| Error::Corruption("dump record truncated at ns_len bytes".into()))?,
    ) as usize;
    pos += 2;

    if ns_len > MAX_NAMESPACE_LEN {
        return Err(Error::Corruption(format!(
            "dump record namespace length {ns_len} exceeds maximum {MAX_NAMESPACE_LEN}"
        )));
    }

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
    // SAFETY: bounds checked above — slice is exactly 2 bytes
    let key_len = u16::from_be_bytes(
        data[pos..pos + 2]
            .try_into()
            .map_err(|_| Error::Corruption("dump record truncated at key_len bytes".into()))?,
    ) as usize;
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
    // SAFETY: bounds checked above — slice is exactly 4 bytes
    let value_data_len =
        u32::from_be_bytes(data[pos..pos + 4].try_into().map_err(|_| {
            Error::Corruption("dump record truncated at value_data_len bytes".into())
        })?) as usize;
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
    // SAFETY: bounds checked above — slice is exactly 8 bytes
    let expires_at_ms =
        u64::from_be_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            Error::Corruption("dump record truncated at expires_at_ms bytes".into())
        })?);

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

    fn io() -> super::super::io::BufferedIo {
        super::super::io::BufferedIo
    }

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
            let mut w = DumpWriter::new(&dump_path, &io()).unwrap();
            w.write_header(Path::new("/tmp/mydb")).unwrap();
            w.write_record("_", &Key::Int(1), &Value::from("a"), 0)
                .unwrap();
            w.write_record("ns2", &Key::from("k"), &Value::Null, 12345)
                .unwrap();
            w.finish().unwrap();
        }

        let mut r = DumpReader::open(&dump_path, &io()).unwrap();
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

        let mut r = DumpReader::open(&path, &io()).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn reader_rejects_truncated_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tiny.rkv");
        fs::write(&path, b"rKV").unwrap(); // too small

        let mut r = DumpReader::open(&path, &io()).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    // --- decode_payload truncation ---

    #[test]
    fn decode_payload_truncated_ns_len() {
        assert!(decode_payload(&[0x00]).is_err());
    }

    #[test]
    fn decode_payload_truncated_namespace() {
        // ns_len = 10, but only 2 bytes of data follow
        let mut data = vec![];
        data.extend_from_slice(&10u16.to_be_bytes());
        data.extend_from_slice(&[0x41, 0x42]);
        assert!(decode_payload(&data).is_err());
    }

    #[test]
    fn decode_payload_truncated_key_len() {
        // Valid namespace, but truncated at key_len
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("v"), 0);
        // namespace is 2 bytes len + 2 bytes "ns" = 4 bytes; truncate just after
        assert!(decode_payload(&payload[..4]).is_err());
    }

    #[test]
    fn decode_payload_truncated_key() {
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("v"), 0);
        // 2 (ns_len) + 2 (ns) + 2 (key_len) = 6; key needs 9 bytes, give only 2
        assert!(decode_payload(&payload[..8]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_tag() {
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("v"), 0);
        // 2 + 2 + 2 + 9 = 15 bytes to get past key; truncate before value_tag
        assert!(decode_payload(&payload[..15]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_data_len() {
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("v"), 0);
        // 15 + 1 (tag) = 16; need 4 more for value_data_len, give only 2
        assert!(decode_payload(&payload[..18]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_data() {
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("hello"), 0);
        // 15 + 1 (tag) + 4 (data_len) = 20; data = 5 bytes, give only 2
        assert!(decode_payload(&payload[..22]).is_err());
    }

    #[test]
    fn decode_payload_truncated_expires() {
        let payload = encode_payload("ns", &Key::Int(1), &Value::from("v"), 12345);
        // Cut off the last 8 bytes (expires_at_ms)
        assert!(decode_payload(&payload[..payload.len() - 8]).is_err());
    }

    // --- DumpReader error paths ---

    #[test]
    fn read_header_bad_version() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("badver.rkv");
        let mut data = vec![];
        data.extend_from_slice(b"rKVD"); // magic
        data.extend_from_slice(&99u16.to_be_bytes()); // bad version
        data.extend_from_slice(&0u16.to_be_bytes()); // path_len = 0
        fs::write(&path, &data).unwrap();

        let mut r = DumpReader::open(&path, &io()).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn read_header_truncated_path() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("trunc_path.rkv");
        let mut data = vec![];
        data.extend_from_slice(b"rKVD");
        data.extend_from_slice(&1u16.to_be_bytes()); // version
        data.extend_from_slice(&100u16.to_be_bytes()); // path_len = 100 (but no data follows)
        fs::write(&path, &data).unwrap();

        let mut r = DumpReader::open(&path, &io()).unwrap();
        let err = r.read_header().unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn read_record_truncated_at_payload_len() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("trunc_rec.rkv");

        {
            let mut w = DumpWriter::new(&path, &io()).unwrap();
            w.write_header(Path::new("/tmp/db")).unwrap();
            w.write_record("_", &Key::Int(1), &Value::from("a"), 0)
                .unwrap();
            w.finish().unwrap();
        }

        // Read the file, then truncate it so the second record's payload_len is cut
        let data = fs::read(&path).unwrap();
        // Remove last 2 bytes from EOF sentinel area
        fs::write(&path, &data[..data.len() - 2]).unwrap();

        let mut r = DumpReader::open(&path, &io()).unwrap();
        r.read_header().unwrap();
        r.read_record(true).unwrap().unwrap(); // first record OK
        assert!(r.read_record(true).is_err()); // truncated
    }

    #[test]
    fn read_record_truncated_at_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("trunc_pay.rkv");

        {
            let mut w = DumpWriter::new(&path, &io()).unwrap();
            w.write_header(Path::new("/tmp/db")).unwrap();
            w.write_record("_", &Key::Int(1), &Value::from("a"), 0)
                .unwrap();
            w.finish().unwrap();
        }

        // Read the file, find the first record payload and truncate mid-payload
        let data = fs::read(&path).unwrap();
        // Header is magic(4) + version(2) + path_len(2) + path
        // Truncate 10 bytes before EOF to cut into the record's payload
        fs::write(&path, &data[..data.len() - 10]).unwrap();

        let mut r = DumpReader::open(&path, &io()).unwrap();
        r.read_header().unwrap();
        // The first record's payload + checksum may be partially cut
        let result = r.read_record(true);
        // Either the first record fails, or it succeeds and the sentinel read fails
        assert!(result.is_err() || r.read_record(true).is_err());
    }

    // --- Pointer value round-trip ---

    #[test]
    fn roundtrip_pointer_value() {
        use super::super::value::ValuePointer;

        let vp = ValuePointer::new([0xAA; 32], 1234);
        let value = Value::Pointer(vp);
        let payload = encode_payload("ns", &Key::Int(1), &value, 0);
        let record = decode_payload(&payload).unwrap();
        assert!(record.value.is_pointer());
    }
}
