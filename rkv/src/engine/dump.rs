use std::borrow::Cow;
use std::fs;
use std::io::Write;
use std::path::Path;

use super::checksum::Checksum;
use super::error::{bytes_to_array, Error, Result};
use super::io::{IoBackend, IoBytes};
use super::key::Key;
use super::revision::RevisionID;
use super::value::Value;

/// Dump file magic bytes: "rKVD".
const MAGIC: [u8; 4] = *b"rKVD";

/// Dump format version 1 (legacy).
const VERSION_1: u16 = 1;

/// Dump format version 2 (revision tracking + encryption).
const VERSION_2: u16 = 2;

/// Header flag: dump file is encrypted.
const FLAG_ENCRYPTED: u8 = 0x01;

/// Salt size for encrypted dumps.
const SALT_SIZE: usize = 16;

// ---------------------------------------------------------------------------
// DumpOptions
// ---------------------------------------------------------------------------

/// Options for `DB::dump_with_options`.
#[derive(Default)]
pub struct DumpOptions {
    /// Only dump entries with revision strictly greater than this value.
    /// `None` means full dump (all entries).
    pub after_revision: Option<RevisionID>,

    /// Encrypt the dump file with this password.
    /// `None` means no encryption.
    pub password: Option<String>,
}

// ---------------------------------------------------------------------------
// DumpWriter
// ---------------------------------------------------------------------------

/// Writes database records to a portable dump file.
///
/// Usage: call `write_header()` once, then `write_record()` for each entry,
/// then `finish()` to write the EOF sentinel.
pub(crate) struct DumpWriter {
    file: fs::File,
    encrypt_key: Option<[u8; 32]>,
}

impl DumpWriter {
    /// Create a new dump writer at the given path.
    pub(crate) fn new(path: &Path, io: &dyn IoBackend) -> Result<Self> {
        let file = io.create_file(path)?;
        Ok(Self {
            file,
            encrypt_key: None,
        })
    }

    /// Write a V1 dump file header (backward-compatible).
    pub(crate) fn write_header(&mut self, db_path: &Path) -> Result<()> {
        let path_bytes = db_path.to_string_lossy().as_bytes().to_vec();
        let path_len = path_bytes.len() as u16;

        self.file.write_all(&MAGIC)?;
        self.file.write_all(&VERSION_1.to_be_bytes())?;
        self.file.write_all(&path_len.to_be_bytes())?;
        self.file.write_all(&path_bytes)?;
        Ok(())
    }

    /// Write a V2 dump file header with flags, after_revision, and optional salt.
    pub(crate) fn write_header_v2(&mut self, db_path: &Path, options: &DumpOptions) -> Result<()> {
        let path_bytes = db_path.to_string_lossy().as_bytes().to_vec();
        let path_len = path_bytes.len() as u16;

        let mut flags: u8 = 0;
        if options.password.is_some() {
            flags |= FLAG_ENCRYPTED;
        }

        let after_rev = options.after_revision.map(|r| r.as_u128()).unwrap_or(0);

        self.file.write_all(&MAGIC)?;
        self.file.write_all(&VERSION_2.to_be_bytes())?;
        self.file.write_all(&path_len.to_be_bytes())?;
        self.file.write_all(&path_bytes)?;
        self.file.write_all(&[flags])?;
        self.file.write_all(&after_rev.to_be_bytes())?;

        // If encrypted, generate salt, derive key, write salt
        if let Some(password) = &options.password {
            let salt = super::crypto::generate_salt();
            let key = super::crypto::derive_key(password, &salt);
            self.file.write_all(&salt)?;
            self.encrypt_key = Some(key);
        }

        Ok(())
    }

    /// Write a single V1 data record (no revision) with checksum.
    pub(crate) fn write_record(
        &mut self,
        namespace: &str,
        key: &Key,
        value: &Value,
        expires_at_ms: u64,
    ) -> Result<()> {
        let payload = encode_payload_v1(namespace, key, value, expires_at_ms);
        self.write_raw_payload(&payload)
    }

    /// Write a single V2 data record (with revision) with checksum.
    pub(crate) fn write_record_v2(
        &mut self,
        namespace: &str,
        key: &Key,
        value: &Value,
        expires_at_ms: u64,
        revision: RevisionID,
    ) -> Result<()> {
        let payload = encode_payload_v2(namespace, key, value, expires_at_ms, revision);
        self.write_raw_payload(&payload)
    }

    /// Write a payload (optionally encrypted) with length prefix and checksum.
    fn write_raw_payload(&mut self, payload: &[u8]) -> Result<()> {
        let encrypted;
        let final_payload = if let Some(key) = &self.encrypt_key {
            encrypted = super::crypto::encrypt(key, payload);
            &encrypted[..]
        } else {
            payload
        };

        let payload_len = final_payload.len() as u32;
        self.file.write_all(&payload_len.to_be_bytes())?;
        self.file.write_all(final_payload)?;
        let checksum = Checksum::compute(final_payload);
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

// ---------------------------------------------------------------------------
// DumpReader
// ---------------------------------------------------------------------------

/// A single record parsed from a dump file.
pub(crate) struct DumpRecord {
    pub namespace: String,
    pub key: Key,
    pub value: Value,
    #[allow(dead_code)] // consumed when TTL support is added to load
    pub expires_at_ms: u64,
    #[allow(dead_code)] // used in tests; will be consumed by incremental load
    pub revision: RevisionID,
}

/// Header parsed from a dump file.
#[derive(Debug)]
pub(crate) struct DumpHeader {
    pub db_path: String,
    #[allow(dead_code)] // used in tests; informational field
    pub version: u16,
    pub flags: u8,
    #[allow(dead_code)] // used in tests; informational field
    pub after_revision: u128,
}

impl DumpHeader {
    pub fn is_encrypted(&self) -> bool {
        self.flags & FLAG_ENCRYPTED != 0
    }
}

/// Reads database records from a dump file.
pub(crate) struct DumpReader {
    data: IoBytes,
    pos: usize,
    version: u16,
    decrypt_key: Option<[u8; 32]>,
    salt: Option<[u8; SALT_SIZE]>,
}

impl DumpReader {
    /// Open a dump file for reading.
    pub(crate) fn open(path: &Path, io: &dyn IoBackend) -> Result<Self> {
        let data = io.read_file(path)?;
        Ok(Self {
            data,
            pos: 0,
            version: 0,
            decrypt_key: None,
            salt: None,
        })
    }

    /// Read and verify the dump file header. Supports V1 and V2.
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

        let version = u16::from_be_bytes(bytes_to_array(&self.data[4..6], "dump file version")?);
        if version != VERSION_1 && version != VERSION_2 {
            return Err(Error::Corruption(format!(
                "dump file unsupported version: {version}"
            )));
        }

        let path_len =
            u16::from_be_bytes(bytes_to_array(&self.data[6..8], "dump file path_len")?) as usize;
        if 8 + path_len > self.data.len() {
            return Err(Error::Corruption("dump file header truncated".into()));
        }

        let db_path = String::from_utf8(self.data[8..8 + path_len].to_vec())
            .map_err(|e| Error::Corruption(format!("dump file path not UTF-8: {e}")))?;

        self.pos = 8 + path_len;
        self.version = version;

        let mut flags: u8 = 0;
        let mut after_revision: u128 = 0;

        if version >= VERSION_2 {
            // Read flags (1 byte) + after_revision (16 bytes)
            if self.pos + 17 > self.data.len() {
                return Err(Error::Corruption(
                    "dump file V2 header truncated at flags".into(),
                ));
            }
            flags = self.data[self.pos];
            self.pos += 1;
            after_revision = u128::from_be_bytes(bytes_to_array(
                &self.data[self.pos..self.pos + 16],
                "dump V2 after_revision",
            )?);
            self.pos += 16;

            // If encrypted, read and cache salt
            if flags & FLAG_ENCRYPTED != 0 {
                if self.pos + SALT_SIZE > self.data.len() {
                    return Err(Error::Corruption(
                        "dump file V2 header truncated at salt".into(),
                    ));
                }
                let salt: [u8; SALT_SIZE] =
                    bytes_to_array(&self.data[self.pos..self.pos + SALT_SIZE], "dump salt")?;
                self.salt = Some(salt);
                self.pos += SALT_SIZE;
            }
        }

        Ok(DumpHeader {
            db_path,
            version,
            flags,
            after_revision,
        })
    }

    /// Set the decryption password. Must be called after `read_header()` for
    /// encrypted dumps. Derives the key from the salt cached during header parsing.
    pub(crate) fn set_password(&mut self, password: &str) -> Result<()> {
        let salt = self
            .salt
            .ok_or_else(|| Error::Corruption("no salt in dump header".into()))?;
        self.decrypt_key = Some(super::crypto::derive_key(password, &salt));
        Ok(())
    }

    /// Read the next record. Returns `None` at EOF sentinel.
    pub(crate) fn read_record(&mut self, verify_checksums: bool) -> Result<Option<DumpRecord>> {
        if self.pos + 4 > self.data.len() {
            return Err(Error::Corruption(
                "dump file truncated at payload_len".into(),
            ));
        }

        let payload_len = u32::from_be_bytes(bytes_to_array(
            &self.data[self.pos..self.pos + 4],
            "dump record payload_len",
        )?) as usize;
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

        let raw_payload = &self.data[self.pos..self.pos + payload_len];

        // Verify checksum (on the raw/encrypted payload)
        if verify_checksums {
            let cksum_start = self.pos + payload_len;
            let checksum = Checksum::from_bytes(&self.data[cksum_start..cksum_start + cksum_size])?;
            checksum.verify(raw_payload)?;
        }

        // Decrypt if needed
        let decrypted;
        let payload = if let Some(key) = &self.decrypt_key {
            decrypted = super::crypto::decrypt(key, raw_payload)?;
            &decrypted
        } else {
            raw_payload
        };

        let record = if self.version >= VERSION_2 {
            decode_payload_v2(payload)?
        } else {
            decode_payload_v1(payload)?
        };
        self.pos += payload_len + cksum_size;

        Ok(Some(record))
    }
}

// ---------------------------------------------------------------------------
// Encoding / Decoding — V1
// ---------------------------------------------------------------------------

/// Encode a V1 record payload.
///
/// Format:
/// ```text
/// [ns_len: 2B BE][namespace][key_len: 2B BE][key_bytes]
/// [value_tag: 1B][value_data_len: 4B BE][value_data]
/// [expires_at_ms: 8B BE]
/// ```
fn encode_payload_v1(namespace: &str, key: &Key, value: &Value, expires_at_ms: u64) -> Vec<u8> {
    let ns_bytes = namespace.as_bytes();
    let key_len = key.encoded_len();
    let value_tag = value.to_tag();
    let value_data = value_to_data(value);

    let capacity = 2 + ns_bytes.len() + 2 + key_len + 1 + 4 + value_data.len() + 8;
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&(ns_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(ns_bytes);
    buf.extend_from_slice(&(key_len as u16).to_be_bytes());
    key.write_bytes_to(&mut buf);
    buf.push(value_tag);
    buf.extend_from_slice(&(value_data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&value_data);
    buf.extend_from_slice(&expires_at_ms.to_be_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Encoding / Decoding — V2
// ---------------------------------------------------------------------------

/// Encode a V2 record payload (V1 + revision).
///
/// Format:
/// ```text
/// [ns_len: 2B BE][namespace][key_len: 2B BE][key_bytes]
/// [value_tag: 1B][value_data_len: 4B BE][value_data]
/// [expires_at_ms: 8B BE][revision: 16B BE]
/// ```
fn encode_payload_v2(
    namespace: &str,
    key: &Key,
    value: &Value,
    expires_at_ms: u64,
    revision: RevisionID,
) -> Vec<u8> {
    let mut buf = encode_payload_v1(namespace, key, value, expires_at_ms);
    buf.extend_from_slice(&revision.as_u128().to_be_bytes());
    buf
}

/// Maximum namespace name length in bytes (sanity limit for untrusted data).
const MAX_NAMESPACE_LEN: usize = 256;

/// Decode a V1 record payload into a DumpRecord.
fn decode_payload_v1(data: &[u8]) -> Result<DumpRecord> {
    let (namespace, key, value, expires_at_ms, _pos) = decode_payload_common(data)?;
    Ok(DumpRecord {
        namespace,
        key,
        value,
        expires_at_ms,
        revision: RevisionID::ZERO,
    })
}

/// Decode a V2 record payload into a DumpRecord (with revision).
fn decode_payload_v2(data: &[u8]) -> Result<DumpRecord> {
    let (namespace, key, value, expires_at_ms, pos) = decode_payload_common(data)?;

    // Revision (16 bytes)
    if pos + 16 > data.len() {
        return Err(Error::Corruption(
            "dump V2 record truncated at revision".into(),
        ));
    }
    let revision_raw = u128::from_be_bytes(
        data[pos..pos + 16]
            .try_into()
            .map_err(|_| Error::Corruption("dump V2 record truncated at revision bytes".into()))?,
    );

    Ok(DumpRecord {
        namespace,
        key,
        value,
        expires_at_ms,
        revision: RevisionID::from(revision_raw),
    })
}

/// Shared decoding logic for V1 and V2. Returns (namespace, key, value, expires_at_ms, pos).
fn decode_payload_common(data: &[u8]) -> Result<(String, Key, Value, u64, usize)> {
    let mut pos = 0;

    // Namespace
    if pos + 2 > data.len() {
        return Err(Error::Corruption("dump record truncated at ns_len".into()));
    }
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
    let expires_at_ms =
        u64::from_be_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            Error::Corruption("dump record truncated at expires_at_ms bytes".into())
        })?);
    pos += 8;

    Ok((namespace, key, value, expires_at_ms, pos))
}

/// Extract raw data bytes for value encoding (same pattern as sstable.rs).
fn value_to_data(value: &Value) -> Cow<'_, [u8]> {
    match value {
        Value::Data(bytes) => Cow::Borrowed(bytes),
        Value::Null | Value::Tombstone => Cow::Borrowed(&[]),
        Value::Pointer(vp) => Cow::Owned(vp.to_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io() -> super::super::io::BufferedIo {
        super::super::io::BufferedIo
    }

    // --- V1 backward compatibility ---

    #[test]
    fn roundtrip_record_encoding() {
        let payload = encode_payload_v1("myns", &Key::Int(42), &Value::from("hello"), 0);
        let record = decode_payload_v1(&payload).unwrap();
        assert_eq!(record.namespace, "myns");
        assert_eq!(record.key, Key::Int(42));
        assert_eq!(record.value, Value::from("hello"));
        assert_eq!(record.expires_at_ms, 0);
        assert_eq!(record.revision, RevisionID::ZERO);
    }

    #[test]
    fn roundtrip_with_ttl() {
        let payload = encode_payload_v1("ns", &Key::from("key"), &Value::Null, 1700000000000);
        let record = decode_payload_v1(&payload).unwrap();
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
        assert_eq!(header.version, VERSION_1);
        assert!(!header.is_encrypted());

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

    // --- V2 roundtrip ---

    #[test]
    fn v2_writer_reader_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dump_path = tmp.path().join("test_v2.rkv");
        let rev = RevisionID::from(42_u128);

        {
            let mut w = DumpWriter::new(&dump_path, &io()).unwrap();
            let opts = DumpOptions::default();
            w.write_header_v2(Path::new("/tmp/mydb"), &opts).unwrap();
            w.write_record_v2("_", &Key::Int(1), &Value::from("a"), 0, rev)
                .unwrap();
            w.finish().unwrap();
        }

        let mut r = DumpReader::open(&dump_path, &io()).unwrap();
        let header = r.read_header().unwrap();
        assert_eq!(header.version, VERSION_2);
        assert!(!header.is_encrypted());
        assert_eq!(header.after_revision, 0);

        let rec = r.read_record(true).unwrap().unwrap();
        assert_eq!(rec.key, Key::Int(1));
        assert_eq!(rec.revision, rev);
        assert!(r.read_record(true).unwrap().is_none());
    }

    #[test]
    fn v2_encrypted_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dump_path = tmp.path().join("test_enc.rkv");
        let rev = RevisionID::from(100_u128);
        let password = "secret123";

        {
            let mut w = DumpWriter::new(&dump_path, &io()).unwrap();
            let opts = DumpOptions {
                after_revision: Some(RevisionID::from(50_u128)),
                password: Some(password.to_string()),
            };
            w.write_header_v2(Path::new("/tmp/mydb"), &opts).unwrap();
            w.write_record_v2("_", &Key::Int(1), &Value::from("encrypted_val"), 0, rev)
                .unwrap();
            w.finish().unwrap();
        }

        let mut r = DumpReader::open(&dump_path, &io()).unwrap();
        let header = r.read_header().unwrap();
        assert_eq!(header.version, VERSION_2);
        assert!(header.is_encrypted());
        assert_eq!(header.after_revision, 50);

        r.set_password(password).unwrap();
        let rec = r.read_record(true).unwrap().unwrap();
        assert_eq!(rec.key, Key::Int(1));
        assert_eq!(rec.value, Value::from("encrypted_val"));
        assert_eq!(rec.revision, rev);
        assert!(r.read_record(true).unwrap().is_none());
    }

    #[test]
    fn v2_encrypted_wrong_password() {
        let tmp = tempfile::tempdir().unwrap();
        let dump_path = tmp.path().join("test_wrongpw.rkv");

        {
            let mut w = DumpWriter::new(&dump_path, &io()).unwrap();
            let opts = DumpOptions {
                after_revision: None,
                password: Some("correct".to_string()),
            };
            w.write_header_v2(Path::new("/tmp/db"), &opts).unwrap();
            w.write_record_v2("_", &Key::Int(1), &Value::from("val"), 0, RevisionID::ZERO)
                .unwrap();
            w.finish().unwrap();
        }

        let mut r = DumpReader::open(&dump_path, &io()).unwrap();
        r.read_header().unwrap();
        r.set_password("wrong").unwrap();
        // Decryption should fail on first record
        assert!(r.read_record(true).is_err());
    }

    // --- V2 payload encoding ---

    #[test]
    fn v2_payload_roundtrip() {
        let rev = RevisionID::from(999_u128);
        let payload = encode_payload_v2("ns", &Key::Int(5), &Value::from("data"), 12345, rev);
        let record = decode_payload_v2(&payload).unwrap();
        assert_eq!(record.namespace, "ns");
        assert_eq!(record.key, Key::Int(5));
        assert_eq!(record.value, Value::from("data"));
        assert_eq!(record.expires_at_ms, 12345);
        assert_eq!(record.revision, rev);
    }

    // --- Error paths (shared with V1) ---

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
        assert!(decode_payload_v1(&[0x00]).is_err());
    }

    #[test]
    fn decode_payload_truncated_namespace() {
        // ns_len = 10, but only 2 bytes of data follow
        let mut data = vec![];
        data.extend_from_slice(&10u16.to_be_bytes());
        data.extend_from_slice(&[0x41, 0x42]);
        assert!(decode_payload_v1(&data).is_err());
    }

    #[test]
    fn decode_payload_truncated_key_len() {
        // Valid namespace, but truncated at key_len
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 0);
        // namespace is 2 bytes len + 2 bytes "ns" = 4 bytes; truncate just after
        assert!(decode_payload_v1(&payload[..4]).is_err());
    }

    #[test]
    fn decode_payload_truncated_key() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 0);
        // 2 (ns_len) + 2 (ns) + 2 (key_len) = 6; key needs 9 bytes, give only 2
        assert!(decode_payload_v1(&payload[..8]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_tag() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 0);
        // 2 + 2 + 2 + 9 = 15 bytes to get past key; truncate before value_tag
        assert!(decode_payload_v1(&payload[..15]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_data_len() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 0);
        // 15 + 1 (tag) = 16; need 4 more for value_data_len, give only 2
        assert!(decode_payload_v1(&payload[..18]).is_err());
    }

    #[test]
    fn decode_payload_truncated_value_data() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("hello"), 0);
        // 15 + 1 (tag) + 4 (data_len) = 20; data = 5 bytes, give only 2
        assert!(decode_payload_v1(&payload[..22]).is_err());
    }

    #[test]
    fn decode_payload_truncated_expires() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 12345);
        // Cut off the last 8 bytes (expires_at_ms)
        assert!(decode_payload_v1(&payload[..payload.len() - 8]).is_err());
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
        let payload = encode_payload_v1("ns", &Key::Int(1), &value, 0);
        let record = decode_payload_v1(&payload).unwrap();
        assert!(record.value.is_pointer());
    }

    // --- V2 truncation ---

    #[test]
    fn v2_decode_truncated_revision() {
        let payload = encode_payload_v1("ns", &Key::Int(1), &Value::from("v"), 0);
        // V2 expects 16 more bytes for revision — V1 payload is too short
        assert!(decode_payload_v2(&payload).is_err());
    }
}
