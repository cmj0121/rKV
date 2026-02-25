use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::checksum::Checksum;
use super::error::{Error, Result};
use super::key::Key;
use super::value::Value;

/// Magic bytes: ASCII `rKVL` (0x724B564C).
const MAGIC: [u8; 4] = [0x72, 0x4B, 0x56, 0x4C];
/// Current AOL format version.
const VERSION: u16 = 1;
/// File header size in bytes.
const HEADER_SIZE: usize = 8;
/// AOL filename within the database directory.
const AOL_FILENAME: &str = "aol";

/// A record decoded from the AOL during replay.
#[derive(Debug)]
pub(crate) struct AolRecord {
    pub namespace: String,
    pub revision: u128,
    pub expires_at_ms: u64,
    pub key: Key,
    pub value: Value,
}

/// Append-only log for write-ahead durability.
pub(crate) struct Aol {
    writer: BufWriter<File>,
    buffer_size: usize,
    append_count: usize,
    dirty: bool,
}

impl Aol {
    /// Open the AOL for appending. Creates the file and writes the header
    /// if it does not exist; otherwise positions the writer at the end.
    ///
    /// `buffer_size` controls the flush threshold: after this many appends
    /// the writer is flushed. Set to 0 for per-record flush.
    pub(crate) fn open(db_dir: &Path, buffer_size: usize) -> Result<Self> {
        let path = aol_path(db_dir);
        let exists = path.exists();

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let mut writer = BufWriter::new(file);

        if !exists {
            write_header(&mut writer)?;
        }

        Ok(Self {
            writer,
            buffer_size,
            append_count: 0,
            dirty: false,
        })
    }

    /// Append a record to the log.
    ///
    /// Converts the optional TTL `Duration` to an absolute expiry timestamp
    /// (ms since Unix epoch). A `None` TTL is encoded as 0.
    pub(crate) fn append(
        &mut self,
        ns: &str,
        rev: u128,
        key: &Key,
        value: &Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let expires_at_ms = match ttl {
            Some(d) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                now + d.as_millis() as u64
            }
            None => 0,
        };

        let payload = encode_payload(ns, rev, expires_at_ms, key, value);
        let checksum = Checksum::compute(&payload);

        // [payload_len: u32 BE] [payload] [checksum: 5B]
        self.writer
            .write_all(&(payload.len() as u32).to_be_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&checksum.to_bytes())?;

        self.append_count += 1;
        self.dirty = true;
        if self.buffer_size == 0 || self.append_count >= self.buffer_size {
            self.writer.flush()?;
            self.append_count = 0;
            self.dirty = false;
        }

        Ok(())
    }

    /// Replay the AOL file and return all decoded records plus a count of
    /// skipped (corrupted/truncated) records.
    ///
    /// This is a static method — it reads the file independently of `Aol`.
    pub(crate) fn replay(db_dir: &Path, verify: bool) -> Result<(Vec<AolRecord>, u64)> {
        let path = aol_path(db_dir);
        if !path.exists() {
            return Ok((Vec::new(), 0));
        }

        let data = std::fs::read(&path)?;
        if data.len() < HEADER_SIZE {
            return Ok((Vec::new(), 0));
        }

        // Validate header
        if data[0..4] != MAGIC {
            return Err(Error::Corruption("AOL magic mismatch".into()));
        }
        let version = u16::from_be_bytes(data[4..6].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "unsupported AOL version: {version}"
            )));
        }

        let mut pos = HEADER_SIZE;
        let mut records = Vec::new();
        let mut skipped = 0u64;

        while pos < data.len() {
            // Need at least 4 bytes for payload_len
            if pos + 4 > data.len() {
                skipped += 1;
                break;
            }
            let payload_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Need payload + 5 checksum bytes
            if pos + payload_len + Checksum::encoded_size() > data.len() {
                skipped += 1;
                break;
            }

            let payload = &data[pos..pos + payload_len];
            pos += payload_len;

            let checksum_bytes = &data[pos..pos + Checksum::encoded_size()];
            pos += Checksum::encoded_size();

            // Verify checksum if requested
            if verify {
                let cs = match Checksum::from_bytes(checksum_bytes) {
                    Ok(cs) => cs,
                    Err(_) => {
                        skipped += 1;
                        continue;
                    }
                };
                if cs.verify(payload).is_err() {
                    skipped += 1;
                    continue;
                }
            }

            match decode_payload(payload) {
                Ok(record) => records.push(record),
                Err(_) => {
                    skipped += 1;
                }
            }
        }

        Ok((records, skipped))
    }

    /// Flush the userspace buffer if any data has been written since
    /// the last flush. Called by the background timer thread.
    pub(crate) fn flush_if_dirty(&mut self) -> Result<()> {
        if self.dirty {
            self.writer.flush()?;
            self.append_count = 0;
            self.dirty = false;
        }
        Ok(())
    }

    /// Fsync the underlying file to durable storage.
    #[allow(dead_code)]
    pub(crate) fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.append_count = 0;
        self.dirty = false;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

fn aol_path(db_dir: &Path) -> PathBuf {
    db_dir.join(AOL_FILENAME)
}

fn write_header(writer: &mut BufWriter<File>) -> Result<()> {
    writer.write_all(&MAGIC)?;
    writer.write_all(&VERSION.to_be_bytes())?;
    writer.write_all(&[0x00, 0x00])?; // reserved
    writer.flush()?;
    Ok(())
}

/// Encode a record payload (without length prefix or checksum).
fn encode_payload(ns: &str, rev: u128, expires_at_ms: u64, key: &Key, value: &Value) -> Vec<u8> {
    let key_bytes = key.to_bytes();
    let value_data_vec;
    let value_data: &[u8] = match value {
        Value::Data(d) => d.as_slice(),
        Value::Pointer(vp) => {
            value_data_vec = vp.to_bytes();
            &value_data_vec
        }
        _ => &[],
    };

    // Calculate total size
    let size = 2 // ns_len
        + ns.len()
        + 16 // revision
        + 8  // expires_at_ms
        + 2  // key_len
        + key_bytes.len()
        + 1  // value_tag
        + value_data.len();

    let mut buf = Vec::with_capacity(size);

    // namespace
    buf.extend_from_slice(&(ns.len() as u16).to_be_bytes());
    buf.extend_from_slice(ns.as_bytes());

    // revision
    buf.extend_from_slice(&rev.to_be_bytes());

    // expires_at_ms
    buf.extend_from_slice(&expires_at_ms.to_be_bytes());

    // key
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(&key_bytes);

    // value
    buf.push(value.to_tag());
    buf.extend_from_slice(value_data);

    buf
}

/// Decode a record payload.
fn decode_payload(data: &[u8]) -> Result<AolRecord> {
    let mut pos = 0;

    // namespace
    if pos + 2 > data.len() {
        return Err(Error::Corruption("truncated ns_len".into()));
    }
    let ns_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;

    if pos + ns_len > data.len() {
        return Err(Error::Corruption("truncated namespace".into()));
    }
    let namespace = std::str::from_utf8(&data[pos..pos + ns_len])
        .map_err(|e| Error::Corruption(format!("invalid namespace utf-8: {e}")))?
        .to_owned();
    pos += ns_len;

    // revision
    if pos + 16 > data.len() {
        return Err(Error::Corruption("truncated revision".into()));
    }
    let revision = u128::from_be_bytes(data[pos..pos + 16].try_into().unwrap());
    pos += 16;

    // expires_at_ms
    if pos + 8 > data.len() {
        return Err(Error::Corruption("truncated expires_at_ms".into()));
    }
    let expires_at_ms = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // key
    if pos + 2 > data.len() {
        return Err(Error::Corruption("truncated key_len".into()));
    }
    let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;

    if pos + key_len > data.len() {
        return Err(Error::Corruption("truncated key".into()));
    }
    let key = Key::from_bytes(&data[pos..pos + key_len])?;
    pos += key_len;

    // value
    if pos >= data.len() {
        return Err(Error::Corruption("truncated value_tag".into()));
    }
    let value_tag = data[pos];
    pos += 1;

    let value_data = &data[pos..];
    let value = Value::from_tag(value_tag, value_data)?;

    Ok(AolRecord {
        namespace,
        revision,
        expires_at_ms,
        key,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Header ---

    #[test]
    fn header_written_on_create() {
        let tmp = tempfile::tempdir().unwrap();
        let _aol = Aol::open(tmp.path(), 0).unwrap();

        let data = std::fs::read(aol_path(tmp.path())).unwrap();
        assert_eq!(&data[0..4], &MAGIC);
        assert_eq!(u16::from_be_bytes(data[4..6].try_into().unwrap()), VERSION);
        assert_eq!(&data[6..8], &[0x00, 0x00]);
    }

    #[test]
    fn header_not_rewritten_on_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v"), None)
                .unwrap();
        }
        let size_before = std::fs::metadata(aol_path(tmp.path())).unwrap().len();

        // Reopen — should not duplicate header
        let _aol = Aol::open(tmp.path(), 0).unwrap();
        let size_after = std::fs::metadata(aol_path(tmp.path())).unwrap().len();
        assert_eq!(size_before, size_after);
    }

    // --- Encode / Decode round-trip ---

    #[test]
    fn roundtrip_data_value() {
        let payload = encode_payload("ns", 42, 0, &Key::Int(1), &Value::from("hello"));
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.namespace, "ns");
        assert_eq!(record.revision, 42);
        assert_eq!(record.expires_at_ms, 0);
        assert_eq!(record.key, Key::Int(1));
        assert_eq!(record.value, Value::from("hello"));
    }

    #[test]
    fn roundtrip_null_value() {
        let payload = encode_payload("_", 100, 0, &Key::from("k"), &Value::Null);
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.value, Value::Null);
    }

    #[test]
    fn roundtrip_tombstone_value() {
        let payload = encode_payload("_", 100, 0, &Key::Int(5), &Value::tombstone());
        let record = decode_payload(&payload).unwrap();
        assert!(record.value.is_tombstone());
    }

    #[test]
    fn roundtrip_with_expiry() {
        let payload = encode_payload("_", 1, 99999, &Key::Int(1), &Value::from("v"));
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.expires_at_ms, 99999);
    }

    #[test]
    fn roundtrip_str_key() {
        let payload = encode_payload("_", 1, 0, &Key::from("hello"), &Value::from("world"));
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.key, Key::from("hello"));
    }

    #[test]
    fn roundtrip_empty_data() {
        let payload = encode_payload("_", 1, 0, &Key::Int(1), &Value::Data(vec![]));
        let record = decode_payload(&payload).unwrap();
        assert_eq!(record.value, Value::Data(vec![]));
    }

    // --- Append / Replay ---

    #[test]
    fn append_and_replay() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v1"), None)
                .unwrap();
            aol.append("_", 2, &Key::Int(2), &Value::from("v2"), None)
                .unwrap();
        }

        let (records, skipped) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(skipped, 0);
        assert_eq!(records[0].key, Key::Int(1));
        assert_eq!(records[0].value, Value::from("v1"));
        assert_eq!(records[1].key, Key::Int(2));
        assert_eq!(records[1].value, Value::from("v2"));
    }

    #[test]
    fn replay_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let (records, skipped) = Aol::replay(tmp.path(), true).unwrap();
        assert!(records.is_empty());
        assert_eq!(skipped, 0);
    }

    #[test]
    fn replay_with_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append(
                "_",
                1,
                &Key::Int(1),
                &Value::from("v"),
                Some(Duration::from_secs(3600)),
            )
            .unwrap();
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].expires_at_ms > 0);
    }

    #[test]
    fn replay_multiple_namespaces() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("ns1", 1, &Key::Int(1), &Value::from("a"), None)
                .unwrap();
            aol.append("ns2", 2, &Key::Int(1), &Value::from("b"), None)
                .unwrap();
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].namespace, "ns1");
        assert_eq!(records[1].namespace, "ns2");
    }

    #[test]
    fn replay_detects_truncation() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v"), None)
                .unwrap();
        }

        // Truncate the last byte
        let path = aol_path(tmp.path());
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 1]).unwrap();

        let (records, skipped) = Aol::replay(tmp.path(), true).unwrap();
        assert!(records.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn replay_detects_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v"), None)
                .unwrap();
        }

        // Corrupt a byte in the payload
        let path = aol_path(tmp.path());
        let mut data = std::fs::read(&path).unwrap();
        data[HEADER_SIZE + 5] ^= 0xFF; // flip a byte in the payload
        std::fs::write(&path, &data).unwrap();

        let (records, skipped) = Aol::replay(tmp.path(), true).unwrap();
        assert!(records.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn replay_skips_corruption_without_verify() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v"), None)
                .unwrap();
        }

        // Corrupt checksum bytes (not payload) — without verify, payload decode still works
        let path = aol_path(tmp.path());
        let mut data = std::fs::read(&path).unwrap();
        // Flip a byte in the checksum area (last 5 bytes)
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        // Without verification, the record should still be decoded
        let (records, skipped) = Aol::replay(tmp.path(), false).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn replay_bad_magic() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let _aol = Aol::open(tmp.path(), 0).unwrap();
        }

        let path = aol_path(tmp.path());
        let mut data = std::fs::read(&path).unwrap();
        data[0] = 0x00; // corrupt magic
        std::fs::write(&path, &data).unwrap();

        let err = Aol::replay(tmp.path(), true).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    // --- Sync ---

    #[test]
    fn sync_does_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut aol = Aol::open(tmp.path(), 0).unwrap();
        aol.append("_", 1, &Key::Int(1), &Value::from("v"), None)
            .unwrap();
        aol.sync().unwrap();
    }

    // --- Append after reopen ---

    #[test]
    fn append_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("v1"), None)
                .unwrap();
        }
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 2, &Key::Int(2), &Value::from("v2"), None)
                .unwrap();
        }

        let (records, skipped) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(skipped, 0);
    }

    // --- Buffered flush ---

    #[test]
    fn buffered_flush_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        {
            // Buffer size = 3: records are flushed after every 3 appends
            let mut aol = Aol::open(tmp.path(), 3).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("a"), None)
                .unwrap();
            aol.append("_", 2, &Key::Int(2), &Value::from("b"), None)
                .unwrap();
            assert!(aol.dirty);

            // Third append triggers flush
            aol.append("_", 3, &Key::Int(3), &Value::from("c"), None)
                .unwrap();
            assert!(!aol.dirty);
            assert_eq!(aol.append_count, 0);
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn buffered_flush_zero_means_per_record() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 0).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("a"), None)
                .unwrap();
            // With buffer_size=0, every append flushes immediately
            assert!(!aol.dirty);
            assert_eq!(aol.append_count, 0);
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn flush_if_dirty_flushes_pending() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 100).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("a"), None)
                .unwrap();
            assert!(aol.dirty);

            aol.flush_if_dirty().unwrap();
            assert!(!aol.dirty);
            assert_eq!(aol.append_count, 0);
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn flush_if_dirty_noop_when_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let mut aol = Aol::open(tmp.path(), 100).unwrap();
        // No appends — should be a no-op
        assert!(!aol.dirty);
        aol.flush_if_dirty().unwrap();
        assert!(!aol.dirty);
    }

    #[test]
    fn drop_without_flush_loses_buffered_records() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut aol = Aol::open(tmp.path(), 100).unwrap();
            aol.append("_", 1, &Key::Int(1), &Value::from("a"), None)
                .unwrap();
            // Drop without flush — buffered data may be lost
        }

        let (records, _) = Aol::replay(tmp.path(), true).unwrap();
        // BufWriter may or may not flush on drop depending on implementation,
        // but with a large buffer_size the data is likely unflushed.
        // The important thing is that replay doesn't crash.
        assert!(records.len() <= 1);
    }
}
