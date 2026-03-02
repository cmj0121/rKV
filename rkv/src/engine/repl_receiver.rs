use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::error::{Error, Result};
use super::replication::{ReplMessage, Role};

/// Callback signature for replaying an AOL record payload on the replica.
///
/// The replica's DB passes a closure that decodes the payload and applies it
/// to the local memtable (same logic as `Aol::replay` but for a single record).
pub(crate) type ReplayFn = Box<dyn Fn(&[u8]) -> Result<()> + Send + Sync>;

/// Callback invoked after full sync completes (SSTable/object files written).
///
/// The replica's DB uses this to reload its in-memory SSTable index so that
/// data received during full sync is immediately queryable.
pub(crate) type PostSyncFn = Box<dyn Fn() -> Result<()> + Send + Sync>;

/// Manages the replica-side replication connection to a primary.
///
/// The receiver connects to the primary, performs a handshake, receives a
/// full sync (SSTable + object files), then enters a live-stream loop
/// consuming AOL records.
#[allow(dead_code)] // consumed by DB integration (upcoming commit)
pub(crate) struct ReplReceiver {
    receiver_handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl ReplReceiver {
    /// Connect to the primary at `addr` and start the replication loop.
    ///
    /// `db_path` is the local database directory where SSTable/object files
    /// are written during full sync. `replay_fn` is called for each AOL
    /// record received during live streaming.
    pub(crate) fn start(
        addr: &str,
        cluster_id: u16,
        db_path: PathBuf,
        max_levels: usize,
        replay_fn: ReplayFn,
        post_sync_fn: PostSyncFn,
        stop: Arc<AtomicBool>,
    ) -> Result<Self> {
        let addr = addr.to_owned();
        let stop_clone = Arc::clone(&stop);

        let receiver_handle = thread::spawn(move || {
            Self::run_loop(
                &addr,
                cluster_id,
                &db_path,
                max_levels,
                &replay_fn,
                &post_sync_fn,
                &stop_clone,
            );
        });

        Ok(Self {
            receiver_handle: Some(receiver_handle),
            stop,
        })
    }

    /// Stop the receiver thread.
    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.receiver_handle.take() {
            let _ = handle.join();
        }
    }

    /// Reconnection loop — retries on failure with exponential backoff.
    fn run_loop(
        addr: &str,
        cluster_id: u16,
        db_path: &Path,
        max_levels: usize,
        replay_fn: &ReplayFn,
        post_sync_fn: &PostSyncFn,
        stop: &Arc<AtomicBool>,
    ) {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        while !stop.load(Ordering::Relaxed) {
            match Self::connect_and_replicate(
                addr,
                cluster_id,
                db_path,
                max_levels,
                replay_fn,
                post_sync_fn,
                stop,
            ) {
                Ok(()) => {
                    // Clean exit (stop signal)
                    break;
                }
                Err(e) => {
                    eprintln!("replication: connection to {addr} failed: {e}");
                    // Wait with backoff before retrying
                    let sleep_end = std::time::Instant::now() + backoff;
                    while std::time::Instant::now() < sleep_end {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        thread::sleep(Duration::from_millis(200));
                    }
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }

    /// Single connection attempt: handshake → full sync → live stream.
    fn connect_and_replicate(
        addr: &str,
        cluster_id: u16,
        db_path: &Path,
        max_levels: usize,
        replay_fn: &ReplayFn,
        post_sync_fn: &PostSyncFn,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        // Set read timeout so we can check the stop flag periodically
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;

        let mut writer = BufWriter::new(stream.try_clone()?);
        let mut reader = BufReader::new(stream);

        // --- Handshake ---
        // Read primary's header + handshake
        ReplMessage::read_handshake_header(&mut reader)?;
        match ReplMessage::read_from(&mut reader)? {
            Some(ReplMessage::Handshake { role, .. }) => {
                if role != Role::Primary {
                    return Err(Error::Corruption(format!(
                        "expected primary handshake, got {role}"
                    )));
                }
            }
            other => {
                return Err(Error::Corruption(format!(
                    "expected handshake message, got {other:?}"
                )));
            }
        }

        // Send replica handshake
        ReplMessage::write_handshake_header(&mut writer)?;
        ReplMessage::Handshake {
            cluster_id,
            role: Role::Replica,
        }
        .write_to(&mut writer)?;
        writer.flush()?;

        // --- Full sync ---
        Self::receive_full_sync(&mut reader, db_path, max_levels, stop)?;

        // Reload SSTable index so synced files are queryable
        post_sync_fn()?;

        // --- Live streaming ---
        Self::receive_live_stream(&mut reader, replay_fn, stop)
    }

    fn receive_full_sync<R: std::io::Read>(
        reader: &mut R,
        db_path: &Path,
        max_levels: usize,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        // Expect FullSyncStart
        let (sst_count, object_count) = match ReplMessage::read_from(reader)? {
            Some(ReplMessage::FullSyncStart {
                sst_count,
                object_count,
                ..
            }) => (sst_count, object_count),
            Some(other) => {
                return Err(Error::Corruption(format!(
                    "expected FullSyncStart, got {other:?}"
                )));
            }
            None => {
                return Err(Error::Corruption(
                    "unexpected EOF waiting for FullSyncStart".into(),
                ));
            }
        };

        let total_expected = sst_count + object_count;
        let mut received = 0u32;

        loop {
            if stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "replication stopped",
                )));
            }

            let msg = match ReplMessage::read_from(reader)? {
                Some(msg) => msg,
                None => {
                    return Err(Error::Corruption("unexpected EOF during full sync".into()));
                }
            };

            match msg {
                ReplMessage::SstChunk {
                    namespace,
                    level,
                    sst_id,
                    data,
                } => {
                    if (level as usize) >= max_levels {
                        return Err(Error::Corruption(format!(
                            "SST level {level} exceeds max_levels {max_levels}"
                        )));
                    }
                    write_sst_file(db_path, &namespace, level, sst_id, &data)?;
                    received += 1;
                }
                ReplMessage::ObjectChunk {
                    namespace,
                    hash,
                    data,
                } => {
                    write_object_file(db_path, &namespace, &hash, &data)?;
                    received += 1;
                }
                ReplMessage::FullSyncEnd => {
                    if received != total_expected {
                        eprintln!(
                            "replication: full sync expected {total_expected} files, got {received}"
                        );
                    }
                    return Ok(());
                }
                ReplMessage::ErrorMsg { message } => {
                    return Err(Error::Corruption(format!(
                        "primary error during full sync: {message}"
                    )));
                }
                other => {
                    return Err(Error::Corruption(format!(
                        "unexpected message during full sync: {other:?}"
                    )));
                }
            }
        }
    }

    fn receive_live_stream<R: std::io::Read>(
        reader: &mut R,
        replay_fn: &ReplayFn,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            let msg = match ReplMessage::read_from(reader) {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    // Primary closed connection
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "primary closed connection",
                    )));
                }
                Err(Error::Io(ref e))
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Read timeout — check stop flag and retry
                    continue;
                }
                Err(e) => return Err(e),
            };

            match msg {
                ReplMessage::AolRecord { payload } => {
                    replay_fn(&payload)?;
                }
                ReplMessage::Heartbeat { .. } => {
                    // Heartbeat — connection is alive, nothing to do
                }
                ReplMessage::ErrorMsg { message } => {
                    return Err(Error::Corruption(format!(
                        "primary error during live stream: {message}"
                    )));
                }
                other => {
                    eprintln!("replication: ignoring unexpected message: {other:?}");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// File writing helpers
// ---------------------------------------------------------------------------

/// Write an SSTable file received during full sync.
#[allow(dead_code)]
fn write_sst_file(
    db_path: &Path,
    namespace: &str,
    level: u8,
    sst_id: u64,
    data: &[u8],
) -> Result<()> {
    let level_dir = db_path
        .join("sst")
        .join(namespace)
        .join(format!("L{level}"));
    std::fs::create_dir_all(&level_dir)?;
    let path = level_dir.join(format!("{sst_id:06}.sst"));
    std::fs::write(&path, data)?;
    Ok(())
}

/// Write a bin object file received during full sync.
#[allow(dead_code)]
fn write_object_file(db_path: &Path, namespace: &str, hash: &[u8; 32], data: &[u8]) -> Result<()> {
    let hex_hash = bytes_to_hex(hash);
    let fan_prefix = &hex_hash[..2];
    let obj_dir = db_path.join("objects").join(namespace).join(fan_prefix);
    std::fs::create_dir_all(&obj_dir)?;
    let path = obj_dir.join(&hex_hash);
    std::fs::write(&path, data)?;
    Ok(())
}

/// Encode bytes as lowercase hex string.
#[allow(dead_code)]
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use super::super::replication::ReplMessage;

    #[test]
    fn write_sst_file_creates_dirs_and_file() {
        let tmp = tempfile::tempdir().unwrap();
        write_sst_file(tmp.path(), "myns", 0, 1, b"sst-data").unwrap();

        let path = tmp.path().join("sst/myns/L0/000001.sst");
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"sst-data");
    }

    #[test]
    fn write_sst_file_overwrites_existing() {
        let tmp = tempfile::tempdir().unwrap();
        write_sst_file(tmp.path(), "_", 1, 42, b"old").unwrap();
        write_sst_file(tmp.path(), "_", 1, 42, b"new").unwrap();

        let path = tmp.path().join("sst/_/L1/000042.sst");
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn write_object_file_creates_dirs_and_file() {
        let tmp = tempfile::tempdir().unwrap();
        let hash = [0xABu8; 32];
        write_object_file(tmp.path(), "_", &hash, b"obj-data").unwrap();

        let hex = bytes_to_hex(&hash);
        let path = tmp.path().join(format!("objects/_/{}/{hex}", &hex[..2]));
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"obj-data");
    }

    #[test]
    fn bytes_to_hex_roundtrip() {
        let input = [0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(bytes_to_hex(&input), "deadbeef");
    }

    #[test]
    fn bytes_to_hex_zeros() {
        let input = [0u8; 4];
        assert_eq!(bytes_to_hex(&input), "00000000");
    }

    #[test]
    fn receive_full_sync_empty() {
        // Simulate an empty full sync (no files)
        let mut buf = Vec::new();
        ReplMessage::FullSyncStart {
            namespace_count: 0,
            sst_count: 0,
            object_count: 0,
        }
        .write_to(&mut buf)
        .unwrap();
        ReplMessage::FullSyncEnd.write_to(&mut buf).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);
        ReplReceiver::receive_full_sync(&mut cursor, tmp.path(), 3, &stop).unwrap();
    }

    #[test]
    fn receive_full_sync_with_sst_and_objects() {
        let mut buf = Vec::new();
        ReplMessage::FullSyncStart {
            namespace_count: 1,
            sst_count: 1,
            object_count: 1,
        }
        .write_to(&mut buf)
        .unwrap();

        ReplMessage::SstChunk {
            namespace: "ns1".to_string(),
            level: 0,
            sst_id: 1,
            data: b"sst-content".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        ReplMessage::ObjectChunk {
            namespace: "ns1".to_string(),
            hash: [0xCC; 32],
            data: b"obj-content".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        ReplMessage::FullSyncEnd.write_to(&mut buf).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);
        ReplReceiver::receive_full_sync(&mut cursor, tmp.path(), 3, &stop).unwrap();

        // Verify SST file was written
        let sst_path = tmp.path().join("sst/ns1/L0/000001.sst");
        assert!(sst_path.exists());
        assert_eq!(std::fs::read(&sst_path).unwrap(), b"sst-content");

        // Verify object file was written
        let hash_hex = bytes_to_hex(&[0xCC; 32]);
        let obj_path = tmp
            .path()
            .join(format!("objects/ns1/{}/{hash_hex}", &hash_hex[..2]));
        assert!(obj_path.exists());
        assert_eq!(std::fs::read(&obj_path).unwrap(), b"obj-content");
    }

    #[test]
    fn receive_full_sync_rejects_bad_level() {
        let mut buf = Vec::new();
        ReplMessage::FullSyncStart {
            namespace_count: 1,
            sst_count: 1,
            object_count: 0,
        }
        .write_to(&mut buf)
        .unwrap();

        ReplMessage::SstChunk {
            namespace: "_".to_string(),
            level: 10, // exceeds max_levels=3
            sst_id: 1,
            data: b"bad".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);
        let err = ReplReceiver::receive_full_sync(&mut cursor, tmp.path(), 3, &stop).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn receive_live_stream_aol_records() {
        let mut buf = Vec::new();
        ReplMessage::AolRecord {
            payload: b"record-1".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();
        ReplMessage::Heartbeat {
            timestamp_ms: 12345,
        }
        .write_to(&mut buf)
        .unwrap();
        ReplMessage::AolRecord {
            payload: b"record-2".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        let replay_fn: ReplayFn = Box::new(move |payload: &[u8]| {
            received_clone.lock().unwrap().push(payload.to_vec());
            Ok(())
        });

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // The stream will end with EOF → ConnectionReset error
        let result = ReplReceiver::receive_live_stream(&mut cursor, &replay_fn, &stop);
        assert!(result.is_err()); // EOF → connection reset

        let records = received.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], b"record-1");
        assert_eq!(records[1], b"record-2");
    }

    #[test]
    fn receive_live_stream_stops_on_error_msg() {
        let mut buf = Vec::new();
        ReplMessage::AolRecord {
            payload: b"ok".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();
        ReplMessage::ErrorMsg {
            message: "primary shutting down".to_string(),
        }
        .write_to(&mut buf)
        .unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        let replay_fn: ReplayFn = Box::new(move |payload: &[u8]| {
            received_clone.lock().unwrap().push(payload.to_vec());
            Ok(())
        });

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        let result = ReplReceiver::receive_live_stream(&mut cursor, &replay_fn, &stop);
        assert!(result.is_err());
        assert!(format!("{:?}", result.unwrap_err()).contains("primary shutting down"));

        let records = received.lock().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn receive_live_stream_stop_flag() {
        // Empty stream — but stop is already set, so it exits immediately
        let buf = Vec::new();
        let replay_fn: ReplayFn = Box::new(|_| Ok(()));

        let stop = Arc::new(AtomicBool::new(true));
        let mut cursor = Cursor::new(buf);

        let result = ReplReceiver::receive_live_stream(&mut cursor, &replay_fn, &stop);
        assert!(result.is_ok());
    }

    use std::sync::Mutex;
}
