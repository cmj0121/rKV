use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::error::{Error, Result};
use super::replication::{ReplMessage, Role};

/// Messages broadcast from the primary engine to connected replicas.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BroadcastMsg {
    /// Raw AOL record payload.
    Aol(Vec<u8>),
    /// Instruction to drop a namespace.
    DropNamespace(String),
}

/// Manages the primary-side replication listener and connected replicas.
///
/// The sender listens on a TCP port and accepts replica connections. For each
/// replica it performs a full sync (streaming SSTable and object files from
/// disk) followed by live AOL record streaming.
#[allow(dead_code)] // consumed by DB integration (upcoming commit)
pub(crate) struct ReplSender {
    listener_handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    /// Per-replica channels for broadcasting messages.
    aol_senders: Arc<Mutex<Vec<mpsc::SyncSender<BroadcastMsg>>>>,
}

/// Context passed to each replica handler thread.
#[allow(dead_code)]
struct ReplicaCtx {
    db_path: PathBuf,
    cluster_id: u16,
    max_levels: usize,
    aol_rx: mpsc::Receiver<BroadcastMsg>,
    stop: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl ReplSender {
    /// Start the replication listener on `bind:port`.
    ///
    /// `flush_fn` is called before each full sync to ensure all memtable data
    /// is persisted to SSTables on disk.
    pub(crate) fn start<F>(
        bind: &str,
        port: u16,
        cluster_id: u16,
        db_path: PathBuf,
        max_levels: usize,
        flush_fn: F,
        stop: Arc<AtomicBool>,
    ) -> Result<Self>
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        let addr = format!("{bind}:{port}");
        let listener = TcpListener::bind(&addr)?;
        listener.set_nonblocking(true)?;

        let aol_senders: Arc<Mutex<Vec<mpsc::SyncSender<BroadcastMsg>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let senders_clone = Arc::clone(&aol_senders);
        let stop_clone = Arc::clone(&stop);
        let flush_fn = Arc::new(flush_fn);

        let listener_handle = thread::spawn(move || {
            Self::listener_loop(
                listener,
                senders_clone,
                stop_clone,
                db_path,
                cluster_id,
                max_levels,
                flush_fn,
            );
        });

        Ok(Self {
            listener_handle: Some(listener_handle),
            stop,
            aol_senders,
        })
    }

    /// Broadcast an AOL record payload to all connected replicas.
    ///
    /// Dead channels (disconnected replicas) are pruned automatically.
    pub(crate) fn broadcast_aol(&self, payload: &[u8]) {
        let mut senders = self.aol_senders.lock().unwrap_or_else(|e| e.into_inner());
        senders.retain(|tx| tx.try_send(BroadcastMsg::Aol(payload.to_vec())).is_ok());
    }

    /// Broadcast a namespace-drop instruction to all connected replicas.
    pub(crate) fn broadcast_drop_namespace(&self, namespace: &str) {
        let mut senders = self.aol_senders.lock().unwrap_or_else(|e| e.into_inner());
        senders.retain(|tx| {
            tx.try_send(BroadcastMsg::DropNamespace(namespace.to_owned()))
                .is_ok()
        });
    }

    /// Stop the listener and all replica handler threads.
    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
    }

    fn listener_loop(
        listener: TcpListener,
        aol_senders: Arc<Mutex<Vec<mpsc::SyncSender<BroadcastMsg>>>>,
        stop: Arc<AtomicBool>,
        db_path: PathBuf,
        cluster_id: u16,
        max_levels: usize,
        flush_fn: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    // Create a bounded channel for this replica
                    let (tx, rx) = mpsc::sync_channel::<BroadcastMsg>(4096);
                    {
                        let mut senders = aol_senders.lock().unwrap_or_else(|e| e.into_inner());
                        senders.push(tx);
                    }

                    // Flush before full sync
                    let _ = flush_fn();

                    let ctx = ReplicaCtx {
                        db_path: db_path.clone(),
                        cluster_id,
                        max_levels,
                        aol_rx: rx,
                        stop: Arc::clone(&stop),
                    };

                    thread::spawn(move || {
                        if let Err(e) = Self::handle_replica(stream, ctx) {
                            eprintln!("replication: replica {addr} disconnected: {e}");
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Non-blocking: no pending connections — sleep briefly
                    thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    eprintln!("replication: accept error: {e}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn handle_replica(stream: TcpStream, ctx: ReplicaCtx) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        let mut writer = BufWriter::new(stream.try_clone()?);
        let mut reader = BufReader::new(stream);

        // --- Handshake ---
        ReplMessage::write_handshake_header(&mut writer)?;
        ReplMessage::Handshake {
            cluster_id: ctx.cluster_id,
            role: Role::Primary,
        }
        .write_to(&mut writer)?;
        writer.flush()?;

        // Read replica handshake
        ReplMessage::read_handshake_header(&mut reader)?;
        match ReplMessage::read_from(&mut reader)? {
            Some(ReplMessage::Handshake { role, .. }) => {
                if role != Role::Replica {
                    return Err(Error::Corruption(format!(
                        "expected replica handshake, got {role}"
                    )));
                }
            }
            other => {
                return Err(Error::Corruption(format!(
                    "expected handshake message, got {other:?}"
                )));
            }
        }

        // --- Full sync ---
        Self::send_full_sync(&mut writer, &ctx)?;

        // --- Live streaming ---
        Self::live_stream(&mut writer, &ctx)
    }

    fn send_full_sync<W: Write>(writer: &mut W, ctx: &ReplicaCtx) -> Result<()> {
        // Enumerate SSTable and object files
        let sst_files = enumerate_sst_files(&ctx.db_path, ctx.max_levels);
        let obj_files = enumerate_object_files(&ctx.db_path);

        // Collect unique namespaces
        let mut namespaces = std::collections::HashSet::new();
        for (ns, _, _, _) in &sst_files {
            namespaces.insert(ns.clone());
        }
        for (ns, _, _) in &obj_files {
            namespaces.insert(ns.clone());
        }

        ReplMessage::FullSyncStart {
            namespace_count: namespaces.len() as u32,
            sst_count: sst_files.len() as u32,
            object_count: obj_files.len() as u32,
        }
        .write_to(writer)?;
        writer.flush()?;

        // Stream SSTable files
        for (namespace, level, sst_id, path) in &sst_files {
            if ctx.stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "replication stopped",
                )));
            }
            let data = std::fs::read(path)?;
            ReplMessage::SstChunk {
                namespace: namespace.clone(),
                level: *level,
                sst_id: *sst_id,
                data,
            }
            .write_to(writer)?;
        }

        // Stream object files
        for (namespace, hash, path) in &obj_files {
            if ctx.stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "replication stopped",
                )));
            }
            let data = std::fs::read(path)?;
            let mut hash_bytes = [0u8; 32];
            if let Ok(decoded) = hex_to_bytes(hash) {
                if decoded.len() == 32 {
                    hash_bytes.copy_from_slice(&decoded);
                }
            }
            ReplMessage::ObjectChunk {
                namespace: namespace.clone(),
                hash: hash_bytes,
                data,
            }
            .write_to(writer)?;
        }

        ReplMessage::FullSyncEnd.write_to(writer)?;
        writer.flush()?;

        Ok(())
    }

    fn write_broadcast_msg<W: Write>(writer: &mut W, msg: BroadcastMsg) -> Result<()> {
        match msg {
            BroadcastMsg::Aol(payload) => {
                ReplMessage::AolRecord { payload }.write_to(writer)?;
            }
            BroadcastMsg::DropNamespace(namespace) => {
                ReplMessage::DropNamespace { namespace }.write_to(writer)?;
            }
        }
        Ok(())
    }

    fn live_stream<W: Write>(writer: &mut W, ctx: &ReplicaCtx) -> Result<()> {
        let mut heartbeat_tick = 0u32;
        loop {
            if ctx.stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            // Try to receive broadcast messages (non-blocking with timeout)
            match ctx.aol_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(msg) => {
                    Self::write_broadcast_msg(writer, msg)?;
                    // Drain any additional pending messages
                    while let Ok(msg) = ctx.aol_rx.try_recv() {
                        Self::write_broadcast_msg(writer, msg)?;
                    }
                    writer.flush()?;
                    heartbeat_tick = 0;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    heartbeat_tick += 1;
                    // Send heartbeat every 10 seconds of inactivity
                    if heartbeat_tick >= 10 {
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        ReplMessage::Heartbeat { timestamp_ms: ts }.write_to(writer)?;
                        writer.flush()?;
                        heartbeat_tick = 0;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed — DB is shutting down
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// File enumeration helpers
// ---------------------------------------------------------------------------

/// Enumerate all SSTable files under `<db>/sst/`.
/// Returns `(namespace, level, sst_id, path)` tuples.
#[allow(dead_code)]
fn enumerate_sst_files(db_path: &Path, max_levels: usize) -> Vec<(String, u8, u64, PathBuf)> {
    let sst_root = db_path.join("sst");
    let mut results = Vec::new();

    let ns_dirs = match std::fs::read_dir(&sst_root) {
        Ok(d) => d,
        Err(_) => return results,
    };

    for ns_entry in ns_dirs.flatten() {
        if !ns_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let ns_name = ns_entry.file_name().to_string_lossy().to_string();

        for level in 0..max_levels {
            let level_dir = ns_entry.path().join(format!("L{level}"));
            let files = match std::fs::read_dir(&level_dir) {
                Ok(d) => d,
                Err(_) => continue,
            };

            for file_entry in files.flatten() {
                let fname = file_entry.file_name().to_string_lossy().to_string();
                if !fname.ends_with(".sst") {
                    continue;
                }
                if let Ok(seq) = fname.trim_end_matches(".sst").parse::<u64>() {
                    results.push((ns_name.clone(), level as u8, seq, file_entry.path()));
                }
            }
        }
    }

    results
}

/// Enumerate all bin object files under `<db>/objects/`.
/// Returns `(namespace, hex_hash, path)` tuples.
#[allow(dead_code)]
fn enumerate_object_files(db_path: &Path) -> Vec<(String, String, PathBuf)> {
    let obj_root = db_path.join("objects");
    let mut results = Vec::new();

    let ns_dirs = match std::fs::read_dir(&obj_root) {
        Ok(d) => d,
        Err(_) => return results,
    };

    for ns_entry in ns_dirs.flatten() {
        if !ns_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let ns_name = ns_entry.file_name().to_string_lossy().to_string();

        // Fan-out directories (2-hex prefix)
        let fan_dirs = match std::fs::read_dir(ns_entry.path()) {
            Ok(d) => d,
            Err(_) => continue,
        };

        for fan_entry in fan_dirs.flatten() {
            if !fan_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }

            let obj_files = match std::fs::read_dir(fan_entry.path()) {
                Ok(d) => d,
                Err(_) => continue,
            };

            for obj_entry in obj_files.flatten() {
                let hash = obj_entry.file_name().to_string_lossy().to_string();
                // Valid hash is 64 hex chars
                if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    results.push((ns_name.clone(), hash, obj_entry.path()));
                }
            }
        }
    }

    results
}

/// Decode a hex string to bytes.
#[allow(dead_code)]
fn hex_to_bytes(hex: &str) -> std::result::Result<Vec<u8>, ()> {
    if !hex.len().is_multiple_of(2) {
        return Err(());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn enumerate_sst_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let files = enumerate_sst_files(tmp.path(), 3);
        assert!(files.is_empty());
    }

    #[test]
    fn enumerate_sst_finds_files() {
        let tmp = tempfile::tempdir().unwrap();
        let l0 = tmp.path().join("sst/_/L0");
        std::fs::create_dir_all(&l0).unwrap();
        std::fs::write(l0.join("000001.sst"), b"sst-data").unwrap();
        std::fs::write(l0.join("000002.sst"), b"sst-data2").unwrap();

        let files = enumerate_sst_files(tmp.path(), 3);
        assert_eq!(files.len(), 2);
        assert!(files
            .iter()
            .any(|(ns, level, id, _)| ns == "_" && *level == 0 && *id == 1));
        assert!(files
            .iter()
            .any(|(ns, level, id, _)| ns == "_" && *level == 0 && *id == 2));
    }

    #[test]
    fn enumerate_sst_multiple_namespaces_and_levels() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("sst/ns1/L0")).unwrap();
        std::fs::create_dir_all(tmp.path().join("sst/ns1/L1")).unwrap();
        std::fs::create_dir_all(tmp.path().join("sst/ns2/L0")).unwrap();
        std::fs::write(tmp.path().join("sst/ns1/L0/000001.sst"), b"d").unwrap();
        std::fs::write(tmp.path().join("sst/ns1/L1/000002.sst"), b"d").unwrap();
        std::fs::write(tmp.path().join("sst/ns2/L0/000003.sst"), b"d").unwrap();

        let files = enumerate_sst_files(tmp.path(), 3);
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn enumerate_obj_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let files = enumerate_object_files(tmp.path());
        assert!(files.is_empty());
    }

    #[test]
    fn enumerate_obj_finds_files() {
        let tmp = tempfile::tempdir().unwrap();
        let fan = tmp.path().join("objects/_/ab");
        std::fs::create_dir_all(&fan).unwrap();
        let hash = "ab".to_string() + &"cd".repeat(31);
        std::fs::write(fan.join(&hash), b"obj-data").unwrap();

        let files = enumerate_object_files(tmp.path());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "_");
        assert_eq!(files[0].1, hash);
    }

    #[test]
    fn hex_to_bytes_valid() {
        assert_eq!(
            hex_to_bytes("deadbeef").unwrap(),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn hex_to_bytes_empty() {
        assert_eq!(hex_to_bytes("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn hex_to_bytes_invalid() {
        assert!(hex_to_bytes("xyz").is_err());
        assert!(hex_to_bytes("a").is_err()); // odd length
    }

    #[test]
    fn full_sync_empty_db() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = ReplicaCtx {
            db_path: tmp.path().to_path_buf(),
            cluster_id: 1,
            max_levels: 3,
            aol_rx: mpsc::sync_channel(1).1,
            stop: Arc::new(AtomicBool::new(false)),
        };

        let mut buf = Vec::new();
        ReplSender::send_full_sync(&mut buf, &ctx).unwrap();

        // Decode the messages
        let mut cursor = Cursor::new(buf);
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert_eq!(
            msg,
            ReplMessage::FullSyncStart {
                namespace_count: 0,
                sst_count: 0,
                object_count: 0,
            }
        );
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert_eq!(msg, ReplMessage::FullSyncEnd);
    }

    #[test]
    fn full_sync_with_sst_files() {
        let tmp = tempfile::tempdir().unwrap();
        let l0 = tmp.path().join("sst/myns/L0");
        std::fs::create_dir_all(&l0).unwrap();
        std::fs::write(l0.join("000001.sst"), b"sst-content").unwrap();

        let ctx = ReplicaCtx {
            db_path: tmp.path().to_path_buf(),
            cluster_id: 1,
            max_levels: 3,
            aol_rx: mpsc::sync_channel(1).1,
            stop: Arc::new(AtomicBool::new(false)),
        };

        let mut buf = Vec::new();
        ReplSender::send_full_sync(&mut buf, &ctx).unwrap();

        let mut cursor = Cursor::new(buf);

        // FullSyncStart
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert!(matches!(
            msg,
            ReplMessage::FullSyncStart {
                namespace_count: 1,
                sst_count: 1,
                object_count: 0,
            }
        ));

        // SstChunk
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        match msg {
            ReplMessage::SstChunk {
                namespace,
                level,
                sst_id,
                data,
            } => {
                assert_eq!(namespace, "myns");
                assert_eq!(level, 0);
                assert_eq!(sst_id, 1);
                assert_eq!(data, b"sst-content");
            }
            other => panic!("expected SstChunk, got {other:?}"),
        }

        // FullSyncEnd
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert_eq!(msg, ReplMessage::FullSyncEnd);
    }

    #[test]
    fn broadcast_aol_to_receivers() {
        let (tx1, rx1) = mpsc::sync_channel(16);
        let (tx2, rx2) = mpsc::sync_channel(16);

        let senders = Arc::new(Mutex::new(vec![tx1, tx2]));
        let sender = ReplSender {
            listener_handle: None,
            stop: Arc::new(AtomicBool::new(false)),
            aol_senders: senders,
        };

        sender.broadcast_aol(b"record1");
        sender.broadcast_aol(b"record2");

        assert!(matches!(rx1.try_recv().unwrap(), BroadcastMsg::Aol(ref p) if p == b"record1"));
        assert!(matches!(rx1.try_recv().unwrap(), BroadcastMsg::Aol(ref p) if p == b"record2"));
        assert!(matches!(rx2.try_recv().unwrap(), BroadcastMsg::Aol(ref p) if p == b"record1"));
        assert!(matches!(rx2.try_recv().unwrap(), BroadcastMsg::Aol(ref p) if p == b"record2"));
    }

    #[test]
    fn broadcast_aol_prunes_dead_channels() {
        let (tx1, rx1) = mpsc::sync_channel(16);
        let (tx2, _rx2_dropped) = mpsc::sync_channel(16);
        // Drop rx2 to simulate disconnected replica
        drop(_rx2_dropped);

        let senders = Arc::new(Mutex::new(vec![tx1, tx2]));
        let sender = ReplSender {
            listener_handle: None,
            stop: Arc::new(AtomicBool::new(false)),
            aol_senders: senders,
        };

        sender.broadcast_aol(b"record");

        // rx1 should receive it
        assert!(matches!(rx1.try_recv().unwrap(), BroadcastMsg::Aol(ref p) if p == b"record"));

        // Dead channel should be pruned
        let count = sender.aol_senders.lock().unwrap().len();
        assert_eq!(count, 1);
    }

    #[test]
    fn broadcast_drop_namespace_to_receivers() {
        let (tx1, rx1) = mpsc::sync_channel(16);

        let senders = Arc::new(Mutex::new(vec![tx1]));
        let sender = ReplSender {
            listener_handle: None,
            stop: Arc::new(AtomicBool::new(false)),
            aol_senders: senders,
        };

        sender.broadcast_drop_namespace("myns");

        match rx1.try_recv().unwrap() {
            BroadcastMsg::DropNamespace(ns) => assert_eq!(ns, "myns"),
            other => panic!("expected DropNamespace, got: {other:?}"),
        }
    }
}
