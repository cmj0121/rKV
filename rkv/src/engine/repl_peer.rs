use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::error::{Error, Result};
use super::io::IoBackend;
use super::replication::{ReplMessage, Role};

/// Messages sent between peer sessions.
#[derive(Debug)]
pub(crate) enum PeerMsg {
    /// Raw AOL record payload to forward to the peer.
    Aol(Vec<u8>),
}

/// Callback signature for replaying a peer record with LWW resolution.
/// Returns `true` if the record was applied (newer), `false` if skipped.
pub(crate) type PeerReplayFn = Arc<dyn Fn(&[u8]) -> Result<bool> + Send + Sync>;

/// Callback to broadcast an accepted record to other peer sessions.
/// Arguments: `(payload, from_cluster_id)`.
pub(crate) type PeerBroadcastFn = Arc<dyn Fn(&[u8], u16) + Send + Sync>;

/// Configuration for starting a peer session.
pub(crate) struct PeerSessionConfig {
    pub(crate) local_cluster_id: u16,
    pub(crate) db_path: std::path::PathBuf,
    pub(crate) max_levels: usize,
    pub(crate) io_backend: Arc<dyn IoBackend>,
    pub(crate) replay_fn: PeerReplayFn,
    pub(crate) broadcast_fn: PeerBroadcastFn,
}

/// A symmetric bidirectional peer session.
///
/// Each session has a reader thread that processes incoming messages and a
/// writer channel that accepts outgoing messages. Both sides are peers —
/// neither is primary nor replica.
pub(crate) struct PeerSession {
    writer_tx: mpsc::SyncSender<PeerMsg>,
    reader_handle: Option<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    remote_cluster_id: u16,
}

#[allow(dead_code)]
impl PeerSession {
    /// Start a peer session on an already-connected TCP stream.
    ///
    /// `is_connector` indicates whether this side initiated the connection
    /// (and should send a SyncRequest). The listener side responds with sync data.
    pub(crate) fn start(
        stream: TcpStream,
        config: &PeerSessionConfig,
        is_connector: bool,
        stop: Arc<AtomicBool>,
    ) -> Result<Self> {
        let local_cluster_id = config.local_cluster_id;
        let db_path = &config.db_path;
        let max_levels = config.max_levels;
        let io_backend = Arc::clone(&config.io_backend);
        let replay_fn = Arc::clone(&config.replay_fn);
        let broadcast_to_others = Arc::clone(&config.broadcast_fn);
        stream.set_nodelay(true)?;
        stream.set_nonblocking(false)?;

        let mut writer = BufWriter::new(stream.try_clone()?);
        let mut reader = BufReader::new(stream.try_clone()?);

        // Set read timeout for periodic stop-flag checks
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;

        // --- Handshake ---
        ReplMessage::write_handshake_header(&mut writer)?;
        ReplMessage::Handshake {
            cluster_id: local_cluster_id,
            role: Role::Peer,
        }
        .write_to(&mut writer)?;
        writer.flush()?;

        ReplMessage::read_handshake_header(&mut reader)?;
        let remote_cluster_id = match ReplMessage::read_from(&mut reader)? {
            Some(ReplMessage::Handshake {
                cluster_id, role, ..
            }) => {
                if role != Role::Peer {
                    return Err(Error::Corruption(format!(
                        "expected peer handshake, got {role}"
                    )));
                }
                cluster_id
            }
            other => {
                return Err(Error::Corruption(format!(
                    "expected handshake message, got {other:?}"
                )));
            }
        };

        // --- Initial sync ---
        if is_connector {
            // Connector sends SyncRequest, listener responds
            // For now, always request full sync (revision=0, force_full=false)
            // to get the peer's current state
            ReplMessage::SyncRequest {
                last_revision: 0,
                force_full: false,
            }
            .write_to(&mut writer)?;
            writer.flush()?;

            // Receive sync response
            let first_msg = match ReplMessage::read_from(&mut reader)? {
                Some(msg) => msg,
                None => {
                    return Err(Error::Corruption(
                        "unexpected EOF waiting for sync response".into(),
                    ));
                }
            };

            match first_msg {
                ReplMessage::FullSyncStart { .. } => {
                    // Receive full sync — apply records via replay_fn
                    Self::receive_full_sync_as_peer(
                        first_msg,
                        &mut reader,
                        db_path,
                        max_levels,
                        &stop,
                    )?;
                }
                ReplMessage::IncrementalSyncStart { record_count } => {
                    Self::receive_incremental_as_peer(
                        &mut reader,
                        record_count,
                        &replay_fn,
                        &stop,
                    )?;
                }
                other => {
                    return Err(Error::Corruption(format!(
                        "expected sync response, got {other:?}"
                    )));
                }
            }
        } else {
            // Listener side: wait for SyncRequest from connector, then respond
            let sync_req = Self::read_sync_request_with_timeout(&stream, &mut reader)?;

            match sync_req {
                Some(ReplMessage::SyncRequest { .. }) => {
                    // Send full sync from our data
                    Self::send_full_sync(&mut writer, db_path, max_levels, &io_backend, &stop)?;
                }
                _ => {
                    // No sync request (old peer?) — send empty full sync
                    Self::send_full_sync(&mut writer, db_path, max_levels, &io_backend, &stop)?;
                }
            }
        }

        // --- Bidirectional live streaming ---
        let (writer_tx, writer_rx) = mpsc::sync_channel::<PeerMsg>(4096);
        let stop_clone = Arc::clone(&stop);

        // Writer thread: sends outgoing messages to the peer
        let writer_handle = thread::spawn(move || {
            Self::writer_loop(&mut writer, writer_rx, &stop_clone);
        });

        // Reader thread: reads incoming messages from the peer
        let stop_clone2 = Arc::clone(&stop);
        let reader_handle = thread::spawn(move || {
            if let Err(e) = Self::reader_loop(
                &mut reader,
                &replay_fn,
                &broadcast_to_others,
                remote_cluster_id,
                &stop_clone2,
            ) {
                if !stop_clone2.load(Ordering::Relaxed) {
                    eprintln!("peer({remote_cluster_id}): reader error: {e}");
                }
            }
        });

        Ok(Self {
            writer_tx,
            reader_handle: Some(reader_handle),
            writer_handle: Some(writer_handle),
            stop,
            remote_cluster_id,
        })
    }

    /// Send an AOL record to this peer.
    pub(crate) fn send(&self, payload: &[u8]) -> bool {
        self.writer_tx
            .try_send(PeerMsg::Aol(payload.to_vec()))
            .is_ok()
    }

    /// Returns the remote peer's cluster ID.
    pub(crate) fn remote_cluster_id(&self) -> u16 {
        self.remote_cluster_id
    }

    /// Check if the session is still alive.
    pub(crate) fn is_alive(&self) -> bool {
        if let Some(ref h) = self.reader_handle {
            !h.is_finished()
        } else {
            false
        }
    }

    /// Stop the session and join threads.
    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Drop the sender to unblock the writer thread
        drop(self.writer_tx.clone());
        if let Some(h) = self.reader_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.writer_handle.take() {
            let _ = h.join();
        }
    }

    // --- Internal ---

    fn writer_loop<W: Write>(writer: &mut W, rx: mpsc::Receiver<PeerMsg>, stop: &Arc<AtomicBool>) {
        let mut heartbeat_tick = 0u32;
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }

            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(PeerMsg::Aol(payload)) => {
                    let msg = ReplMessage::AolRecord { payload };
                    if msg.write_to(writer).is_err() {
                        return;
                    }
                    // Drain pending
                    while let Ok(PeerMsg::Aol(payload)) = rx.try_recv() {
                        let msg = ReplMessage::AolRecord { payload };
                        if msg.write_to(writer).is_err() {
                            return;
                        }
                    }
                    if writer.flush().is_err() {
                        return;
                    }
                    heartbeat_tick = 0;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    heartbeat_tick += 1;
                    if heartbeat_tick >= 10 {
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let hb = ReplMessage::Heartbeat { timestamp_ms: ts };
                        if hb.write_to(writer).is_err() {
                            return;
                        }
                        if writer.flush().is_err() {
                            return;
                        }
                        heartbeat_tick = 0;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return;
                }
            }
        }
    }

    fn reader_loop<R: std::io::Read>(
        reader: &mut R,
        replay_fn: &PeerReplayFn,
        broadcast_to_others: &PeerBroadcastFn,
        remote_cluster_id: u16,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            let msg = match ReplMessage::read_from(reader) {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "peer closed connection",
                    )));
                }
                Err(Error::Io(ref e))
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            };

            match msg {
                ReplMessage::AolRecord { payload } => {
                    // Apply via LWW and forward to other peers if accepted
                    match replay_fn(&payload) {
                        Ok(true) => {
                            // Record was applied — forward to other peers
                            broadcast_to_others(&payload, remote_cluster_id);
                        }
                        Ok(false) => {
                            // Skipped (loop prevention or LWW conflict)
                        }
                        Err(e) => {
                            eprintln!("peer({remote_cluster_id}): replay error: {e}");
                        }
                    }
                }
                ReplMessage::Heartbeat { .. } => {
                    // Connection alive, nothing to do
                }
                ReplMessage::ErrorMsg { message } => {
                    return Err(Error::Corruption(format!("peer error: {message}")));
                }
                other => {
                    eprintln!("peer({remote_cluster_id}): ignoring unexpected message: {other:?}");
                }
            }
        }
    }

    /// Read a SyncRequest with a 2-second timeout.
    fn read_sync_request_with_timeout<R: std::io::Read>(
        stream: &TcpStream,
        reader: &mut R,
    ) -> Result<Option<ReplMessage>> {
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let result = ReplMessage::read_from(reader);
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;

        match result {
            Ok(Some(msg @ ReplMessage::SyncRequest { .. })) => Ok(Some(msg)),
            Ok(Some(other)) => Err(Error::Corruption(format!(
                "expected SyncRequest, got {other:?}"
            ))),
            Ok(None) => Ok(None),
            Err(Error::Io(ref e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn send_full_sync<W: Write>(
        writer: &mut W,
        db_path: &Path,
        max_levels: usize,
        io_backend: &Arc<dyn IoBackend>,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        let sst_files = super::repl_sender::enumerate_sst_files(db_path, max_levels);
        let obj_files = super::repl_sender::enumerate_object_files(db_path);

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

        for (namespace, level, sst_id, path) in &sst_files {
            if stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "stopped",
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

        for (namespace, hash, path) in &obj_files {
            if stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "stopped",
                )));
            }
            let data = std::fs::read(path)?;
            let mut hash_bytes = [0u8; 32];
            if let Ok(decoded) = super::repl_sender::hex_to_bytes(hash) {
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
        let _ = io_backend; // used in future for reading files
        Ok(())
    }

    fn receive_full_sync_as_peer<R: std::io::Read>(
        first_msg: ReplMessage,
        reader: &mut R,
        db_path: &Path,
        max_levels: usize,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        // Delegate to the existing receiver logic
        super::repl_receiver::ReplReceiver::receive_full_sync_from_msg(
            first_msg, reader, db_path, max_levels, stop,
        )
    }

    fn receive_incremental_as_peer<R: std::io::Read>(
        reader: &mut R,
        record_count: u32,
        replay_fn: &PeerReplayFn,
        stop: &Arc<AtomicBool>,
    ) -> Result<()> {
        for i in 0..record_count {
            if stop.load(Ordering::Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "stopped",
                )));
            }

            let msg = match ReplMessage::read_from(reader)? {
                Some(msg) => msg,
                None => {
                    return Err(Error::Corruption(format!(
                        "unexpected EOF during incremental sync at record {i}/{record_count}"
                    )));
                }
            };

            match msg {
                ReplMessage::AolRecord { payload } => {
                    let _ = replay_fn(&payload);
                }
                other => {
                    return Err(Error::Corruption(format!(
                        "unexpected message during incremental sync: {other:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn writer_loop_sends_aol_records() {
        let (tx, rx) = mpsc::sync_channel(16);
        let stop = Arc::new(AtomicBool::new(false));

        tx.send(PeerMsg::Aol(b"record-1".to_vec())).unwrap();
        tx.send(PeerMsg::Aol(b"record-2".to_vec())).unwrap();

        // Drop sender so writer_loop exits on Disconnected after draining
        drop(tx);

        let mut buf = Vec::new();
        PeerSession::writer_loop(&mut buf, rx, &stop);

        // Decode messages from buffer
        let mut cursor = Cursor::new(buf);
        let msg1 = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert!(matches!(msg1, ReplMessage::AolRecord { ref payload } if payload == b"record-1"));
        let msg2 = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert!(matches!(msg2, ReplMessage::AolRecord { ref payload } if payload == b"record-2"));
    }

    #[test]
    fn reader_loop_processes_aol_records() {
        let mut buf = Vec::new();
        ReplMessage::AolRecord {
            payload: b"peer-record".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        let replay_fn: PeerReplayFn = Arc::new(move |payload: &[u8]| {
            received_clone.lock().unwrap().push(payload.to_vec());
            Ok(true)
        });

        let forwarded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let forwarded_clone = Arc::clone(&forwarded);
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> =
            Arc::new(move |payload: &[u8], from: u16| {
                forwarded_clone
                    .lock()
                    .unwrap()
                    .push((payload.to_vec(), from));
            });

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // Will hit EOF → ConnectionReset
        let result = PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, 42, &stop);
        assert!(result.is_err());

        let records = received.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], b"peer-record");

        let fwd = forwarded.lock().unwrap();
        assert_eq!(fwd.len(), 1);
        assert_eq!(fwd[0].1, 42); // from cluster 42
    }

    #[test]
    fn reader_loop_skips_rejected_records() {
        let mut buf = Vec::new();
        ReplMessage::AolRecord {
            payload: b"old-record".to_vec(),
        }
        .write_to(&mut buf)
        .unwrap();

        let replay_fn: PeerReplayFn = Arc::new(|_| Ok(false)); // reject all

        let forwarded = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let forwarded_clone = Arc::clone(&forwarded);
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> =
            Arc::new(move |payload: &[u8], _| {
                forwarded_clone.lock().unwrap().push(payload.to_vec());
            });

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        let _ = PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, 99, &stop);

        // Should not forward rejected records
        let fwd = forwarded.lock().unwrap();
        assert!(fwd.is_empty());
    }

    #[test]
    fn reader_loop_handles_heartbeat() {
        let mut buf = Vec::new();
        ReplMessage::Heartbeat {
            timestamp_ms: 12345,
        }
        .write_to(&mut buf)
        .unwrap();

        let replay_fn: PeerReplayFn = Arc::new(|_| Ok(true));
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> = Arc::new(|_, _| {});

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // Should process heartbeat and then EOF
        let result = PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, 1, &stop);
        assert!(result.is_err()); // EOF
    }

    #[test]
    fn reader_loop_stops_on_flag() {
        let buf = Vec::new();
        let replay_fn: PeerReplayFn = Arc::new(|_| Ok(true));
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> = Arc::new(|_, _| {});

        let stop = Arc::new(AtomicBool::new(true)); // already stopped
        let mut cursor = Cursor::new(buf);

        let result = PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, 1, &stop);
        assert!(result.is_ok());
    }
}
