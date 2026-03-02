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

/// Callback invoked after a full sync completes — reloads SSTable index,
/// resets memtables, truncates AOL, and registers new namespaces.
pub(crate) type PeerPostSyncFn = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// Callback to flush the AOL buffer before reading it for incremental sync.
pub(crate) type PeerFlushFn = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// Messages sent between peer sessions.
#[derive(Debug)]
pub(crate) enum PeerMsg {
    /// Raw AOL record payload to forward to the peer.
    Aol(Vec<u8>),
    /// Instruct the peer to drop a namespace.
    DropNamespace(String),
}

/// Callback signature for replaying a peer record with LWW resolution.
/// Returns `true` if the record was applied (newer), `false` if skipped.
pub(crate) type PeerReplayFn = Arc<dyn Fn(&[u8]) -> Result<bool> + Send + Sync>;

/// Callback to broadcast an accepted record to other peer sessions.
/// Arguments: `(payload, from_cluster_id)`.
pub(crate) type PeerBroadcastFn = Arc<dyn Fn(&[u8], u16) + Send + Sync>;

/// Callback invoked when a peer sends a DropNamespace message.
pub(crate) type PeerDropNsFn = Arc<dyn Fn(&str) -> Result<()> + Send + Sync>;

/// Configuration for starting a peer session.
pub(crate) struct PeerSessionConfig {
    pub(crate) local_cluster_id: u16,
    pub(crate) db_path: std::path::PathBuf,
    pub(crate) max_levels: usize,
    pub(crate) io_backend: Arc<dyn IoBackend>,
    pub(crate) replay_fn: PeerReplayFn,
    pub(crate) broadcast_fn: PeerBroadcastFn,
    pub(crate) post_sync_fn: PeerPostSyncFn,
    pub(crate) last_revision: Arc<std::sync::Mutex<u128>>,
    pub(crate) flush_fn: PeerFlushFn,
    pub(crate) drop_ns_fn: PeerDropNsFn,
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
        let post_sync_fn = Arc::clone(&config.post_sync_fn);
        let last_revision_tracker = Arc::clone(&config.last_revision);

        if is_connector {
            // Read current last_revision for incremental sync
            let current_rev = {
                let lr = last_revision_tracker
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *lr
            };

            ReplMessage::SyncRequest {
                last_revision: current_rev,
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
                ReplMessage::FullSyncStart {
                    sst_count,
                    object_count,
                    ..
                } => {
                    let has_data = sst_count > 0 || object_count > 0;
                    // Receive full sync — SST files written to disk
                    Self::receive_full_sync_as_peer(
                        ReplMessage::FullSyncStart {
                            namespace_count: 0, // not used by receiver
                            sst_count,
                            object_count,
                        },
                        &mut reader,
                        db_path,
                        max_levels,
                        &stop,
                    )?;
                    // Only reload when the peer actually sent SST/object data.
                    // An empty full sync means "I have nothing" — no reason to
                    // wipe local state.
                    if has_data {
                        // Reset revision after full sync (SSTs replace everything)
                        {
                            let mut lr = last_revision_tracker
                                .lock()
                                .unwrap_or_else(|e| e.into_inner());
                            *lr = 0;
                        }
                        // Reload SST index, reset memtables, truncate AOL
                        (post_sync_fn)()?;
                    }
                }
                ReplMessage::IncrementalSyncStart { record_count } => {
                    // Incremental sync — replay_fn already updates memtable + AOL
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
                Some(ReplMessage::SyncRequest {
                    last_revision,
                    force_full,
                }) if !force_full && last_revision > 0 => {
                    // Flush AOL buffer so records_after_revision sees latest data
                    let _ = (config.flush_fn)();
                    // Try incremental sync
                    let records = super::aol::records_after_revision(
                        db_path,
                        last_revision,
                        io_backend.as_ref(),
                    );
                    if records.is_empty() {
                        // AOL truncated or no matching records → full sync
                        Self::send_full_sync(&mut writer, db_path, max_levels, &io_backend, &stop)?;
                    } else {
                        Self::send_incremental_sync(&mut writer, &records)?;
                    }
                }
                _ => {
                    // force_full, revision=0, or no SyncRequest → full sync
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
        let drop_ns_fn = Arc::clone(&config.drop_ns_fn);
        let reader_handle = thread::spawn(move || {
            if let Err(e) = Self::reader_loop(
                &mut reader,
                &replay_fn,
                &broadcast_to_others,
                &drop_ns_fn,
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

    /// Send a drop-namespace command to this peer.
    pub(crate) fn send_drop_namespace(&self, namespace: &str) -> bool {
        self.writer_tx
            .try_send(PeerMsg::DropNamespace(namespace.to_owned()))
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
                Ok(peer_msg) => {
                    if Self::write_peer_msg(writer, peer_msg).is_err() {
                        return;
                    }
                    // Drain pending
                    while let Ok(queued) = rx.try_recv() {
                        if Self::write_peer_msg(writer, queued).is_err() {
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

    fn write_peer_msg<W: Write>(writer: &mut W, msg: PeerMsg) -> Result<()> {
        match msg {
            PeerMsg::Aol(payload) => {
                ReplMessage::AolRecord { payload }.write_to(writer)?;
            }
            PeerMsg::DropNamespace(namespace) => {
                ReplMessage::DropNamespace { namespace }.write_to(writer)?;
            }
        }
        Ok(())
    }

    fn reader_loop<R: std::io::Read>(
        reader: &mut R,
        replay_fn: &PeerReplayFn,
        broadcast_to_others: &PeerBroadcastFn,
        drop_ns_fn: &PeerDropNsFn,
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
                ReplMessage::DropNamespace { namespace } => {
                    if let Err(e) = drop_ns_fn(&namespace) {
                        eprintln!("peer({remote_cluster_id}): drop namespace error: {e}");
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

    fn send_incremental_sync<W: Write>(writer: &mut W, records: &[Vec<u8>]) -> Result<()> {
        ReplMessage::IncrementalSyncStart {
            record_count: records.len() as u32,
        }
        .write_to(writer)?;
        writer.flush()?;

        for payload in records {
            ReplMessage::AolRecord {
                payload: payload.clone(),
            }
            .write_to(writer)?;
        }
        writer.flush()?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PeerListener — accepts inbound peer connections
// ---------------------------------------------------------------------------

/// Listens for inbound peer connections and spawns a PeerSession for each.
#[allow(dead_code)]
pub(crate) struct PeerListener {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl PeerListener {
    pub(crate) fn start(
        bind: &str,
        port: u16,
        config: Arc<PeerSessionConfig>,
        sessions: Arc<std::sync::Mutex<Vec<PeerSession>>>,
        stop: Arc<AtomicBool>,
    ) -> Result<Self> {
        let addr = format!("{bind}:{port}");
        let listener = std::net::TcpListener::bind(&addr)?;
        listener.set_nonblocking(true)?;
        let stop_clone = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            Self::listen_loop(listener, &config, &sessions, &stop_clone);
        });

        Ok(Self {
            handle: Some(handle),
            stop,
        })
    }

    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    fn listen_loop(
        listener: std::net::TcpListener,
        config: &PeerSessionConfig,
        sessions: &std::sync::Mutex<Vec<PeerSession>>,
        stop: &Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    let session_stop = Arc::new(AtomicBool::new(false));
                    match PeerSession::start(stream, config, false, session_stop) {
                        Ok(session) => {
                            let cid = session.remote_cluster_id();
                            eprintln!(
                                "peer: accepted inbound connection from {addr} (cluster {cid})"
                            );
                            let mut sessions = sessions.lock().unwrap_or_else(|e| e.into_inner());
                            // Remove dead sessions
                            sessions.retain(|s| s.is_alive());
                            sessions.push(session);
                        }
                        Err(e) => {
                            eprintln!("peer: inbound connection from {addr} failed: {e}");
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    eprintln!("peer: accept error: {e}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PeerConnector — dials outbound peer connections with reconnect
// ---------------------------------------------------------------------------

/// Connects to a configured peer address and maintains the connection
/// with exponential backoff on failure.
#[allow(dead_code)]
pub(crate) struct PeerConnector {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl PeerConnector {
    pub(crate) fn start(
        peer_addr: String,
        config: Arc<PeerSessionConfig>,
        sessions: Arc<std::sync::Mutex<Vec<PeerSession>>>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let stop_clone = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            Self::connect_loop(&peer_addr, &config, &sessions, &stop_clone);
        });

        Self {
            handle: Some(handle),
            stop,
        }
    }

    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    fn connect_loop(
        addr: &str,
        config: &PeerSessionConfig,
        sessions: &std::sync::Mutex<Vec<PeerSession>>,
        stop: &Arc<AtomicBool>,
    ) {
        // Load checkpoint from previous run
        if let Some(rev) = load_peer_checkpoint(&config.db_path) {
            let mut lr = config
                .last_revision
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if rev > *lr {
                *lr = rev;
            }
        }

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        while !stop.load(Ordering::Relaxed) {
            match TcpStream::connect(addr) {
                Ok(stream) => {
                    let session_stop = Arc::new(AtomicBool::new(false));
                    match PeerSession::start(stream, config, true, session_stop) {
                        Ok(session) => {
                            let cid = session.remote_cluster_id();
                            eprintln!("peer: connected to {addr} (cluster {cid})");
                            backoff = Duration::from_secs(1);

                            {
                                let mut sessions =
                                    sessions.lock().unwrap_or_else(|e| e.into_inner());
                                sessions.retain(|s| s.is_alive());
                                sessions.push(session);
                            }

                            // Wait for the session to die before reconnecting
                            loop {
                                if stop.load(Ordering::Relaxed) {
                                    // Clean exit — save checkpoint
                                    let rev = *config
                                        .last_revision
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner());
                                    save_peer_checkpoint(&config.db_path, rev);
                                    return;
                                }
                                thread::sleep(Duration::from_secs(1));

                                let sessions = sessions.lock().unwrap_or_else(|e| e.into_inner());
                                let still_alive = sessions
                                    .iter()
                                    .any(|s| s.remote_cluster_id() == cid && s.is_alive());
                                if !still_alive {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("peer: handshake with {addr} failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("peer: connect to {addr} failed: {e}");
                }
            }

            // Save checkpoint on session death before backoff
            {
                let rev = *config
                    .last_revision
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                save_peer_checkpoint(&config.db_path, rev);
            }

            // Backoff before reconnecting
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

// ---------------------------------------------------------------------------
// Peer checkpoint persistence
// ---------------------------------------------------------------------------

const PEER_CHECKPOINT_FILE: &str = "peer_checkpoint";

/// Load the peer last-revision checkpoint from disk. Returns `None` if the
/// file doesn't exist or is malformed.
pub(crate) fn load_peer_checkpoint(db_path: &Path) -> Option<u128> {
    let path = db_path.join(PEER_CHECKPOINT_FILE);
    let data = std::fs::read(&path).ok()?;
    if data.len() < 16 {
        return None;
    }
    Some(u128::from_be_bytes(data[0..16].try_into().ok()?))
}

/// Persist the peer last-revision checkpoint to disk. Best-effort — errors
/// are logged but not propagated.
pub(crate) fn save_peer_checkpoint(db_path: &Path, revision: u128) {
    let path = db_path.join(PEER_CHECKPOINT_FILE);
    if let Err(e) = std::fs::write(&path, revision.to_be_bytes()) {
        eprintln!("peer: failed to save checkpoint: {e}");
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

        let drop_ns_fn: PeerDropNsFn = Arc::new(|_| Ok(()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // Will hit EOF → ConnectionReset
        let result =
            PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, &drop_ns_fn, 42, &stop);
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

        let drop_ns_fn: PeerDropNsFn = Arc::new(|_| Ok(()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        let _ =
            PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, &drop_ns_fn, 99, &stop);

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

        let drop_ns_fn: PeerDropNsFn = Arc::new(|_| Ok(()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // Should process heartbeat and then EOF
        let result =
            PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, &drop_ns_fn, 1, &stop);
        assert!(result.is_err()); // EOF
    }

    #[test]
    fn reader_loop_stops_on_flag() {
        let buf = Vec::new();
        let replay_fn: PeerReplayFn = Arc::new(|_| Ok(true));
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> = Arc::new(|_, _| {});

        let drop_ns_fn: PeerDropNsFn = Arc::new(|_| Ok(()));
        let stop = Arc::new(AtomicBool::new(true)); // already stopped
        let mut cursor = Cursor::new(buf);

        let result =
            PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, &drop_ns_fn, 1, &stop);
        assert!(result.is_ok());
    }

    #[test]
    fn writer_loop_sends_drop_namespace() {
        let (tx, rx) = mpsc::sync_channel(16);
        let stop = Arc::new(AtomicBool::new(false));

        tx.send(PeerMsg::DropNamespace("myns".to_owned())).unwrap();
        drop(tx);

        let mut buf = Vec::new();
        PeerSession::writer_loop(&mut buf, rx, &stop);

        let mut cursor = Cursor::new(buf);
        let msg = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
        assert!(matches!(msg, ReplMessage::DropNamespace { ref namespace } if namespace == "myns"));
    }

    #[test]
    fn reader_loop_handles_drop_namespace() {
        let mut buf = Vec::new();
        ReplMessage::DropNamespace {
            namespace: "test_ns".to_owned(),
        }
        .write_to(&mut buf)
        .unwrap();

        let replay_fn: PeerReplayFn = Arc::new(|_| Ok(true));
        let broadcast: Arc<dyn Fn(&[u8], u16) + Send + Sync> = Arc::new(|_, _| {});

        let dropped = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let dropped_clone = Arc::clone(&dropped);
        let drop_ns_fn: PeerDropNsFn = Arc::new(move |ns: &str| {
            dropped_clone.lock().unwrap().push(ns.to_owned());
            Ok(())
        });

        let stop = Arc::new(AtomicBool::new(false));
        let mut cursor = Cursor::new(buf);

        // Will hit EOF after processing
        let _ =
            PeerSession::reader_loop(&mut cursor, &replay_fn, &broadcast, &drop_ns_fn, 7, &stop);

        let ns_list = dropped.lock().unwrap();
        assert_eq!(ns_list.len(), 1);
        assert_eq!(ns_list[0], "test_ns");
    }
}
