use std::fmt;
use std::io::{self, Read, Write};
use std::str::FromStr;

use super::checksum::Checksum;
use super::error::{Error, Result};

/// Node role in a replication topology.
#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    /// Standalone node — no replication (default).
    Standalone,
    /// Primary node — accepts writes and streams to replicas.
    Primary,
    /// Replica node — receives writes from primary, rejects local writes.
    Replica,
}

impl Default for Role {
    fn default() -> Self {
        Self::Standalone
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standalone => write!(f, "standalone"),
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
        }
    }
}

impl FromStr for Role {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "standalone" => Ok(Self::Standalone),
            "primary" => Ok(Self::Primary),
            "replica" => Ok(Self::Replica),
            _ => Err(Error::InvalidConfig(format!(
                "unknown role '{s}' (expected: standalone, primary, replica)"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire protocol message types
// ---------------------------------------------------------------------------

// Wire protocol constants and types are defined now but consumed by the
// replication sender/receiver in upcoming commits.
#[allow(dead_code)]
/// Message type tags for the replication wire protocol.
const MSG_HANDSHAKE: u8 = 0x01;
#[allow(dead_code)]
const MSG_FULL_SYNC_START: u8 = 0x02;
#[allow(dead_code)]
const MSG_SST_CHUNK: u8 = 0x03;
#[allow(dead_code)]
const MSG_OBJECT_CHUNK: u8 = 0x04;
#[allow(dead_code)]
const MSG_AOL_RECORD: u8 = 0x05;
#[allow(dead_code)]
const MSG_HEARTBEAT: u8 = 0x06;
#[allow(dead_code)]
const MSG_ACK: u8 = 0x07;
#[allow(dead_code)]
const MSG_FULL_SYNC_END: u8 = 0x08;
#[allow(dead_code)]
const MSG_ERROR: u8 = 0x09;
#[allow(dead_code)]
const MSG_DROP_NAMESPACE: u8 = 0x0A;
#[allow(dead_code)]
const MSG_SYNC_REQUEST: u8 = 0x0B;
#[allow(dead_code)]
const MSG_INCREMENTAL_SYNC_START: u8 = 0x0C;

/// Role tags for handshake messages.
#[allow(dead_code)]
const ROLE_PRIMARY: u8 = 0x01;
#[allow(dead_code)]
const ROLE_REPLICA: u8 = 0x02;

/// Magic bytes for the replication protocol: ASCII `rKVR`.
#[allow(dead_code)]
const REPL_MAGIC: [u8; 4] = [0x72, 0x4B, 0x56, 0x52];
/// Current replication protocol version.
#[allow(dead_code)]
const REPL_VERSION: u16 = 1;

/// A message in the replication wire protocol.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ReplMessage {
    /// Initial handshake — exchanged by both sides on connect.
    Handshake { cluster_id: u16, role: Role },
    /// Primary signals the start of a full sync.
    FullSyncStart {
        namespace_count: u32,
        sst_count: u32,
        object_count: u32,
    },
    /// A chunk of SSTable data during full sync.
    SstChunk {
        namespace: String,
        level: u8,
        sst_id: u64,
        data: Vec<u8>,
    },
    /// A chunk of bin-object data during full sync.
    ObjectChunk {
        namespace: String,
        hash: [u8; 32],
        data: Vec<u8>,
    },
    /// An AOL record streamed from primary to replica.
    AolRecord { payload: Vec<u8> },
    /// Heartbeat — sent periodically to keep the connection alive.
    Heartbeat { timestamp_ms: u64 },
    /// Acknowledgement from replica — confirms last processed revision.
    Ack { last_revision: u128 },
    /// Primary signals the end of a full sync.
    FullSyncEnd,
    /// Error message — signals a protocol-level error.
    ErrorMsg { message: String },
    /// Primary instructs replica to drop a namespace and all its data.
    DropNamespace { namespace: String },
    /// Replica requests sync from primary, optionally incremental.
    SyncRequest {
        last_revision: u128,
        force_full: bool,
    },
    /// Primary signals the start of an incremental sync.
    IncrementalSyncStart { record_count: u32 },
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

#[allow(dead_code)]
impl ReplMessage {
    /// Encode this message into the wire format and write it to `w`.
    ///
    /// Wire frame: `[msg_type: 1B][payload_len: 4B BE][payload][checksum: 5B]`
    pub(crate) fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        let (tag, payload) = self.encode_payload();
        let checksum = Checksum::compute(&payload);

        w.write_all(&[tag])?;
        w.write_all(&(payload.len() as u32).to_be_bytes())?;
        w.write_all(&payload)?;
        w.write_all(&checksum.to_bytes())?;
        Ok(())
    }

    /// Read one message from the wire format.
    ///
    /// Returns `Ok(None)` on clean EOF (zero bytes read for msg_type).
    pub(crate) fn read_from<R: Read>(r: &mut R) -> Result<Option<Self>> {
        // Read msg_type (1 byte)
        let mut tag_buf = [0u8; 1];
        match r.read_exact(&mut tag_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }
        let tag = tag_buf[0];

        // Read payload_len (4 bytes BE)
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let payload_len = u32::from_be_bytes(len_buf) as usize;

        // Read payload
        let mut payload = vec![0u8; payload_len];
        r.read_exact(&mut payload)?;

        // Read and verify checksum (5 bytes)
        let mut cksum_buf = [0u8; 5];
        r.read_exact(&mut cksum_buf)?;
        let checksum = Checksum::from_bytes(&cksum_buf)?;
        checksum.verify(&payload)?;

        Self::decode_payload(tag, &payload).map(Some)
    }

    /// Encode a handshake header (magic + version) for initial connection.
    pub(crate) fn write_handshake_header<W: Write>(w: &mut W) -> Result<()> {
        w.write_all(&REPL_MAGIC)?;
        w.write_all(&REPL_VERSION.to_be_bytes())?;
        Ok(())
    }

    /// Read and validate a handshake header.
    pub(crate) fn read_handshake_header<R: Read>(r: &mut R) -> Result<()> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if magic != REPL_MAGIC {
            return Err(Error::Corruption("replication magic mismatch".into()));
        }
        let mut ver_buf = [0u8; 2];
        r.read_exact(&mut ver_buf)?;
        let version = u16::from_be_bytes(ver_buf);
        if version != REPL_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported replication protocol version: {version}"
            )));
        }
        Ok(())
    }

    fn encode_payload(&self) -> (u8, Vec<u8>) {
        match self {
            ReplMessage::Handshake { cluster_id, role } => {
                let role_tag = match role {
                    Role::Primary => ROLE_PRIMARY,
                    Role::Replica => ROLE_REPLICA,
                    Role::Standalone => 0x00,
                };
                let mut buf = Vec::with_capacity(3);
                buf.extend_from_slice(&cluster_id.to_be_bytes());
                buf.push(role_tag);
                (MSG_HANDSHAKE, buf)
            }
            ReplMessage::FullSyncStart {
                namespace_count,
                sst_count,
                object_count,
            } => {
                let mut buf = Vec::with_capacity(12);
                buf.extend_from_slice(&namespace_count.to_be_bytes());
                buf.extend_from_slice(&sst_count.to_be_bytes());
                buf.extend_from_slice(&object_count.to_be_bytes());
                (MSG_FULL_SYNC_START, buf)
            }
            ReplMessage::SstChunk {
                namespace,
                level,
                sst_id,
                data,
            } => {
                let ns_bytes = namespace.as_bytes();
                let mut buf = Vec::with_capacity(2 + ns_bytes.len() + 1 + 8 + data.len());
                buf.extend_from_slice(&(ns_bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(ns_bytes);
                buf.push(*level);
                buf.extend_from_slice(&sst_id.to_be_bytes());
                buf.extend_from_slice(data);
                (MSG_SST_CHUNK, buf)
            }
            ReplMessage::ObjectChunk {
                namespace,
                hash,
                data,
            } => {
                let ns_bytes = namespace.as_bytes();
                let mut buf = Vec::with_capacity(2 + ns_bytes.len() + 32 + data.len());
                buf.extend_from_slice(&(ns_bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(ns_bytes);
                buf.extend_from_slice(hash);
                buf.extend_from_slice(data);
                (MSG_OBJECT_CHUNK, buf)
            }
            ReplMessage::AolRecord { payload } => (MSG_AOL_RECORD, payload.clone()),
            ReplMessage::Heartbeat { timestamp_ms } => {
                (MSG_HEARTBEAT, timestamp_ms.to_be_bytes().to_vec())
            }
            ReplMessage::Ack { last_revision } => (MSG_ACK, last_revision.to_be_bytes().to_vec()),
            ReplMessage::FullSyncEnd => (MSG_FULL_SYNC_END, Vec::new()),
            ReplMessage::ErrorMsg { message } => {
                let bytes = message.as_bytes();
                let mut buf = Vec::with_capacity(2 + bytes.len());
                buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(bytes);
                (MSG_ERROR, buf)
            }
            ReplMessage::DropNamespace { namespace } => {
                let ns_bytes = namespace.as_bytes();
                let mut buf = Vec::with_capacity(2 + ns_bytes.len());
                buf.extend_from_slice(&(ns_bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(ns_bytes);
                (MSG_DROP_NAMESPACE, buf)
            }
            ReplMessage::SyncRequest {
                last_revision,
                force_full,
            } => {
                let mut buf = Vec::with_capacity(17);
                buf.extend_from_slice(&last_revision.to_be_bytes());
                buf.push(u8::from(*force_full));
                (MSG_SYNC_REQUEST, buf)
            }
            ReplMessage::IncrementalSyncStart { record_count } => (
                MSG_INCREMENTAL_SYNC_START,
                record_count.to_be_bytes().to_vec(),
            ),
        }
    }

    fn decode_payload(tag: u8, data: &[u8]) -> Result<Self> {
        match tag {
            MSG_HANDSHAKE => {
                if data.len() < 3 {
                    return Err(Error::Corruption("truncated handshake".into()));
                }
                let cluster_id = u16::from_be_bytes([data[0], data[1]]);
                let role = match data[2] {
                    ROLE_PRIMARY => Role::Primary,
                    ROLE_REPLICA => Role::Replica,
                    0x00 => Role::Standalone,
                    other => {
                        return Err(Error::Corruption(format!(
                            "unknown role tag: 0x{other:02x}"
                        )))
                    }
                };
                Ok(ReplMessage::Handshake { cluster_id, role })
            }
            MSG_FULL_SYNC_START => {
                if data.len() < 12 {
                    return Err(Error::Corruption("truncated full_sync_start".into()));
                }
                let namespace_count = u32::from_be_bytes(data[0..4].try_into().unwrap());
                let sst_count = u32::from_be_bytes(data[4..8].try_into().unwrap());
                let object_count = u32::from_be_bytes(data[8..12].try_into().unwrap());
                Ok(ReplMessage::FullSyncStart {
                    namespace_count,
                    sst_count,
                    object_count,
                })
            }
            MSG_SST_CHUNK => {
                if data.len() < 2 {
                    return Err(Error::Corruption("truncated sst_chunk ns_len".into()));
                }
                let ns_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                let mut pos = 2;
                if pos + ns_len + 1 + 8 > data.len() {
                    return Err(Error::Corruption("truncated sst_chunk".into()));
                }
                let namespace = std::str::from_utf8(&data[pos..pos + ns_len])
                    .map_err(|e| Error::Corruption(format!("invalid namespace utf-8: {e}")))?
                    .to_owned();
                pos += ns_len;
                let level = data[pos];
                pos += 1;
                let sst_id = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let chunk_data = data[pos..].to_vec();
                Ok(ReplMessage::SstChunk {
                    namespace,
                    level,
                    sst_id,
                    data: chunk_data,
                })
            }
            MSG_OBJECT_CHUNK => {
                if data.len() < 2 {
                    return Err(Error::Corruption("truncated object_chunk ns_len".into()));
                }
                let ns_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                let mut pos = 2;
                if pos + ns_len + 32 > data.len() {
                    return Err(Error::Corruption("truncated object_chunk".into()));
                }
                let namespace = std::str::from_utf8(&data[pos..pos + ns_len])
                    .map_err(|e| Error::Corruption(format!("invalid namespace utf-8: {e}")))?
                    .to_owned();
                pos += ns_len;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&data[pos..pos + 32]);
                pos += 32;
                let chunk_data = data[pos..].to_vec();
                Ok(ReplMessage::ObjectChunk {
                    namespace,
                    hash,
                    data: chunk_data,
                })
            }
            MSG_AOL_RECORD => Ok(ReplMessage::AolRecord {
                payload: data.to_vec(),
            }),
            MSG_HEARTBEAT => {
                if data.len() < 8 {
                    return Err(Error::Corruption("truncated heartbeat".into()));
                }
                let timestamp_ms = u64::from_be_bytes(data[0..8].try_into().unwrap());
                Ok(ReplMessage::Heartbeat { timestamp_ms })
            }
            MSG_ACK => {
                if data.len() < 16 {
                    return Err(Error::Corruption("truncated ack".into()));
                }
                let last_revision = u128::from_be_bytes(data[0..16].try_into().unwrap());
                Ok(ReplMessage::Ack { last_revision })
            }
            MSG_FULL_SYNC_END => Ok(ReplMessage::FullSyncEnd),
            MSG_ERROR => {
                if data.len() < 2 {
                    return Err(Error::Corruption("truncated error msg".into()));
                }
                let msg_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                if 2 + msg_len > data.len() {
                    return Err(Error::Corruption("truncated error msg body".into()));
                }
                let message = std::str::from_utf8(&data[2..2 + msg_len])
                    .map_err(|e| Error::Corruption(format!("invalid error msg utf-8: {e}")))?
                    .to_owned();
                Ok(ReplMessage::ErrorMsg { message })
            }
            MSG_DROP_NAMESPACE => {
                if data.len() < 2 {
                    return Err(Error::Corruption("truncated drop_namespace ns_len".into()));
                }
                let ns_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                if 2 + ns_len > data.len() {
                    return Err(Error::Corruption("truncated drop_namespace body".into()));
                }
                let namespace = std::str::from_utf8(&data[2..2 + ns_len])
                    .map_err(|e| Error::Corruption(format!("invalid namespace utf-8: {e}")))?
                    .to_owned();
                Ok(ReplMessage::DropNamespace { namespace })
            }
            MSG_SYNC_REQUEST => {
                if data.len() < 17 {
                    return Err(Error::Corruption("truncated sync_request".into()));
                }
                let last_revision = u128::from_be_bytes(data[0..16].try_into().unwrap());
                let force_full = data[16] != 0;
                Ok(ReplMessage::SyncRequest {
                    last_revision,
                    force_full,
                })
            }
            MSG_INCREMENTAL_SYNC_START => {
                if data.len() < 4 {
                    return Err(Error::Corruption("truncated incremental_sync_start".into()));
                }
                let record_count = u32::from_be_bytes(data[0..4].try_into().unwrap());
                Ok(ReplMessage::IncrementalSyncStart { record_count })
            }
            _ => Err(Error::Corruption(format!(
                "unknown replication message type: 0x{tag:02x}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(msg: &ReplMessage) -> ReplMessage {
        let mut buf = Vec::new();
        msg.write_to(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        ReplMessage::read_from(&mut cursor).unwrap().unwrap()
    }

    // --- Role ---

    #[test]
    fn role_display() {
        assert_eq!(Role::Standalone.to_string(), "standalone");
        assert_eq!(Role::Primary.to_string(), "primary");
        assert_eq!(Role::Replica.to_string(), "replica");
    }

    #[test]
    fn role_from_str() {
        assert_eq!("standalone".parse::<Role>().unwrap(), Role::Standalone);
        assert_eq!("primary".parse::<Role>().unwrap(), Role::Primary);
        assert_eq!("replica".parse::<Role>().unwrap(), Role::Replica);
        assert!("invalid".parse::<Role>().is_err());
    }

    #[test]
    fn role_default() {
        assert_eq!(Role::default(), Role::Standalone);
    }

    // --- Handshake header ---

    #[test]
    fn handshake_header_roundtrip() {
        let mut buf = Vec::new();
        ReplMessage::write_handshake_header(&mut buf).unwrap();
        assert_eq!(buf.len(), 6);
        let mut cursor = Cursor::new(buf);
        ReplMessage::read_handshake_header(&mut cursor).unwrap();
    }

    #[test]
    fn handshake_header_bad_magic() {
        let buf = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let mut cursor = Cursor::new(buf);
        assert!(ReplMessage::read_handshake_header(&mut cursor).is_err());
    }

    #[test]
    fn handshake_header_bad_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&REPL_MAGIC);
        buf.extend_from_slice(&99u16.to_be_bytes());
        let mut cursor = Cursor::new(buf);
        assert!(ReplMessage::read_handshake_header(&mut cursor).is_err());
    }

    // --- Message roundtrips ---

    #[test]
    fn roundtrip_handshake() {
        let msg = ReplMessage::Handshake {
            cluster_id: 0xABCD,
            role: Role::Primary,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_handshake_replica() {
        let msg = ReplMessage::Handshake {
            cluster_id: 42,
            role: Role::Replica,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_full_sync_start() {
        let msg = ReplMessage::FullSyncStart {
            namespace_count: 3,
            sst_count: 10,
            object_count: 5,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_sst_chunk() {
        let msg = ReplMessage::SstChunk {
            namespace: "myns".to_string(),
            level: 2,
            sst_id: 12345,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_object_chunk() {
        let msg = ReplMessage::ObjectChunk {
            namespace: "ns1".to_string(),
            hash: [0xAA; 32],
            data: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_aol_record() {
        let msg = ReplMessage::AolRecord {
            payload: vec![10, 20, 30, 40, 50],
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_heartbeat() {
        let msg = ReplMessage::Heartbeat {
            timestamp_ms: 1_700_000_000_000,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_ack() {
        let msg = ReplMessage::Ack {
            last_revision: 0xDEAD_BEEF_CAFE_1234_5678_9ABC_DEF0_1234,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_full_sync_end() {
        let msg = ReplMessage::FullSyncEnd;
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_error_msg() {
        let msg = ReplMessage::ErrorMsg {
            message: "something went wrong".to_string(),
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_drop_namespace() {
        let msg = ReplMessage::DropNamespace {
            namespace: "myns".to_string(),
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_drop_namespace_default() {
        let msg = ReplMessage::DropNamespace {
            namespace: "_".to_string(),
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_sync_request_incremental() {
        let msg = ReplMessage::SyncRequest {
            last_revision: 0xDEAD_BEEF_CAFE_1234_5678_9ABC_DEF0_1234,
            force_full: false,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_sync_request_force_full() {
        let msg = ReplMessage::SyncRequest {
            last_revision: 0,
            force_full: true,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_incremental_sync_start() {
        let msg = ReplMessage::IncrementalSyncStart { record_count: 42 };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_incremental_sync_start_zero() {
        let msg = ReplMessage::IncrementalSyncStart { record_count: 0 };
        assert_eq!(roundtrip(&msg), msg);
    }

    // --- EOF handling ---

    #[test]
    fn read_from_empty_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        assert!(ReplMessage::read_from(&mut cursor).unwrap().is_none());
    }

    // --- Corruption detection ---

    #[test]
    fn corrupted_checksum_detected() {
        let msg = ReplMessage::Heartbeat {
            timestamp_ms: 12345,
        };
        let mut buf = Vec::new();
        msg.write_to(&mut buf).unwrap();

        // Corrupt a byte in the payload
        buf[6] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        assert!(ReplMessage::read_from(&mut cursor).is_err());
    }

    #[test]
    fn unknown_msg_type_detected() {
        // Build a valid frame with unknown tag 0xFF
        let payload = vec![0u8; 4];
        let checksum = Checksum::compute(&payload);
        let mut buf = Vec::new();
        buf.push(0xFF); // unknown tag
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&checksum.to_bytes());

        let mut cursor = Cursor::new(buf);
        assert!(ReplMessage::read_from(&mut cursor).is_err());
    }

    // --- Multiple messages in sequence ---

    #[test]
    fn multiple_messages_in_stream() {
        let messages = vec![
            ReplMessage::Handshake {
                cluster_id: 1,
                role: Role::Primary,
            },
            ReplMessage::FullSyncStart {
                namespace_count: 1,
                sst_count: 2,
                object_count: 0,
            },
            ReplMessage::FullSyncEnd,
            ReplMessage::AolRecord {
                payload: vec![1, 2, 3],
            },
            ReplMessage::Heartbeat {
                timestamp_ms: 99999,
            },
        ];

        let mut buf = Vec::new();
        for msg in &messages {
            msg.write_to(&mut buf).unwrap();
        }

        let mut cursor = Cursor::new(buf);
        for expected in &messages {
            let decoded = ReplMessage::read_from(&mut cursor).unwrap().unwrap();
            assert_eq!(&decoded, expected);
        }
        // Should return None at EOF
        assert!(ReplMessage::read_from(&mut cursor).unwrap().is_none());
    }

    // --- Empty payloads ---

    #[test]
    fn roundtrip_sst_chunk_empty_data() {
        let msg = ReplMessage::SstChunk {
            namespace: "_".to_string(),
            level: 0,
            sst_id: 1,
            data: vec![],
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_object_chunk_empty_data() {
        let msg = ReplMessage::ObjectChunk {
            namespace: "_".to_string(),
            hash: [0u8; 32],
            data: vec![],
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_aol_record_empty() {
        let msg = ReplMessage::AolRecord { payload: vec![] };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_error_msg_empty() {
        let msg = ReplMessage::ErrorMsg {
            message: String::new(),
        };
        assert_eq!(roundtrip(&msg), msg);
    }
}
