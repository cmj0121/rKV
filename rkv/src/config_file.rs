use std::fmt;
use std::path::{Path, PathBuf};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{Compression, Config, IoModel, Role};

/// A size value that can be deserialized from either an integer (bytes) or a
/// human-readable string like `"4mb"`, `"1kb"`, `"2gb"`.
#[derive(Clone, Debug, PartialEq)]
pub struct Size(pub usize);

impl Serialize for Size {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0 as u64)
    }
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SizeVisitor;

        impl<'de> de::Visitor<'de> for SizeVisitor {
            type Value = Size;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte count (integer) or size string like \"4mb\"")
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Size, E> {
                Ok(Size(v as usize))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Size, E> {
                if v < 0 {
                    return Err(E::custom("size cannot be negative"));
                }
                Ok(Size(v as usize))
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Size, E> {
                parse_size(s).map(Size).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

/// Parse a human-readable size string into bytes.
pub fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    // Try plain integer first
    if let Ok(n) = s.parse::<usize>() {
        return Ok(n);
    }

    let lower = s.to_ascii_lowercase();
    let (num_part, multiplier) = if let Some(n) = lower.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('b') {
        (n, 1)
    } else {
        return Err(format!("invalid size: {s}"));
    };

    let num_part = num_part.trim();
    if let Ok(n) = num_part.parse::<usize>() {
        return Ok(n * multiplier);
    }
    let num: f64 = num_part.parse().map_err(|_| format!("invalid size: {s}"))?;
    if num < 0.0 || !num.is_finite() {
        return Err(format!("invalid size: {s}"));
    }
    Ok((num * multiplier as f64) as usize)
}

/// Format a byte count as a human-readable string for template output.
pub fn format_size(bytes: usize) -> String {
    if bytes == 0 {
        return "0".to_string();
    }
    if bytes.is_multiple_of(1024 * 1024 * 1024) {
        format!("{}gb", bytes / (1024 * 1024 * 1024))
    } else if bytes.is_multiple_of(1024 * 1024) {
        format!("{}mb", bytes / (1024 * 1024))
    } else if bytes.is_multiple_of(1024) {
        format!("{}kb", bytes / 1024)
    } else {
        bytes.to_string()
    }
}

// ---------------------------------------------------------------------------
// Serde helpers for enums
// ---------------------------------------------------------------------------

fn serialize_compression<S: Serializer>(c: &Compression, s: S) -> Result<S::Ok, S::Error> {
    let label = match c {
        Compression::None => "none",
        Compression::LZ4 => "lz4",
        Compression::Zstd => "zstd",
    };
    s.serialize_str(label)
}

fn deserialize_compression<'de, D: Deserializer<'de>>(d: D) -> Result<Compression, D::Error> {
    let s = String::deserialize(d)?;
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(Compression::None),
        "lz4" => Ok(Compression::LZ4),
        "zstd" | "zstandard" => Ok(Compression::Zstd),
        _ => Err(de::Error::custom(format!(
            "unknown compression: {s} (expected: none, lz4, zstd)"
        ))),
    }
}

fn serialize_io_model<S: Serializer>(m: &IoModel, s: S) -> Result<S::Ok, S::Error> {
    let label = match m {
        IoModel::None => "none",
        IoModel::DirectIO => "directio",
        IoModel::Mmap => "mmap",
    };
    s.serialize_str(label)
}

fn deserialize_io_model<'de, D: Deserializer<'de>>(d: D) -> Result<IoModel, D::Error> {
    let s = String::deserialize(d)?;
    match s.to_ascii_lowercase().as_str() {
        "none" | "buffered" => Ok(IoModel::None),
        "directio" | "direct" => Ok(IoModel::DirectIO),
        "mmap" => Ok(IoModel::Mmap),
        _ => Err(de::Error::custom(format!(
            "unknown io_model: {s} (expected: none, directio, mmap)"
        ))),
    }
}

fn serialize_role<S: Serializer>(r: &Role, s: S) -> Result<S::Ok, S::Error> {
    let label = match r {
        Role::Standalone => "standalone",
        Role::Primary => "primary",
        Role::Replica => "replica",
        Role::Peer => "peer",
    };
    s.serialize_str(label)
}

fn deserialize_role<'de, D: Deserializer<'de>>(d: D) -> Result<Role, D::Error> {
    let s = String::deserialize(d)?;
    s.parse::<Role>().map_err(de::Error::custom)
}

// ---------------------------------------------------------------------------
// FileConfig — the top-level config file structure
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub replication: ReplicationSection,
    #[serde(default)]
    pub cluster: ClusterSection,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageSection {
    pub path: Option<PathBuf>,
    pub create_if_missing: bool,
    pub write_buffer_size: Size,
    pub max_levels: usize,
    pub block_size: Size,
    pub cache_size: Size,
    pub object_size: Size,
    pub compress: bool,
    pub bloom_bits: usize,
    pub bloom_prefix_len: usize,
    pub verify_checksums: bool,
    #[serde(
        serialize_with = "serialize_compression",
        deserialize_with = "deserialize_compression"
    )]
    pub compression: Compression,
    #[serde(
        serialize_with = "serialize_io_model",
        deserialize_with = "deserialize_io_model"
    )]
    pub io_model: IoModel,
    pub aol_buffer_size: usize,
    pub l0_max_count: usize,
    pub l0_max_size: Size,
    pub l1_max_size: Size,
    pub default_max_size: Size,
    pub write_stall_size: Size,
}

impl Default for StorageSection {
    fn default() -> Self {
        let c = Config::new("/tmp/default");
        Self {
            path: None,
            create_if_missing: c.create_if_missing,
            write_buffer_size: Size(c.write_buffer_size),
            max_levels: c.max_levels,
            block_size: Size(c.block_size),
            cache_size: Size(c.cache_size),
            object_size: Size(c.object_size),
            compress: c.compress,
            bloom_bits: c.bloom_bits,
            bloom_prefix_len: c.bloom_prefix_len,
            verify_checksums: c.verify_checksums,
            compression: c.compression,
            io_model: c.io_model,
            aol_buffer_size: c.aol_buffer_size,
            l0_max_count: c.l0_max_count,
            l0_max_size: Size(c.l0_max_size),
            l1_max_size: Size(c.l1_max_size),
            default_max_size: Size(c.default_max_size),
            write_stall_size: Size(c.write_stall_size),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSection {
    pub bind: String,
    pub port: u16,
    pub body_limit: Size,
    pub timeout: u64,
    pub ui: bool,
    pub allow_ips: Vec<String>,
    pub allow_all: bool,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_owned(),
            port: 8321,
            body_limit: Size(2 * 1024 * 1024),
            timeout: 30,
            ui: false,
            allow_ips: Vec::new(),
            allow_all: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationSection {
    #[serde(
        serialize_with = "serialize_role",
        deserialize_with = "deserialize_role"
    )]
    pub role: Role,
    pub cluster_id: Option<u16>,
    pub repl_port: u16,
    pub primary_addr: Option<String>,
    pub peers: Vec<String>,
}

impl Default for ReplicationSection {
    fn default() -> Self {
        Self {
            role: Role::default(),
            cluster_id: None,
            repl_port: 8322,
            primary_addr: None,
            peers: Vec::new(),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterSection {
    pub shard_group: u16,
    pub owned_namespaces: Vec<String>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Supported config file formats.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConfigFormat {
    Yaml,
    Toml,
}

impl ConfigFormat {
    /// Detect format from file extension.
    pub fn from_path(path: &Path) -> Result<Self, String> {
        match path.extension().and_then(|e| e.to_str()) {
            Some("yaml" | "yml") => Ok(Self::Yaml),
            Some("toml") => Ok(Self::Toml),
            Some(ext) => Err(format!(
                "unsupported config format: .{ext} (expected .yaml, .yml, or .toml)"
            )),
            None => Err("config file has no extension (expected .yaml, .yml, or .toml)".into()),
        }
    }
}

/// Load a config file from disk, auto-detecting format from extension.
pub fn load_file(path: &Path) -> Result<FileConfig, String> {
    let format = ConfigFormat::from_path(path)?;
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    parse(&content, format)
}

/// Parse config from a string in the given format.
pub fn parse(content: &str, format: ConfigFormat) -> Result<FileConfig, String> {
    match format {
        ConfigFormat::Yaml => {
            serde_yaml::from_str(content).map_err(|e| format!("YAML parse error: {e}"))
        }
        ConfigFormat::Toml => toml::from_str(content).map_err(|e| format!("TOML parse error: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Merging into Config
// ---------------------------------------------------------------------------

impl FileConfig {
    /// Apply file config values to an engine Config.
    /// The `path` parameter is used as fallback if `storage.path` is not set.
    pub fn apply_to_config(&self, config: &mut Config) {
        let s = &self.storage;
        if let Some(ref p) = s.path {
            config.path = p.clone();
        }
        config.create_if_missing = s.create_if_missing;
        config.write_buffer_size = s.write_buffer_size.0;
        config.max_levels = s.max_levels;
        config.block_size = s.block_size.0;
        config.cache_size = s.cache_size.0;
        config.object_size = s.object_size.0;
        config.compress = s.compress;
        config.bloom_bits = s.bloom_bits;
        config.bloom_prefix_len = s.bloom_prefix_len;
        config.verify_checksums = s.verify_checksums;
        config.compression = s.compression;
        config.io_model = s.io_model;
        config.aol_buffer_size = s.aol_buffer_size;
        config.l0_max_count = s.l0_max_count;
        config.l0_max_size = s.l0_max_size.0;
        config.l1_max_size = s.l1_max_size.0;
        config.default_max_size = s.default_max_size.0;
        config.write_stall_size = s.write_stall_size.0;

        // Replication
        let r = &self.replication;
        config.role = r.role;
        config.cluster_id = r.cluster_id;
        config.repl_port = r.repl_port;
        config.primary_addr = r.primary_addr.clone();
        config.peers = r.peers.clone();

        // Cluster
        config.shard_group = self.cluster.shard_group;
        config.owned_namespaces = self.cluster.owned_namespaces.clone();
    }
}

// ---------------------------------------------------------------------------
// Template generation
// ---------------------------------------------------------------------------

const YAML_TEMPLATE: &str = r#"# rKV configuration file
# All size values accept integers (bytes) or human-readable strings: 1kb, 4mb, 2gb

storage:
  # path: /data/rkv
  create_if_missing: true
  write_buffer_size: 4mb
  max_levels: 3
  block_size: 4kb
  cache_size: 8mb
  object_size: 1kb
  compress: true
  bloom_bits: 10
  bloom_prefix_len: 0
  verify_checksums: true
  compression: lz4          # none | lz4 | zstd
  io_model: mmap             # none | directio | mmap
  aol_buffer_size: 128
  l0_max_count: 4
  l0_max_size: 64mb
  l1_max_size: 256mb
  default_max_size: 2gb
  write_stall_size: 8mb

server:
  bind: 127.0.0.1
  port: 8321
  body_limit: 2mb
  timeout: 30               # seconds, 0 = no timeout
  ui: false
  allow_ips: []
  allow_all: false

replication:
  role: standalone           # standalone | primary | replica | peer
  # cluster_id: null         # 0-65535, null = random
  repl_port: 8322
  # primary_addr: null       # required for replica role
  peers: []

cluster:
  shard_group: 0
  owned_namespaces: []
"#;

const TOML_TEMPLATE: &str = r#"# rKV configuration file
# All size values accept integers (bytes) or human-readable strings: "1kb", "4mb", "2gb"

[storage]
# path = "/data/rkv"
create_if_missing = true
write_buffer_size = "4mb"
max_levels = 3
block_size = "4kb"
cache_size = "8mb"
object_size = "1kb"
compress = true
bloom_bits = 10
bloom_prefix_len = 0
verify_checksums = true
compression = "lz4"          # none | lz4 | zstd
io_model = "mmap"            # none | directio | mmap
aol_buffer_size = 128
l0_max_count = 4
l0_max_size = "64mb"
l1_max_size = "256mb"
default_max_size = "2gb"
write_stall_size = "8mb"

[server]
bind = "127.0.0.1"
port = 8321
body_limit = "2mb"
timeout = 30                 # seconds, 0 = no timeout
ui = false
allow_ips = []
allow_all = false

[replication]
role = "standalone"          # standalone | primary | replica | peer
# cluster_id = 1             # 0-65535, omit for random
repl_port = 8322
# primary_addr = "10.0.0.1:8322"  # required for replica role
peers = []

[cluster]
shard_group = 0
owned_namespaces = []
"#;

pub fn template(format: ConfigFormat) -> &'static str {
    match format {
        ConfigFormat::Yaml => YAML_TEMPLATE,
        ConfigFormat::Toml => TOML_TEMPLATE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_integers() {
        assert_eq!(parse_size("0").unwrap(), 0);
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_size_units() {
        assert_eq!(parse_size("1kb").unwrap(), 1024);
        assert_eq!(parse_size("4mb").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("100b").unwrap(), 100);
    }

    #[test]
    fn parse_size_case_insensitive() {
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
    }

    #[test]
    fn parse_size_fractional() {
        assert_eq!(parse_size("1.5mb").unwrap(), 1_572_864);
    }

    #[test]
    fn parse_size_errors() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("-1kb").is_err());
    }

    #[test]
    fn format_size_roundtrip() {
        assert_eq!(format_size(0), "0");
        assert_eq!(format_size(1024), "1kb");
        assert_eq!(format_size(4 * 1024 * 1024), "4mb");
        assert_eq!(format_size(2 * 1024 * 1024 * 1024), "2gb");
        assert_eq!(format_size(500), "500");
    }

    #[test]
    fn yaml_template_parses() {
        let fc = parse(YAML_TEMPLATE, ConfigFormat::Yaml).unwrap();
        assert_eq!(fc.storage.write_buffer_size, Size(4 * 1024 * 1024));
        assert_eq!(fc.server.port, 8321);
    }

    #[test]
    fn toml_template_parses() {
        let fc = parse(TOML_TEMPLATE, ConfigFormat::Toml).unwrap();
        assert_eq!(fc.storage.write_buffer_size, Size(4 * 1024 * 1024));
        assert_eq!(fc.server.port, 8321);
    }

    #[test]
    fn yaml_roundtrip() {
        let yaml = r#"
storage:
  write_buffer_size: 8mb
  compression: zstd
  io_model: directio
server:
  port: 9000
  ui: true
replication:
  role: primary
  repl_port: 9322
cluster:
  shard_group: 3
"#;
        let fc = parse(yaml, ConfigFormat::Yaml).unwrap();
        assert_eq!(fc.storage.write_buffer_size, Size(8 * 1024 * 1024));
        assert!(matches!(fc.storage.compression, Compression::Zstd));
        assert!(matches!(fc.storage.io_model, IoModel::DirectIO));
        assert_eq!(fc.server.port, 9000);
        assert!(fc.server.ui);
        assert!(matches!(fc.replication.role, Role::Primary));
        assert_eq!(fc.cluster.shard_group, 3);
    }

    #[test]
    fn toml_roundtrip() {
        let toml_str = r#"
[storage]
write_buffer_size = "8mb"
compression = "zstd"
io_model = "directio"

[server]
port = 9000
ui = true

[replication]
role = "primary"
repl_port = 9322

[cluster]
shard_group = 3
"#;
        let fc = parse(toml_str, ConfigFormat::Toml).unwrap();
        assert_eq!(fc.storage.write_buffer_size, Size(8 * 1024 * 1024));
        assert!(matches!(fc.storage.compression, Compression::Zstd));
        assert_eq!(fc.server.port, 9000);
        assert_eq!(fc.cluster.shard_group, 3);
    }

    #[test]
    fn apply_to_config_sets_fields() {
        let yaml = r#"
storage:
  path: /tmp/test-apply
  write_buffer_size: 16mb
  max_levels: 5
  compression: zstd
replication:
  role: peer
  peers:
    - "10.0.0.2:8322"
cluster:
  shard_group: 2
  owned_namespaces:
    - users
    - orders
"#;
        let fc = parse(yaml, ConfigFormat::Yaml).unwrap();
        let mut config = Config::new("/tmp/ignored");
        fc.apply_to_config(&mut config);

        assert_eq!(config.path, PathBuf::from("/tmp/test-apply"));
        assert_eq!(config.write_buffer_size, 16 * 1024 * 1024);
        assert_eq!(config.max_levels, 5);
        assert!(matches!(config.compression, Compression::Zstd));
        assert!(matches!(config.role, Role::Peer));
        assert_eq!(config.peers, vec!["10.0.0.2:8322"]);
        assert_eq!(config.shard_group, 2);
        assert_eq!(config.owned_namespaces, vec!["users", "orders"]);
    }

    #[test]
    fn format_detection() {
        assert_eq!(
            ConfigFormat::from_path(Path::new("config.yaml")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            ConfigFormat::from_path(Path::new("config.yml")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            ConfigFormat::from_path(Path::new("config.toml")).unwrap(),
            ConfigFormat::Toml
        );
        assert!(ConfigFormat::from_path(Path::new("config.json")).is_err());
        assert!(ConfigFormat::from_path(Path::new("config")).is_err());
    }

    #[test]
    fn unknown_field_rejected() {
        let yaml = "storage:\n  unknown_field: true\n";
        assert!(parse(yaml, ConfigFormat::Yaml).is_err());
    }

    #[test]
    fn empty_config_uses_defaults() {
        let fc = parse("{}", ConfigFormat::Yaml).unwrap();
        assert_eq!(fc.storage.max_levels, 3);
        assert_eq!(fc.server.port, 8321);
        assert!(matches!(fc.replication.role, Role::Standalone));
    }
}
