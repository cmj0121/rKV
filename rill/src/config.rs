use std::path::Path;

use rkv::config_file::{ConfigFormat, StorageSection};
use rkv::Config;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RillConfig {
    pub host: String,
    pub port: u16,
    pub ui: bool,
    pub auth: AuthSection,
    pub rkv: RkvBackend,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct AuthSection {
    pub admin_token: Option<String>,
    pub writer_token: Option<String>,
    pub reader_token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RkvBackend {
    pub mode: BackendMode,
    pub data: String,
    pub url: String,
    pub storage: StorageSection,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendMode {
    #[default]
    Embed,
    Remote,
}

impl Default for RkvBackend {
    fn default() -> Self {
        Self {
            mode: BackendMode::Embed,
            data: "./rill-data".to_string(),
            url: "http://localhost:8321".to_string(),
            storage: StorageSection::default(),
        }
    }
}

impl Default for RillConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 3000,
            ui: false,
            auth: AuthSection::default(),
            rkv: RkvBackend::default(),
        }
    }
}

impl RillConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let format = ConfigFormat::from_path(path)?;
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
        match format {
            ConfigFormat::Yaml => {
                serde_yaml::from_str(&content).map_err(|e| format!("YAML parse error: {e}"))
            }
            ConfigFormat::Toml => {
                toml::from_str(&content).map_err(|e| format!("TOML parse error: {e}"))
            }
        }
    }

    pub fn dump(&self, format: &str) -> Result<String, String> {
        match format {
            "yaml" => serde_yaml::to_string(self).map_err(|e| format!("YAML error: {e}")),
            "toml" => toml::to_string_pretty(self).map_err(|e| format!("TOML error: {e}")),
            _ => Err(format!(
                "unsupported format: {format} (use 'yaml' or 'toml')"
            )),
        }
    }

    pub fn template(format: &str) -> Result<&'static str, String> {
        match format {
            "yaml" => Ok(YAML_TEMPLATE),
            "toml" => Ok(TOML_TEMPLATE),
            _ => Err(format!(
                "unsupported format: {format} (use 'yaml' or 'toml')"
            )),
        }
    }
}

impl RkvBackend {
    pub fn to_rkv_config(&self) -> Config {
        let mut config = Config::new(&self.data);
        let s = &self.storage;
        config.create_if_missing = s.create_if_missing;
        config.write_buffer_size = s.write_buffer_size.0;
        config.max_levels = s.max_levels;
        config.block_size = s.block_size.0;
        config.cache_size = s.cache_size.0;
        config.object_size = s.object_size.0;
        config.compress = s.compress;
        config.bloom_bits = s.bloom_bits;
        config.bloom_prefix_len = s.bloom_prefix_len;
        config.filter_policy = s.filter_policy;
        config.verify_checksums = s.verify_checksums;
        config.compression = s.compression;
        config.compression_per_level = s.compression_per_level.clone();
        config.io_model = s.io_model;
        config.aol_buffer_size = s.aol_buffer_size;
        config.l0_max_count = s.l0_max_count;
        config.l0_max_size = s.l0_max_size.0;
        config.l1_max_size = s.l1_max_size.0;
        config.default_max_size = s.default_max_size.0;
        config.write_stall_size = s.write_stall_size.0;
        config.in_memory = s.in_memory;
        config
    }
}

const YAML_TEMPLATE: &str = r#"# Rill configuration file
# All size values accept integers (bytes) or human-readable strings: 1kb, 4mb, 2gb

host: 0.0.0.0
port: 3000
ui: false

auth:
  # admin_token: null          # queue create/delete + all
  # writer_token: null         # push messages
  # reader_token: null         # pop/peek messages

rkv:
  mode: embed                  # embed | remote
  data: ./rill-data            # embed: data directory
  url: http://localhost:8321   # remote: rKV server URL
  storage:                     # embed: storage engine tuning
    create_if_missing: true
    write_buffer_size: 4mb
    max_levels: 3
    block_size: 4kb
    cache_size: 64mb
    object_size: 1kb
    compress: true
    bloom_bits: 10
    bloom_prefix_len: 0
    filter_policy: bloom       # bloom | ribbon
    verify_checksums: true
    compression: lz4           # none | lz4 | zstd
    compression_per_level: []  # e.g. [lz4, lz4, zstd]
    io_model: mmap             # none | directio | mmap
    aol_buffer_size: 128
    l0_max_count: 4
    l0_max_size: 64mb
    l1_max_size: 256mb
    default_max_size: 2gb
    write_stall_size: 8mb
    in_memory: false
"#;

const TOML_TEMPLATE: &str = r#"# Rill configuration file
# All size values accept integers (bytes) or human-readable strings: "1kb", "4mb", "2gb"

host = "0.0.0.0"
port = 3000
ui = false

[auth]
# admin_token = "secret"      # queue create/delete + all
# writer_token = "secret"     # push messages
# reader_token = "secret"     # pop/peek messages

[rkv]
mode = "embed"                 # embed | remote
data = "./rill-data"           # embed: data directory
url = "http://localhost:8321"  # remote: rKV server URL

[rkv.storage]                  # embed: storage engine tuning
create_if_missing = true
write_buffer_size = "4mb"
max_levels = 3
block_size = "4kb"
cache_size = "64mb"
object_size = "1kb"
compress = true
bloom_bits = 10
bloom_prefix_len = 0
filter_policy = "bloom"        # bloom | ribbon
verify_checksums = true
compression = "lz4"            # none | lz4 | zstd
compression_per_level = []     # e.g. ["lz4", "lz4", "zstd"]
io_model = "mmap"              # none | directio | mmap
aol_buffer_size = 128
l0_max_count = 4
l0_max_size = "64mb"
l1_max_size = "256mb"
default_max_size = "2gb"
write_stall_size = "8mb"
in_memory = false
"#;
