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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_embed() {
        let cfg = RillConfig::default();
        assert_eq!(cfg.rkv.mode, BackendMode::Embed);
        assert_eq!(cfg.rkv.data, "./rill-data");
        assert_eq!(cfg.port, 3000);
    }

    #[test]
    fn yaml_template_parses() {
        let cfg: RillConfig = serde_yaml::from_str(YAML_TEMPLATE).unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Embed);
        assert_eq!(cfg.rkv.data, "./rill-data");
        assert_eq!(cfg.rkv.url, "http://localhost:8321");
        assert_eq!(cfg.rkv.storage.max_levels, 3);
        assert_eq!(cfg.port, 3000);
    }

    #[test]
    fn toml_template_parses() {
        let cfg: RillConfig = toml::from_str(TOML_TEMPLATE).unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Embed);
        assert_eq!(cfg.rkv.data, "./rill-data");
        assert_eq!(cfg.rkv.url, "http://localhost:8321");
        assert_eq!(cfg.rkv.storage.max_levels, 3);
        assert_eq!(cfg.port, 3000);
    }

    #[test]
    fn yaml_remote_mode() {
        let yaml = r#"
host: 0.0.0.0
port: 4000
rkv:
  mode: remote
  url: http://rkv-server:8321
"#;
        let cfg: RillConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Remote);
        assert_eq!(cfg.rkv.url, "http://rkv-server:8321");
        assert_eq!(cfg.port, 4000);
    }

    #[test]
    fn toml_remote_mode() {
        let toml_str = r#"
port = 4000

[rkv]
mode = "remote"
url = "http://rkv-server:8321"
"#;
        let cfg: RillConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Remote);
        assert_eq!(cfg.rkv.url, "http://rkv-server:8321");
    }

    #[test]
    fn yaml_embed_with_storage_tuning() {
        let yaml = r#"
rkv:
  mode: embed
  data: /var/lib/rill
  storage:
    write_buffer_size: 16mb
    cache_size: 128mb
    compression: zstd
    max_levels: 5
"#;
        let cfg: RillConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Embed);
        assert_eq!(cfg.rkv.data, "/var/lib/rill");
        assert_eq!(cfg.rkv.storage.write_buffer_size.0, 16 * 1024 * 1024);
        assert_eq!(cfg.rkv.storage.cache_size.0, 128 * 1024 * 1024);
        assert_eq!(cfg.rkv.storage.max_levels, 5);
    }

    #[test]
    fn yaml_auth_tokens() {
        let yaml = r#"
auth:
  admin_token: admin123
  writer_token: writer456
  reader_token: reader789
"#;
        let cfg: RillConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.auth.admin_token.as_deref(), Some("admin123"));
        assert_eq!(cfg.auth.writer_token.as_deref(), Some("writer456"));
        assert_eq!(cfg.auth.reader_token.as_deref(), Some("reader789"));
    }

    #[test]
    fn empty_yaml_uses_defaults() {
        let cfg: RillConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.rkv.mode, BackendMode::Embed);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
        assert!(cfg.auth.admin_token.is_none());
    }

    #[test]
    fn to_rkv_config_uses_data_path() {
        let mut cfg = RillConfig::default();
        cfg.rkv.data = "/tmp/test-rill".to_string();
        cfg.rkv.storage.max_levels = 7;
        let rkv_cfg = cfg.rkv.to_rkv_config();
        assert_eq!(rkv_cfg.path, std::path::PathBuf::from("/tmp/test-rill"));
        assert_eq!(rkv_cfg.max_levels, 7);
    }

    #[test]
    fn dump_yaml_roundtrip() {
        let cfg = RillConfig::default();
        let yaml = cfg.dump("yaml").unwrap();
        let parsed: RillConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.rkv.mode, cfg.rkv.mode);
        assert_eq!(parsed.port, cfg.port);
        assert_eq!(parsed.rkv.data, cfg.rkv.data);
    }

    #[test]
    fn dump_toml_roundtrip() {
        let cfg = RillConfig::default();
        let toml_str = cfg.dump("toml").unwrap();
        let parsed: RillConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.rkv.mode, cfg.rkv.mode);
        assert_eq!(parsed.port, cfg.port);
        assert_eq!(parsed.rkv.data, cfg.rkv.data);
    }

    #[test]
    fn dump_unsupported_format() {
        let cfg = RillConfig::default();
        assert!(cfg.dump("json").is_err());
    }

    #[test]
    fn template_unsupported_format() {
        assert!(RillConfig::template("json").is_err());
    }

    #[test]
    fn load_file_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rill.yaml");
        std::fs::write(
            &path,
            "port: 5000\nrkv:\n  mode: remote\n  url: http://db:8321\n",
        )
        .unwrap();
        let cfg = RillConfig::load(&path).unwrap();
        assert_eq!(cfg.port, 5000);
        assert_eq!(cfg.rkv.mode, BackendMode::Remote);
        assert_eq!(cfg.rkv.url, "http://db:8321");
    }

    #[test]
    fn load_file_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rill.toml");
        std::fs::write(
            &path,
            "port = 5000\n\n[rkv]\nmode = \"remote\"\nurl = \"http://db:8321\"\n",
        )
        .unwrap();
        let cfg = RillConfig::load(&path).unwrap();
        assert_eq!(cfg.port, 5000);
        assert_eq!(cfg.rkv.mode, BackendMode::Remote);
    }

    #[test]
    fn load_file_missing() {
        assert!(RillConfig::load(Path::new("/nonexistent/rill.yaml")).is_err());
    }

    #[test]
    fn load_file_bad_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rill.json");
        std::fs::write(&path, "{}").unwrap();
        assert!(RillConfig::load(&path).is_err());
    }
}
