use std::path::PathBuf;

use crate::config_file::FileConfig;

#[derive(clap::Args)]
pub struct ServerConfig {
    /// Loaded file config (set programmatically, not a CLI arg)
    #[arg(skip)]
    pub file_config: Option<FileConfig>,

    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: String,

    /// Listen port
    #[arg(long, default_value_t = 8321)]
    pub port: u16,

    /// Database path
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Allowed source IPs (repeatable)
    #[arg(long = "allow-ip")]
    pub allow_ip: Vec<String>,

    /// Disable IP restriction
    #[arg(long, default_value_t = false)]
    pub allow_all: bool,

    /// Create database if missing
    #[arg(long, default_value_t = true)]
    pub create: bool,

    /// Maximum request body size (e.g. "2mb", "512kb", "1gb", or plain bytes)
    #[arg(long, default_value = "2mb", value_parser = parse_body_limit)]
    pub body_limit: usize,

    /// Request timeout in seconds (0 = no timeout)
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,

    /// Enable embedded web UI at /ui
    #[arg(long, default_value_t = false)]
    pub ui: bool,

    /// Replication role (standalone, primary, replica)
    #[arg(long, default_value = "standalone")]
    pub role: String,

    /// Replication listen port (primary only)
    #[arg(long, default_value_t = 8322)]
    pub repl_port: u16,

    /// Primary address to connect to (replica only, e.g. "10.0.0.1:8322")
    #[arg(long)]
    pub primary_addr: Option<String>,

    /// Peer addresses for master-master replication (repeatable, e.g. --peers "10.0.0.2:8322")
    #[arg(long)]
    pub peers: Vec<String>,

    /// Cluster ID for RevisionID generation (0–65535, omit for random)
    #[arg(long)]
    pub cluster_id: Option<u16>,

    /// Shard group ID for cluster mode
    #[arg(long)]
    pub shard_group: Option<u16>,

    /// Comma-separated list of namespaces owned by this shard node
    #[arg(long, value_delimiter = ',')]
    pub owned_namespaces: Vec<String>,
}

/// Parse a human-readable size string into bytes.
///
/// Accepts: plain bytes (`2097152`), or suffix (`512kb`, `2mb`, `1gb`).
/// Case-insensitive.
pub fn parse_body_limit(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("body limit cannot be empty".to_string());
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
        return Err(format!("invalid body limit: {s}"));
    };

    let num_part = num_part.trim();

    // Prefer exact integer arithmetic; fall back to f64 only for fractions.
    if let Ok(n) = num_part.parse::<usize>() {
        return Ok(n * multiplier);
    }

    let num: f64 = num_part
        .parse()
        .map_err(|_| format!("invalid body limit: {s}"))?;

    if num < 0.0 {
        return Err(format!("body limit cannot be negative: {s}"));
    }

    if !num.is_finite() {
        return Err(format!("invalid body limit: {s}"));
    }

    Ok((num * multiplier as f64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_body_limit_plain_bytes() {
        assert_eq!(parse_body_limit("1024").unwrap(), 1024);
        assert_eq!(parse_body_limit("0").unwrap(), 0);
    }

    #[test]
    fn parse_body_limit_suffixes() {
        assert_eq!(parse_body_limit("1kb").unwrap(), 1024);
        assert_eq!(parse_body_limit("2mb").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_body_limit("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_body_limit("512KB").unwrap(), 512 * 1024);
        assert_eq!(parse_body_limit("100b").unwrap(), 100);
    }

    #[test]
    fn parse_body_limit_fractional() {
        assert_eq!(parse_body_limit("1.5mb").unwrap(), 1_572_864);
        assert_eq!(parse_body_limit("0.5kb").unwrap(), 512);
    }

    #[test]
    fn parse_body_limit_zero() {
        assert_eq!(parse_body_limit("0").unwrap(), 0);
        assert_eq!(parse_body_limit("0kb").unwrap(), 0);
        assert_eq!(parse_body_limit("0mb").unwrap(), 0);
    }

    #[test]
    fn parse_body_limit_errors() {
        assert!(parse_body_limit("").is_err());
        assert!(parse_body_limit("abc").is_err());
        assert!(parse_body_limit("xmb").is_err());
    }
}
