use std::path::PathBuf;

#[derive(clap::Args)]
pub struct ServerConfig {
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
}
