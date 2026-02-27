mod config;
mod error;
mod middleware;
mod routes;
mod types;

pub use config::ServerConfig;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use rkv::{Config, Namespace, DB};

pub struct AppState {
    pub db: DB,
    /// Cached passwords for encrypted namespaces.
    /// Populated by POST /api/namespaces, lost on restart.
    ns_passwords: RwLock<HashMap<String, String>>,
}

impl AppState {
    /// Open a namespace, using cached password if available.
    pub fn namespace(&self, name: &str) -> rkv::Result<Namespace<'_>> {
        let passwords = self.ns_passwords.read().unwrap();
        let pw = passwords.get(name).map(|s| s.as_str());
        self.db.namespace(name, pw)
    }
}

pub fn run(config: ServerConfig) {
    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async move {
        let path = config.db.unwrap_or_else(default_db_path);

        let mut db_config = Config::new(&path);
        db_config.create_if_missing = config.create;

        let db = match DB::open(db_config) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("failed to open database: {e}");
                std::process::exit(1);
            }
        };

        let state = Arc::new(AppState {
            db,
            ns_passwords: RwLock::new(HashMap::new()),
        });
        let ip_layer = middleware::IpFilterLayer::new(config.allow_all, &config.allow_ip);
        let app = routes::router(state).layer(ip_layer);

        let addr = format!("{}:{}", config.bind, config.port);
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("failed to bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        println!("rKV server listening on {addr}");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
    });
}

fn default_db_path() -> PathBuf {
    dirs_sys::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".rkv")
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    println!("\nshutting down...");
}
