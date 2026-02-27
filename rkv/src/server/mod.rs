mod config;
mod routes;

pub use config::ServerConfig;

use std::path::PathBuf;
use std::sync::Arc;

use rkv::{Config, DB};

#[allow(dead_code)] // db consumed in Phase 2+
pub struct AppState {
    pub db: DB,
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

        let state = Arc::new(AppState { db });
        let app = routes::router(state);

        let addr = format!("{}:{}", config.bind, config.port);
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("failed to bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        println!("rKV server listening on {addr}");
        axum::serve(listener, app)
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
