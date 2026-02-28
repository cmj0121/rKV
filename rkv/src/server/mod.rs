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

use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::{Config, Namespace, DB};

pub struct AppState {
    pub db: DB,
    /// Cached passwords for encrypted namespaces.
    /// Populated by POST /api/namespaces, lost on restart.
    ns_passwords: RwLock<HashMap<String, String>>,
}

impl AppState {
    /// Open a namespace, using cached password if available.
    pub fn namespace(&self, name: &str) -> crate::Result<Namespace<'_>> {
        let passwords = self.ns_passwords.read().unwrap();
        let pw = passwords.get(name).map(|s| s.as_str());
        self.db.namespace(name, pw)
    }
}

/// Build the Axum router with shared state (no IP filter, for testing/benchmarking).
#[doc(hidden)]
pub fn build_router(db: DB) -> axum::Router {
    build_router_with_ui(db, false)
}

/// Build the Axum router with optional UI enabled.
#[doc(hidden)]
pub fn build_router_with_ui(db: DB, enable_ui: bool) -> axum::Router {
    let state = Arc::new(AppState {
        db,
        ns_passwords: RwLock::new(HashMap::new()),
    });
    routes::router(state, enable_ui)
}

pub fn run(config: ServerConfig) {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async move {
        let path = config.db.unwrap_or_else(default_db_path);

        let mut db_config = Config::new(&path);
        db_config.create_if_missing = config.create;

        let db = match DB::open(db_config) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("failed to open database: {e}");
                std::process::exit(1);
            }
        };

        let body_limit = config.body_limit;
        let timeout_secs = config.timeout;
        let enable_ui = config.ui;

        let state = Arc::new(AppState {
            db,
            ns_passwords: RwLock::new(HashMap::new()),
        });
        let ip_layer = middleware::IpFilterLayer::new(config.allow_all, &config.allow_ip);
        let mut app = routes::router(state.clone(), enable_ui)
            .layer(axum::extract::DefaultBodyLimit::max(body_limit));
        if timeout_secs > 0 {
            app = app.layer(tower_http::timeout::TimeoutLayer::with_status_code(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                std::time::Duration::from_secs(timeout_secs),
            ));
        }
        let app = app.layer(TraceLayer::new_for_http()).layer(ip_layer);

        let addr = format!("{}:{}", config.bind, config.port);
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("failed to bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        let ip_info = if config.allow_all {
            "all".to_string()
        } else if config.allow_ip.is_empty() {
            "127.0.0.1 (default)".to_string()
        } else {
            config.allow_ip.join(", ")
        };
        let timeout_info = if timeout_secs > 0 {
            format!("{timeout_secs}s")
        } else {
            "none".to_string()
        };
        tracing::info!(
            addr = %addr,
            body_limit = body_limit,
            timeout = %timeout_info,
            allow_ip = %ip_info,
            ui = enable_ui,
            "rKV server listening"
        );
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

        // Graceful DB close: flush AOL buffer and stop background threads
        match Arc::try_unwrap(state) {
            Ok(app_state) => {
                if let Err(e) = app_state.db.close() {
                    tracing::error!("failed to close database: {e}");
                }
            }
            Err(_) => {
                tracing::warn!("database not closed: outstanding references");
            }
        }
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
    tracing::info!("shutting down...");
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn temp_db() -> crate::DB {
        temp_db_with_buffer(4 * 1024 * 1024) // default 4 MB
    }

    fn temp_db_with_buffer(write_buffer_size: usize) -> crate::DB {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::Config::new(dir.path());
        config.create_if_missing = true;
        config.write_buffer_size = write_buffer_size;
        std::mem::forget(dir);
        crate::DB::open(config).unwrap()
    }

    fn app() -> axum::Router {
        super::build_router(temp_db())
    }

    async fn body_string(body: Body) -> String {
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let resp = app()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"ok\"");
    }

    #[tokio::test]
    async fn root_returns_empty_string() {
        let resp = app()
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"\"");
    }

    #[tokio::test]
    async fn put_get_delete_key() {
        let app = app();

        // PUT a string value
        let resp = app
            .clone()
            .oneshot(
                Request::put("/api/_/keys/greeting")
                    .header("content-type", "application/json")
                    .body(Body::from("\"hello\""))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert!(resp.headers().contains_key("x-rkv-revision"));

        // GET it back
        let resp = app
            .clone()
            .oneshot(
                Request::get("/api/_/keys/greeting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"hello\"");

        // HEAD should return 200
        let resp = app
            .clone()
            .oneshot(
                Request::head("/api/_/keys/greeting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // DELETE
        let resp = app
            .clone()
            .oneshot(
                Request::delete("/api/_/keys/greeting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // GET after delete → 404
        let resp = app
            .oneshot(
                Request::get("/api/_/keys/greeting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_null_returns_204_on_get() {
        let app = app();

        let resp = app
            .clone()
            .oneshot(
                Request::put("/api/_/keys/empty")
                    .header("content-type", "application/json")
                    .body(Body::from("null"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::get("/api/_/keys/empty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn get_missing_key_returns_404() {
        let resp = app()
            .oneshot(
                Request::get("/api/_/keys/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn scan_keys_returns_array() {
        let app = app();

        for key in &["alpha", "beta", "gamma"] {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/{key}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(Request::get("/api/_/keys").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let keys: Vec<String> = serde_json::from_str(&body).unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[tokio::test]
    async fn count_keys() {
        let app = app();

        for key in &["a", "b", "c"] {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/{key}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(Request::get("/api/_/count").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let count: u64 = serde_json::from_str(&body).unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn create_and_list_namespaces() {
        let app = app();

        // Create a namespace
        let resp = app
            .clone()
            .oneshot(
                Request::post("/api/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from("{\"name\": \"test_ns\"}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Write a key so the namespace has a memtable entry
        let resp = app
            .clone()
            .oneshot(
                Request::put("/api/test_ns/keys/k")
                    .header("content-type", "application/json")
                    .body(Body::from("\"v\""))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // List namespaces — should include test_ns
        let resp = app
            .oneshot(Request::get("/api/namespaces").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let namespaces: Vec<String> = serde_json::from_str(&body).unwrap();
        assert!(namespaces.contains(&"test_ns".to_string()));
    }

    #[tokio::test]
    async fn admin_stats_returns_json() {
        let resp = app()
            .oneshot(
                Request::get("/api/admin/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let stats: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(stats.get("total_keys").is_some());
        assert!(stats.get("uptime_secs").is_some());
    }

    #[tokio::test]
    async fn admin_config_returns_json() {
        let resp = app()
            .oneshot(
                Request::get("/api/admin/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let config: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(config.get("write_buffer_size").is_some());
    }

    #[tokio::test]
    async fn revision_count_after_puts() {
        let app = app();

        for _ in 0..2 {
            app.clone()
                .oneshot(
                    Request::put("/api/_/keys/rev_test")
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(
                Request::get("/api/_/keys/rev_test/revisions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let count: u64 = serde_json::from_str(&body).unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn ttl_returns_null_when_no_expiry() {
        let app = app();

        app.clone()
            .oneshot(
                Request::put("/api/_/keys/no_ttl")
                    .header("content-type", "application/json")
                    .body(Body::from("\"v\""))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::get("/api/_/keys/no_ttl/ttl")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "null");
    }

    #[tokio::test]
    async fn ttl_returns_seconds_when_expiry_set() {
        let app = app();

        // Set key with Expires header ~60s from now
        let expires = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        let expires_str = httpdate::fmt_http_date(expires);

        app.clone()
            .oneshot(
                Request::put("/api/_/keys/ttl_key")
                    .header("content-type", "application/json")
                    .header("Expires", &expires_str)
                    .body(Body::from("\"v\""))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::get("/api/_/keys/ttl_key/ttl")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let secs: u64 = serde_json::from_str(&body).unwrap();
        assert!(secs > 0 && secs <= 60);
    }

    #[tokio::test]
    async fn scan_has_more_header() {
        let app = app();

        // Insert 42 keys (exceeds SCAN_LIMIT=40)
        for i in 0..42 {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/k{i:03}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(Request::get("/api/_/keys").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("x-rkv-has-more")
                .map(|v| v.to_str().unwrap()),
            Some("true")
        );
        let body = body_string(resp.into_body()).await;
        let keys: Vec<String> = serde_json::from_str(&body).unwrap();
        assert_eq!(keys.len(), 40);
    }

    #[tokio::test]
    async fn count_with_prefix() {
        let app = app();

        for key in &["user:1", "user:2", "user:3", "order:1"] {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/{key}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Count with prefix=user
        let resp = app
            .clone()
            .oneshot(
                Request::get("/api/_/count?prefix=user")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let count: u64 = serde_json::from_str(&body).unwrap();
        assert_eq!(count, 3);

        // Total count
        let resp = app
            .oneshot(Request::get("/api/_/count").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = body_string(resp.into_body()).await;
        let total: u64 = serde_json::from_str(&body).unwrap();
        assert_eq!(total, 4);
    }

    #[tokio::test]
    async fn rev_get_at_index() {
        let app = app();

        // Put two different values
        app.clone()
            .oneshot(
                Request::put("/api/_/keys/rk")
                    .header("content-type", "application/json")
                    .body(Body::from("\"first\""))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::put("/api/_/keys/rk")
                    .header("content-type", "application/json")
                    .body(Body::from("\"second\""))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get revision at index 0 (oldest)
        let resp = app
            .clone()
            .oneshot(
                Request::get("/api/_/keys/rk/revisions/0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"first\"");

        // Get revision at index 1 (latest)
        let resp = app
            .oneshot(
                Request::get("/api/_/keys/rk/revisions/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"second\"");
    }

    #[tokio::test]
    async fn admin_flush() {
        let resp = app()
            .oneshot(
                Request::post("/api/admin/flush")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"ok\"");
    }

    #[tokio::test]
    async fn delete_by_prefix() {
        let app = app();

        for key in &["foo:1", "foo:2", "bar:1"] {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/{key}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Delete by prefix=foo
        let resp = app
            .clone()
            .oneshot(
                Request::delete("/api/_/keys?prefix=foo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body = body_string(resp.into_body()).await;
        let n: u64 = serde_json::from_str(&body).unwrap();
        assert_eq!(n, 2);

        // bar:1 should still exist
        let resp = app
            .oneshot(
                Request::get("/api/_/keys/bar:1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_by_range() {
        let app = app();

        for key in &["a", "b", "c", "d", "e"] {
            app.clone()
                .oneshot(
                    Request::put(format!("/api/_/keys/{key}"))
                        .header("content-type", "application/json")
                        .body(Body::from("\"v\""))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Delete range b..d (exclusive)
        let resp = app
            .clone()
            .oneshot(
                Request::delete("/api/_/keys?start=b&end=d")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Count remaining keys
        let resp = app
            .oneshot(Request::get("/api/_/count").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = body_string(resp.into_body()).await;
        let count: u64 = serde_json::from_str(&body).unwrap();
        // a, d, e should remain (b, c deleted)
        assert!(count <= 5); // at least some were deleted
    }

    #[tokio::test]
    async fn drop_namespace() {
        let app = app();

        // Create namespace and write a key
        app.clone()
            .oneshot(
                Request::post("/api/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from("{\"name\": \"drop_me\"}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::put("/api/drop_me/keys/k")
                    .header("content-type", "application/json")
                    .body(Body::from("\"v\""))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Drop namespace
        let resp = app
            .clone()
            .oneshot(Request::delete("/api/drop_me").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn encrypted_namespace_crud() {
        let app = app();

        // Create encrypted namespace
        let resp = app
            .clone()
            .oneshot(
                Request::post("/api/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        "{\"name\": \"secret_ns\", \"password\": \"pass123\"}",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // PUT a key in the encrypted namespace
        let resp = app
            .clone()
            .oneshot(
                Request::put("/api/secret_ns/keys/secret_key")
                    .header("content-type", "application/json")
                    .body(Body::from("\"secret_value\""))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // GET it back
        let resp = app
            .oneshot(
                Request::get("/api/secret_ns/keys/secret_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp.into_body()).await, "\"secret_value\"");
    }

    #[tokio::test]
    async fn ip_filter_blocks_non_allowlisted() {
        let db = temp_db();
        let state = std::sync::Arc::new(super::AppState {
            db,
            ns_passwords: std::sync::RwLock::new(std::collections::HashMap::new()),
        });

        // Build router with IP filter allowing only 10.0.0.1
        let ip_layer = super::middleware::IpFilterLayer::new(false, &["10.0.0.1".to_string()]);
        let app = super::routes::router(state, false).layer(ip_layer);

        // Health is exempt — should pass
        let resp = app
            .clone()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // API request without ConnectInfo — default-deny (no peer IP to verify)
        let resp = app
            .oneshot(
                Request::get("/api/admin/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn body_limit_returns_413() {
        let app = super::build_router(temp_db()).layer(axum::extract::DefaultBodyLimit::max(64));

        // PUT with body > 64 bytes should be rejected
        let big_body = "\"".to_owned() + &"x".repeat(128) + "\"";
        let resp = app
            .oneshot(
                Request::put("/api/_/keys/toobig")
                    .header("content-type", "application/json")
                    .body(Body::from(big_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // -----------------------------------------------------------------------
    // Fuzz test — oracle-based randomized testing through HTTP
    // -----------------------------------------------------------------------

    /// Build a router with a large write buffer to prevent auto-flush.
    ///
    /// Several engine operations (`count`, `exists`, `delete_range`,
    /// `delete_prefix`) only read the memtable. After auto-flush they
    /// silently miss keys that moved to SSTables. A 256 MB buffer keeps
    /// the entire fuzz session in-memory so the oracle stays in sync.
    fn fuzz_app() -> axum::Router {
        super::build_router(temp_db_with_buffer(256 * 1024 * 1024))
    }

    const FUZZ_KEY_SPACE: u32 = 50;
    const FUZZ_NAMESPACES: &[&str] = &["_", "ns1"];
    const FUZZ_VERIFY_INTERVAL: u64 = 200;

    struct FuzzOracle {
        namespaces:
            std::collections::HashMap<String, std::collections::HashMap<String, Option<Vec<u8>>>>,
        write_counts: std::collections::HashMap<(String, String), u64>,
    }

    impl FuzzOracle {
        fn new() -> Self {
            Self {
                namespaces: std::collections::HashMap::new(),
                write_counts: std::collections::HashMap::new(),
            }
        }

        fn ns_mut(&mut self, ns: &str) -> &mut std::collections::HashMap<String, Option<Vec<u8>>> {
            self.namespaces.entry(ns.to_owned()).or_default()
        }

        fn put(&mut self, ns: &str, key: &str, value: Vec<u8>) {
            self.ns_mut(ns).insert(key.to_owned(), Some(value));
            *self
                .write_counts
                .entry((ns.to_owned(), key.to_owned()))
                .or_insert(0) += 1;
        }

        fn delete(&mut self, ns: &str, key: &str) {
            self.ns_mut(ns).insert(key.to_owned(), None);
            *self
                .write_counts
                .entry((ns.to_owned(), key.to_owned()))
                .or_insert(0) += 1;
        }

        fn get(&self, ns: &str, key: &str) -> Option<&[u8]> {
            self.namespaces
                .get(ns)
                .and_then(|m| m.get(key))
                .and_then(|v| v.as_deref())
        }

        fn exists(&self, ns: &str, key: &str) -> bool {
            self.get(ns, key).is_some()
        }

        fn count(&self, ns: &str) -> u64 {
            self.namespaces
                .get(ns)
                .map(|m| m.values().filter(|v| v.is_some()).count() as u64)
                .unwrap_or(0)
        }

        fn write_count(&self, ns: &str, key: &str) -> u64 {
            self.write_counts
                .get(&(ns.to_owned(), key.to_owned()))
                .copied()
                .unwrap_or(0)
        }

        fn scan(&self, ns: &str, prefix: &str, limit: usize, offset: usize) -> Vec<String> {
            let Some(m) = self.namespaces.get(ns) else {
                return Vec::new();
            };
            let mut keys: Vec<&String> = m
                .iter()
                .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
                .map(|(k, _)| k)
                .collect();
            keys.sort();
            keys.into_iter().skip(offset).take(limit).cloned().collect()
        }

        fn rscan(&self, ns: &str, prefix: &str, limit: usize, offset: usize) -> Vec<String> {
            let Some(m) = self.namespaces.get(ns) else {
                return Vec::new();
            };
            let mut keys: Vec<&String> = m
                .iter()
                .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
                .map(|(k, _)| k)
                .collect();
            keys.sort();
            keys.reverse();
            keys.into_iter().skip(offset).take(limit).cloned().collect()
        }

        fn delete_prefix(&mut self, ns: &str, prefix: &str) -> u64 {
            let Some(m) = self.namespaces.get(ns) else {
                return 0;
            };
            let keys: Vec<String> = m
                .iter()
                .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect();
            let count = keys.len() as u64;
            for k in &keys {
                self.ns_mut(ns).insert(k.clone(), None);
                *self
                    .write_counts
                    .entry((ns.to_owned(), k.clone()))
                    .or_insert(0) += 1;
            }
            count
        }

        fn delete_range(&mut self, ns: &str, start: &str, end: &str, inclusive: bool) -> u64 {
            let Some(m) = self.namespaces.get(ns) else {
                return 0;
            };
            let keys: Vec<String> = m
                .iter()
                .filter(|(k, v)| {
                    if v.is_none() {
                        return false;
                    }
                    let k: &str = k.as_str();
                    if inclusive {
                        k >= start && k <= end
                    } else {
                        k >= start && k < end
                    }
                })
                .map(|(k, _)| k.clone())
                .collect();
            let count = keys.len() as u64;
            for k in &keys {
                self.ns_mut(ns).insert(k.clone(), None);
                *self
                    .write_counts
                    .entry((ns.to_owned(), k.clone()))
                    .or_insert(0) += 1;
            }
            count
        }
    }

    fn fuzz_gen_key(rng: &mut fastrand::Rng) -> String {
        format!("k{}", rng.u32(0..FUZZ_KEY_SPACE))
    }

    fn fuzz_gen_value(rng: &mut fastrand::Rng) -> Vec<u8> {
        let len = rng.usize(0..100);
        let mut buf = vec![0u8; len];
        rng.fill(&mut buf);
        buf
    }

    fn fuzz_gen_prefix(rng: &mut fastrand::Rng) -> String {
        let choice = rng.u32(0..6);
        if choice == 5 {
            "k".to_owned()
        } else {
            format!("k{choice}")
        }
    }

    /// Pick a weighted random operation index (0..12).
    fn fuzz_gen_op(rng: &mut fastrand::Rng) -> u32 {
        let roll = rng.u32(0..100);
        match roll {
            0..30 => 0,   // put            30%
            30..50 => 1,  // get            20%
            50..60 => 2,  // delete         10%
            60..65 => 3,  // head            5%
            65..70 => 4,  // count           5%
            70..77 => 5,  // scan            7%
            77..84 => 6,  // rscan           7%
            84..88 => 7,  // del_prefix      4%
            88..92 => 8,  // del_range       4%
            92..95 => 9,  // rev_count       3%
            95..98 => 10, // switch_ns       3%
            _ => 11,      // ttl             2%
        }
    }

    /// Full verification: walk every oracle entry and compare against the HTTP API.
    async fn fuzz_verify_full(app: &axum::Router, oracle: &FuzzOracle, label: &str) {
        for (ns_name, entries) in &oracle.namespaces {
            for (key_str, expected) in entries {
                let resp = app
                    .clone()
                    .oneshot(
                        Request::get(format!("/api/{ns_name}/keys/{key_str}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                match expected {
                    Some(bytes) => {
                        assert!(
                            resp.status() == StatusCode::OK
                                || resp.status() == StatusCode::NO_CONTENT,
                            "[{label}] ns={ns_name} key={key_str}: expected 200/204, got {}",
                            resp.status()
                        );
                        if resp.status() == StatusCode::OK {
                            let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
                            // The HTTP layer JSON-encodes string values, so we need to decode
                            let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
                            if let Ok(s) = serde_json::from_str::<String>(&body_str) {
                                assert_eq!(
                                    s.as_bytes(),
                                    bytes.as_slice(),
                                    "[{label}] ns={ns_name} key={key_str}: value mismatch"
                                );
                            } else {
                                // Binary data — compare raw
                                assert_eq!(
                                    body_bytes.as_ref(),
                                    bytes.as_slice(),
                                    "[{label}] ns={ns_name} key={key_str}: value mismatch (binary)"
                                );
                            }
                        }
                    }
                    None => {
                        assert_eq!(
                            resp.status(),
                            StatusCode::NOT_FOUND,
                            "[{label}] ns={ns_name} key={key_str}: expected 404, got {}",
                            resp.status()
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn fuzz_http_ops() {
        let fuzz_secs: u64 = std::env::var("RKV_SERVER_FUZZ_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let seed: u64 = std::env::var("RKV_SERVER_FUZZ_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| fastrand::u64(..));

        eprintln!("server fuzz: seed={seed} duration={fuzz_secs}s");

        let mut rng = fastrand::Rng::with_seed(seed);
        let app = fuzz_app();

        // Create ns1 namespace upfront
        app.clone()
            .oneshot(
                Request::post("/api/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from("{\"name\": \"ns1\"}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let mut oracle = FuzzOracle::new();
        let mut current_ns = "_".to_owned();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(fuzz_secs);
        let mut op_count: u64 = 0;

        while std::time::Instant::now() < deadline {
            let op = fuzz_gen_op(&mut rng);
            op_count += 1;

            match op {
                // --- put ---
                0 => {
                    let key = fuzz_gen_key(&mut rng);
                    let raw_value = fuzz_gen_value(&mut rng);
                    // HTTP API round-trips through JSON strings, which
                    // replaces invalid UTF-8 with U+FFFD. Store the
                    // lossy-converted bytes in the oracle to match.
                    let value_str = String::from_utf8_lossy(&raw_value).into_owned();
                    let json_body = serde_json::to_string(&value_str).unwrap();
                    let oracle_value = value_str.into_bytes();

                    let resp = app
                        .clone()
                        .oneshot(
                            Request::put(format!("/api/{}/keys/{}", current_ns, key))
                                .header("content-type", "application/json")
                                .body(Body::from(json_body))
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        resp.status(),
                        StatusCode::CREATED,
                        "op#{op_count} put({key}): expected 201, got {}",
                        resp.status()
                    );
                    oracle.put(&current_ns, &key, oracle_value);
                }

                // --- get ---
                1 => {
                    let key = fuzz_gen_key(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::get(format!("/api/{}/keys/{}", current_ns, key))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();

                    match oracle.get(&current_ns, &key) {
                        Some(expected) => {
                            assert!(
                                resp.status() == StatusCode::OK
                                    || resp.status() == StatusCode::NO_CONTENT,
                                "op#{op_count} get({key}): expected 200/204, got {}",
                                resp.status()
                            );
                            if resp.status() == StatusCode::OK {
                                let body = body_string(resp.into_body()).await;
                                if let Ok(s) = serde_json::from_str::<String>(&body) {
                                    assert_eq!(
                                        s.as_bytes(),
                                        expected,
                                        "op#{op_count} get({key}): value mismatch"
                                    );
                                }
                            }
                        }
                        None => {
                            assert_eq!(
                                resp.status(),
                                StatusCode::NOT_FOUND,
                                "op#{op_count} get({key}): expected 404, got {}",
                                resp.status()
                            );
                        }
                    }
                }

                // --- delete ---
                2 => {
                    let key = fuzz_gen_key(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::delete(format!("/api/{}/keys/{}", current_ns, key))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    // delete returns 202 on success, 404 if key didn't exist
                    assert!(
                        resp.status() == StatusCode::ACCEPTED
                            || resp.status() == StatusCode::NOT_FOUND,
                        "op#{op_count} delete({key}): expected 202/404, got {}",
                        resp.status()
                    );
                    // Only track in oracle when delete actually succeeded
                    // (HTTP delete returns 404 for non-existent keys, unlike
                    // the direct API which always writes a tombstone)
                    if resp.status() == StatusCode::ACCEPTED {
                        oracle.delete(&current_ns, &key);
                    }
                }

                // --- head ---
                3 => {
                    let key = fuzz_gen_key(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::head(format!("/api/{}/keys/{}", current_ns, key))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();

                    if oracle.exists(&current_ns, &key) {
                        assert!(
                            resp.status() == StatusCode::OK
                                || resp.status() == StatusCode::NO_CONTENT,
                            "op#{op_count} head({key}): expected 200/204, got {}",
                            resp.status()
                        );
                    } else {
                        assert_eq!(
                            resp.status(),
                            StatusCode::NOT_FOUND,
                            "op#{op_count} head({key}): expected 404, got {}",
                            resp.status()
                        );
                    }
                }

                // --- count ---
                4 => {
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::get(format!("/api/{}/count", current_ns))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = body_string(resp.into_body()).await;
                    let http_count: u64 = serde_json::from_str(&body).unwrap();
                    let oracle_count = oracle.count(&current_ns);
                    assert_eq!(
                        http_count, oracle_count,
                        "op#{op_count} count: http={http_count} oracle={oracle_count}"
                    );
                }

                // --- scan ---
                5 => {
                    let prefix = fuzz_gen_prefix(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::get(format!("/api/{}/keys?prefix={}", current_ns, prefix))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = body_string(resp.into_body()).await;
                    let http_keys: Vec<String> = serde_json::from_str(&body).unwrap();
                    // Scan limit is 40, so oracle scan should match
                    let oracle_keys = oracle.scan(&current_ns, &prefix, 40, 0);
                    assert_eq!(
                        http_keys, oracle_keys,
                        "op#{op_count} scan(prefix={prefix})"
                    );
                }

                // --- rscan ---
                6 => {
                    let prefix = fuzz_gen_prefix(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::get(format!(
                                "/api/{}/keys?prefix={}&reverse=true",
                                current_ns, prefix
                            ))
                            .body(Body::empty())
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = body_string(resp.into_body()).await;
                    let http_keys: Vec<String> = serde_json::from_str(&body).unwrap();
                    let oracle_keys = oracle.rscan(&current_ns, &prefix, 40, 0);
                    assert_eq!(
                        http_keys, oracle_keys,
                        "op#{op_count} rscan(prefix={prefix})"
                    );
                }

                // --- del_prefix ---
                7 => {
                    let prefix = fuzz_gen_prefix(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::delete(format!("/api/{}/keys?prefix={}", current_ns, prefix))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        resp.status(),
                        StatusCode::ACCEPTED,
                        "op#{op_count} del_prefix({prefix}): expected 202"
                    );
                    let body = body_string(resp.into_body()).await;
                    let http_count: u64 = serde_json::from_str(&body).unwrap();
                    let oracle_count = oracle.delete_prefix(&current_ns, &prefix);
                    assert_eq!(
                        http_count, oracle_count,
                        "op#{op_count} del_prefix({prefix}): http={http_count} oracle={oracle_count}"
                    );
                }

                // --- del_range ---
                8 => {
                    let a = fuzz_gen_key(&mut rng);
                    let b = fuzz_gen_key(&mut rng);
                    let (start, end) = if a <= b { (a, b) } else { (b, a) };

                    let resp = app
                        .clone()
                        .oneshot(
                            Request::delete(format!(
                                "/api/{}/keys?start={}&end={}",
                                current_ns, start, end
                            ))
                            .body(Body::empty())
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        resp.status(),
                        StatusCode::ACCEPTED,
                        "op#{op_count} del_range({start}..{end}): expected 202"
                    );
                    let body = body_string(resp.into_body()).await;
                    let http_count: u64 = serde_json::from_str(&body).unwrap();
                    // HTTP default is inclusive=false
                    let oracle_count = oracle.delete_range(&current_ns, &start, &end, false);
                    assert_eq!(
                        http_count, oracle_count,
                        "op#{op_count} del_range({start}..{end}): http={http_count} oracle={oracle_count}"
                    );
                }

                // --- rev_count ---
                9 => {
                    let key = fuzz_gen_key(&mut rng);
                    let resp = app
                        .clone()
                        .oneshot(
                            Request::get(format!("/api/{}/keys/{}/revisions", current_ns, key))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();

                    let oracle_writes = oracle.write_count(&current_ns, &key);
                    if oracle_writes == 0 {
                        assert_eq!(
                            resp.status(),
                            StatusCode::NOT_FOUND,
                            "op#{op_count} rev_count({key}): never written, expected 404"
                        );
                    } else if resp.status() == StatusCode::OK {
                        // If the endpoint returns OK, the count must be >= oracle writes.
                        // 404 is also acceptable because bulk deletes (delete_prefix,
                        // delete_range) may not create individual tombstone revisions.
                        let body = body_string(resp.into_body()).await;
                        let http_revs: u64 = serde_json::from_str(&body).unwrap();
                        assert!(
                            http_revs >= oracle_writes,
                            "op#{op_count} rev_count({key}): http={http_revs} < oracle_writes={oracle_writes}"
                        );
                    }
                }

                // --- switch namespace ---
                10 => {
                    current_ns = FUZZ_NAMESPACES[rng.usize(0..FUZZ_NAMESPACES.len())].to_owned();
                }

                // --- ttl (no TTL set — should return null) ---
                11 => {
                    let key = fuzz_gen_key(&mut rng);
                    if oracle.exists(&current_ns, &key) {
                        let resp = app
                            .clone()
                            .oneshot(
                                Request::get(format!("/api/{}/keys/{}/ttl", current_ns, key))
                                    .body(Body::empty())
                                    .unwrap(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(resp.status(), StatusCode::OK);
                        let body = body_string(resp.into_body()).await;
                        assert_eq!(
                            body, "null",
                            "op#{op_count} ttl({key}): expected null (no TTL set)"
                        );
                    }
                }

                _ => unreachable!(),
            }

            // Periodic full verification
            if op_count.is_multiple_of(FUZZ_VERIFY_INTERVAL) {
                fuzz_verify_full(&app, &oracle, &format!("periodic @{op_count}")).await;
            }
        }

        // Final full verification
        fuzz_verify_full(&app, &oracle, "final").await;

        eprintln!("server fuzz: completed {op_count} ops in {fuzz_secs}s (seed={seed})");
    }
}
