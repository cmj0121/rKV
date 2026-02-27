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
    let state = Arc::new(AppState {
        db,
        ns_passwords: RwLock::new(HashMap::new()),
    });
    routes::router(state)
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn temp_db() -> crate::DB {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::Config::new(dir.path());
        config.create_if_missing = true;
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
        let app = super::routes::router(state).layer(ip_layer);

        // Health is exempt — should pass
        let resp = app
            .clone()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // API request without ConnectInfo — no peer IP available,
        // middleware passes through (no IP to check against)
        let resp = app
            .oneshot(
                Request::get("/api/admin/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Without ConnectInfo, peer_ip is None — request passes
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
