use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path as AxumPath, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use clap::Parser;
use rill::backend::{Backend, RkvClient};
use rill::config::{BackendMode, RillConfig};
use rkv::DB;
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "rill", about = "Message queue powered by rKV")]
struct Cli {
    /// Path to config file (YAML or TOML)
    #[arg(long, short, global = true, env = "RILL_CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Dump config to stdout (default or from --config file)
    Init {
        /// Output format: yaml or toml
        #[arg(long, default_value = "yaml")]
        format: String,
    },
    /// Start the HTTP server
    Serve {
        #[arg(long, env = "RILL_HOST")]
        host: Option<String>,

        #[arg(long, env = "RILL_PORT")]
        port: Option<u16>,

        #[arg(long, env = "RILL_ADMIN_TOKEN")]
        admin_token: Option<String>,

        #[arg(long, env = "RILL_WRITER_TOKEN")]
        writer_token: Option<String>,

        #[arg(long, env = "RILL_READER_TOKEN")]
        reader_token: Option<String>,

        #[arg(long, env = "RILL_UI")]
        ui: bool,

        /// rKV backend mode: embed or remote
        #[arg(long, env = "RILL_RKV_MODE")]
        rkv_mode: Option<String>,

        /// Data directory (embed mode)
        #[arg(long, env = "RILL_DATA")]
        data: Option<String>,

        /// rKV server URL (remote mode)
        #[arg(long, env = "RILL_RKV_URL")]
        rkv_url: Option<String>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    Reader = 0,
    Writer = 1,
    Admin = 2,
}

struct AppState {
    backend: Backend,
    admin_token: Option<String>,
    writer_token: Option<String>,
    reader_token: Option<String>,
    ui_enabled: bool,
    started_at: Instant,
}

enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Forbidden,
    NotFound(&'static str),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, format!(r#"{{"error":"{msg}"}}"#)),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                r#"{"error":"unauthorized"}"#.to_string(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                r#"{"error":"forbidden"}"#.to_string(),
            ),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.to_string()),
            Self::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(r#"{{"error":"{msg}"}}"#),
            ),
        };
        (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
    }
}

impl AppState {
    fn authenticate(&self, headers: &HeaderMap) -> Option<Role> {
        if self.admin_token.is_none() && self.writer_token.is_none() && self.reader_token.is_none()
        {
            return Some(Role::Admin);
        }

        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))?;

        if self.admin_token.as_deref() == Some(token) {
            Some(Role::Admin)
        } else if self.writer_token.as_deref() == Some(token) {
            Some(Role::Writer)
        } else if self.reader_token.as_deref() == Some(token) {
            Some(Role::Reader)
        } else {
            None
        }
    }

    fn require_role(&self, headers: &HeaderMap, minimum: Role) -> Result<(), ApiError> {
        match self.authenticate(headers) {
            None => Err(ApiError::Unauthorized),
            Some(role) if role >= minimum => Ok(()),
            Some(_) => Err(ApiError::Forbidden),
        }
    }
}

// --- Request types ---

#[derive(Deserialize)]
struct CreateQueueRequest {
    name: String,
}

fn validate_queue_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest("queue name cannot be empty"));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest("queue name too long (max 128 chars)"));
    }
    if name
        .chars()
        .any(|c| !c.is_alphanumeric() && c != '-' && c != '_')
    {
        return Err(ApiError::BadRequest(
            "queue name may only contain alphanumeric, dash, or underscore",
        ));
    }
    Ok(())
}

// --- Handlers ---

async fn root() -> &'static str {
    ""
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.started_at.elapsed().as_secs();
    let mode = match &state.backend {
        Backend::Embed(_) => "embed",
        Backend::Remote(_) => "remote",
    };
    let queue_count = state
        .backend
        .list_queues()
        .await
        .map(|q| q.len())
        .unwrap_or(0);
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "mode": mode,
        "queues": queue_count,
        "uptime_seconds": uptime,
    }))
}

async fn create_queue(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateQueueRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Admin)?;
    validate_queue_name(&body.name)?;
    state
        .backend
        .create_queue(&body.name)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"queue": body.name, "created": true})))
}

async fn delete_queue(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Admin)?;
    validate_queue_name(&name)?;
    state
        .backend
        .delete_queue(&name)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"queue": name, "deleted": true})))
}

async fn list_queues(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Reader)?;
    let queues = state
        .backend
        .list_queues()
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"queues": queues})))
}

async fn push_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: String,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Writer)?;
    validate_queue_name(&name)?;
    state
        .backend
        .push_message(&name, &body)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"pushed": true})))
}

async fn pop_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Reader)?;
    validate_queue_name(&name)?;
    let msg = state
        .backend
        .pop_message(&name)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"message": msg})))
}

#[derive(Deserialize)]
struct PeekQuery {
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

const MAX_PEEK_LIMIT: usize = 100;

async fn queue_info(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Reader)?;
    validate_queue_name(&name)?;
    let length = state
        .backend
        .queue_length(&name)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(json!({"queue": name, "length": length})))
}

async fn peek_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    axum::extract::Query(query): axum::extract::Query<PeekQuery>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_role(&headers, Role::Reader)?;
    validate_queue_name(&name)?;
    let limit = query.limit.min(MAX_PEEK_LIMIT);
    let messages = state
        .backend
        .peek_messages(&name, query.offset, limit)
        .await
        .map_err(ApiError::Internal)?;
    let items: Vec<_> = messages
        .into_iter()
        .map(|(id, body)| json!({"id": id, "body": body}))
        .collect();
    Ok(Json(json!({"messages": items, "queue": name})))
}

async fn ui_index(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    if !state.ui_enabled {
        return Err(ApiError::NotFound(
            r#"{"error":"UI not enabled. Start with --ui flag."}"#,
        ));
    }
    Ok(Html(include_str!("ui/index.html")))
}

async fn ui_app_js(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    if !state.ui_enabled {
        return Err(ApiError::NotFound(
            r#"{"error":"UI not enabled. Start with --ui flag."}"#,
        ));
    }
    Ok((
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!("ui/app.js"),
    ))
}

async fn ui_style_css(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    if !state.ui_enabled {
        return Err(ApiError::NotFound(
            r#"{"error":"UI not enabled. Start with --ui flag."}"#,
        ));
    }
    Ok((
        [(header::CONTENT_TYPE, "text/css")],
        include_str!("ui/style.css"),
    ))
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/ui", get(ui_index))
        .route("/ui/app.js", get(ui_app_js))
        .route("/ui/style.css", get(ui_style_css))
        .route("/queues", post(create_queue))
        .route("/queues", get(list_queues))
        .route("/queues/{name}", post(push_message))
        .route("/queues/{name}", get(pop_message))
        .route("/queues/{name}", delete(delete_queue))
        .route("/queues/{name}/info", get(queue_info))
        .route("/queues/{name}/messages", get(peek_messages))
        .with_state(state)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Load config file if provided, otherwise use defaults
    let mut cfg = match &cli.config {
        Some(path) => RillConfig::load(Path::new(path)).expect("failed to load config"),
        None => RillConfig::default(),
    };

    match cli.command {
        Command::Init { format } => {
            let output = if cli.config.is_some() {
                // Config file provided — dump the loaded (merged) config
                cfg.dump(&format).unwrap_or_else(|e| {
                    eprintln!("{e}");
                    std::process::exit(1);
                })
            } else {
                // No config file — show annotated template
                RillConfig::template(&format)
                    .unwrap_or_else(|e| {
                        eprintln!("{e}");
                        std::process::exit(1);
                    })
                    .to_string()
            };
            print!("{output}");
        }
        Command::Serve {
            host,
            port,
            admin_token,
            writer_token,
            reader_token,
            ui,
            rkv_mode,
            data,
            rkv_url,
        } => {
            // CLI flags override config file
            if let Some(h) = host {
                cfg.host = h;
            }
            if let Some(p) = port {
                cfg.port = p;
            }
            if let Some(t) = admin_token {
                cfg.auth.admin_token = Some(t);
            }
            if let Some(t) = writer_token {
                cfg.auth.writer_token = Some(t);
            }
            if let Some(t) = reader_token {
                cfg.auth.reader_token = Some(t);
            }
            if ui {
                cfg.ui = true;
            }
            if let Some(m) = rkv_mode {
                cfg.rkv.mode = match m.as_str() {
                    "embed" => BackendMode::Embed,
                    "remote" => BackendMode::Remote,
                    _ => {
                        warn!("invalid rkv mode: {m} (use 'embed' or 'remote')");
                        std::process::exit(1);
                    }
                };
            }
            if let Some(d) = data {
                cfg.rkv.data = d;
            }
            if let Some(u) = rkv_url {
                cfg.rkv.url = u;
            }

            let backend = match cfg.rkv.mode {
                BackendMode::Embed => {
                    let rkv_config = cfg.rkv.to_rkv_config();
                    let db = DB::open(rkv_config).expect("failed to open rKV database");
                    info!("rKV database opened at {}", cfg.rkv.data);
                    Backend::Embed(Box::new(db))
                }
                BackendMode::Remote => {
                    info!("connecting to rKV server at {}", cfg.rkv.url);
                    Backend::Remote(RkvClient::new(&cfg.rkv.url))
                }
            };

            let state = Arc::new(AppState {
                backend,
                admin_token: cfg.auth.admin_token,
                writer_token: cfg.auth.writer_token,
                reader_token: cfg.auth.reader_token,
                ui_enabled: cfg.ui,
                started_at: Instant::now(),
            });

            let app = build_router(state.clone());
            let addr = format!("{}:{}", cfg.host, cfg.port);
            info!("rill listening on {addr}");
            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .unwrap();

            // Flush embedded DB on shutdown
            if let Backend::Embed(db) = &state.backend {
                info!("flushing database...");
                let _ = db.flush();
            }
            info!("rill stopped");
        }
    }
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => { info!("received SIGINT, shutting down..."); }
        () = terminate => { info!("received SIGTERM, shutting down..."); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rkv::Config;
    use tower::ServiceExt;

    fn test_state(ui: bool) -> Arc<AppState> {
        let db = DB::open(Config::in_memory()).unwrap();
        Arc::new(AppState {
            backend: Backend::Embed(Box::new(db)),
            admin_token: Some("admin-tok".to_string()),
            writer_token: Some("writer-tok".to_string()),
            reader_token: Some("reader-tok".to_string()),
            ui_enabled: ui,
            started_at: Instant::now(),
        })
    }

    fn open_state() -> Arc<AppState> {
        let db = DB::open(Config::in_memory()).unwrap();
        Arc::new(AppState {
            backend: Backend::Embed(Box::new(db)),
            admin_token: None,
            writer_token: None,
            reader_token: None,
            ui_enabled: false,
            started_at: Instant::now(),
        })
    }

    async fn request(
        app: &Router,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&str>,
    ) -> (StatusCode, String) {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(tok) = token {
            builder = builder.header("Authorization", format!("Bearer {tok}"));
        }
        if body.is_some() {
            builder = builder.header("Content-Type", "application/json");
        }
        let req = builder
            .body(Body::from(body.unwrap_or("").to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    // --- Public endpoints ---

    #[tokio::test]
    async fn root_returns_200_empty() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "GET", "/", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "");
    }

    #[tokio::test]
    async fn health_returns_diagnostics() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "GET", "/health", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#""status":"ok"#));
        assert!(body.contains(r#""mode":"embed"#));
        assert!(body.contains(r#""version""#));
        assert!(body.contains(r#""uptime_seconds""#));
        assert!(body.contains(r#""queues""#));
    }

    // --- Open mode (no tokens) ---

    #[tokio::test]
    async fn open_mode_allows_all() {
        let app = build_router(open_state());
        let (status, _) = request(&app, "GET", "/queues", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = request(&app, "POST", "/queues", None, Some(r#"{"name":"test"}"#)).await;
        assert_eq!(status, StatusCode::OK);
    }

    // --- Auth: unauthorized ---

    #[tokio::test]
    async fn no_token_returns_401() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "GET", "/queues", None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.contains("unauthorized"));
    }

    #[tokio::test]
    async fn bad_token_returns_401() {
        let app = build_router(test_state(false));
        let (status, _) = request(&app, "GET", "/queues", Some("wrong"), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    // --- Auth: forbidden ---

    #[tokio::test]
    async fn reader_cannot_push() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "POST", "/queues/test", Some("reader-tok"), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.contains("forbidden"));
    }

    #[tokio::test]
    async fn writer_cannot_create_queue() {
        let app = build_router(test_state(false));
        let (status, _) = request(
            &app,
            "POST",
            "/queues",
            Some("writer-tok"),
            Some(r#"{"name":"q"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reader_cannot_delete_queue() {
        let app = build_router(test_state(false));
        let (status, _) = request(&app, "DELETE", "/queues/test", Some("reader-tok"), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    // --- Auth: allowed ---

    #[tokio::test]
    async fn admin_can_create_queue() {
        let app = build_router(test_state(false));
        let (status, body) = request(
            &app,
            "POST",
            "/queues",
            Some("admin-tok"),
            Some(r#"{"name":"tasks"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("tasks"));
        assert!(body.contains("true"));
    }

    #[tokio::test]
    async fn writer_can_push() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "POST", "/queues/test", Some("writer-tok"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("pushed"));
    }

    #[tokio::test]
    async fn reader_can_pop() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "GET", "/queues/test", Some("reader-tok"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("message"));
    }

    #[tokio::test]
    async fn reader_can_list_queues() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "GET", "/queues", Some("reader-tok"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("queues"));
    }

    // --- UI ---

    #[tokio::test]
    async fn ui_disabled() {
        let app = build_router(test_state(false));
        let (status, _) = request(&app, "GET", "/ui", None, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ui_enabled() {
        let app = build_router(test_state(true));
        let (status, body) = request(&app, "GET", "/ui", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Rill"));
    }

    #[tokio::test]
    async fn ui_serves_js() {
        let app = build_router(test_state(true));
        let (status, body) = request(&app, "GET", "/ui/app.js", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Rill Web UI"));
    }

    #[tokio::test]
    async fn ui_serves_css() {
        let app = build_router(test_state(true));
        let (status, body) = request(&app, "GET", "/ui/style.css", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("--accent"));
    }

    // --- Queue name validation ---

    #[tokio::test]
    async fn invalid_queue_name_empty() {
        let app = build_router(open_state());
        let (status, body) = request(&app, "POST", "/queues", None, Some(r#"{"name":""}"#)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("empty"));
    }

    #[tokio::test]
    async fn invalid_queue_name_special_chars() {
        let app = build_router(open_state());
        let (status, body) =
            request(&app, "POST", "/queues", None, Some(r#"{"name":"a/b"}"#)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("alphanumeric"));
    }

    #[tokio::test]
    async fn invalid_queue_name_too_long() {
        let app = build_router(open_state());
        let long_name = "a".repeat(129);
        let payload = format!(r#"{{"name":"{long_name}"}}"#);
        let (status, body) = request(&app, "POST", "/queues", None, Some(&payload)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("too long"));
    }

    #[tokio::test]
    async fn invalid_name_rejected_on_push() {
        let app = build_router(open_state());
        let (status, _) = request(&app, "POST", "/queues/a.b", None, Some("msg")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_name_rejected_on_pop() {
        let app = build_router(open_state());
        let (status, _) = request(&app, "GET", "/queues/a.b", None, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_name_rejected_on_delete() {
        let app = build_router(open_state());
        let (status, _) = request(&app, "DELETE", "/queues/a.b", None, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn valid_queue_name_with_dash_and_underscore() {
        let app = build_router(open_state());
        let (status, _) = request(
            &app,
            "POST",
            "/queues",
            None,
            Some(r#"{"name":"my-queue_1"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // --- Pagination limit cap ---

    #[tokio::test]
    async fn peek_limit_capped_at_max() {
        let app = build_router(open_state());
        request(&app, "POST", "/queues", None, Some(r#"{"name":"cap"}"#)).await;
        // Push 3 messages
        for i in 0..3 {
            request(&app, "POST", "/queues/cap", None, Some(&format!("m{i}"))).await;
        }
        // Request limit=999 — should still work (capped server-side to MAX_PEEK_LIMIT)
        let (status, body) =
            request(&app, "GET", "/queues/cap/messages?limit=999", None, None).await;
        assert_eq!(status, StatusCode::OK);
        // All 3 messages returned (3 < MAX_PEEK_LIMIT)
        assert!(body.contains("m0"));
        assert!(body.contains("m1"));
        assert!(body.contains("m2"));
    }

    // --- Delete queue ---

    #[tokio::test]
    async fn admin_can_delete_queue() {
        let app = build_router(test_state(false));
        let (status, body) = request(&app, "DELETE", "/queues/myq", Some("admin-tok"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("myq"));
        assert!(body.contains("deleted"));
    }

    // --- E2E: push, pop, FIFO through HTTP ---

    #[tokio::test]
    async fn e2e_push_pop_fifo() {
        let app = build_router(open_state());
        // Create queue
        request(&app, "POST", "/queues", None, Some(r#"{"name":"e2e"}"#)).await;
        // Push two messages
        request(&app, "POST", "/queues/e2e", None, Some("msg-a")).await;
        request(&app, "POST", "/queues/e2e", None, Some("msg-b")).await;
        // Pop in FIFO order
        let (_, body) = request(&app, "GET", "/queues/e2e", None, None).await;
        assert!(body.contains("msg-a"));
        let (_, body) = request(&app, "GET", "/queues/e2e", None, None).await;
        assert!(body.contains("msg-b"));
        // Pop empty returns null
        let (_, body) = request(&app, "GET", "/queues/e2e", None, None).await;
        assert!(body.contains("null"));
    }

    #[tokio::test]
    async fn e2e_create_list_delete() {
        let app = build_router(open_state());
        // Create two queues
        request(&app, "POST", "/queues", None, Some(r#"{"name":"q1"}"#)).await;
        request(&app, "POST", "/queues", None, Some(r#"{"name":"q2"}"#)).await;
        // List should contain both
        let (_, body) = request(&app, "GET", "/queues", None, None).await;
        assert!(body.contains("q1"));
        assert!(body.contains("q2"));
        // Push to q1 then delete it
        request(&app, "POST", "/queues/q1", None, Some("data")).await;
        request(&app, "DELETE", "/queues/q1", None, None).await;
        // Pop from deleted queue returns null
        let (_, body) = request(&app, "GET", "/queues/q1", None, None).await;
        assert!(body.contains("null"));
    }

    #[tokio::test]
    async fn e2e_pop_nonexistent_queue() {
        let app = build_router(open_state());
        let (status, body) = request(&app, "GET", "/queues/nope", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("null"));
    }

    // --- Queue info and peek ---

    #[tokio::test]
    async fn e2e_queue_info() {
        let app = build_router(open_state());
        request(&app, "POST", "/queues", None, Some(r#"{"name":"info"}"#)).await;
        request(&app, "POST", "/queues/info", None, Some("a")).await;
        request(&app, "POST", "/queues/info", None, Some("b")).await;
        let (status, body) = request(&app, "GET", "/queues/info/info", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(r#""length":2"#));
    }

    #[tokio::test]
    async fn e2e_peek_messages() {
        let app = build_router(open_state());
        request(&app, "POST", "/queues", None, Some(r#"{"name":"peek"}"#)).await;
        request(&app, "POST", "/queues/peek", None, Some("x")).await;
        request(&app, "POST", "/queues/peek", None, Some("y")).await;
        // Peek without consuming
        let (status, body) = request(&app, "GET", "/queues/peek/messages", None, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"x\""));
        assert!(body.contains("\"y\""));
        // Messages still there
        let (_, body) = request(&app, "GET", "/queues/peek/info", None, None).await;
        assert!(body.contains(r#""length":2"#));
    }
}
