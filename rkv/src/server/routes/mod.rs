mod admin;
mod health;
mod keys;
mod namespaces;
mod scan;

use std::sync::Arc;

use axum::routing::{delete, get, post};
use axum::Router;

use super::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health (no auth)
        .route("/", get(health::root))
        .route("/health", get(health::health))
        // Namespaces
        .route(
            "/api/namespaces",
            get(namespaces::list_namespaces).post(namespaces::create_namespace),
        )
        .route("/api/{ns}", delete(namespaces::drop_namespace))
        // Key CRUD
        .route(
            "/api/{ns}/keys/{key}",
            get(keys::get_key)
                .put(keys::put_key)
                .delete(keys::delete_key)
                .head(keys::head_key),
        )
        // Scan & bulk ops
        .route(
            "/api/{ns}/keys",
            get(scan::list_keys).delete(scan::delete_keys),
        )
        .route("/api/{ns}/count", get(scan::count_keys))
        // Admin
        .route("/api/admin/stats", get(admin::get_stats))
        .route("/api/admin/analyze", post(admin::analyze))
        .route("/api/admin/flush", post(admin::flush))
        .route("/api/admin/sync", post(admin::sync))
        .route("/api/admin/compact", post(admin::compact))
        .route("/api/admin/config", get(admin::get_config))
        .with_state(state)
}
