mod admin;
mod batch;
mod health;
mod keys;
mod namespaces;
mod revisions;
mod scan;
mod ui;

use std::sync::Arc;

use axum::routing::{delete, get, post, put};
use axum::Router;

use super::AppState;

pub fn router(state: Arc<AppState>, enable_ui: bool) -> Router {
    let mut r = Router::new()
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
        // Pop (atomic scan+delete)
        .route("/api/{ns}/pop", post(keys::pop_first))
        // Scan & bulk ops
        .route(
            "/api/{ns}/keys",
            get(scan::list_keys).delete(scan::delete_keys),
        )
        .route("/api/{ns}/count", get(scan::count_keys))
        // Batch writes
        .route("/api/{ns}/batch", post(batch::write_batch))
        // Revisions & TTL
        .route("/api/{ns}/keys/{key}/revisions", get(revisions::rev_count))
        .route(
            "/api/{ns}/keys/{key}/revisions/{index}",
            get(revisions::rev_get),
        )
        .route("/api/{ns}/keys/{key}/ttl", get(revisions::get_ttl))
        // Admin
        .route("/api/admin/stats", get(admin::get_stats))
        .route("/api/admin/analyze", post(admin::analyze))
        .route("/api/admin/flush", post(admin::flush))
        .route("/api/admin/sync", post(admin::sync))
        .route("/api/admin/compact", post(admin::compact))
        .route("/api/admin/config", get(admin::get_config))
        .route("/api/admin/force-sync", post(admin::force_sync))
        // Cluster admin
        .route("/api/admin/cluster", get(admin::get_cluster))
        .route("/api/admin/route", post(admin::set_route))
        .route("/api/admin/dedup", put(admin::set_dedup))
        // Per-namespace dedup
        .route(
            "/api/{ns}/dedup",
            get(namespaces::get_ns_dedup).put(namespaces::set_ns_dedup),
        )
        // Prometheus metrics endpoint
        .route("/metrics", get(admin::prometheus_metrics))
        .with_state(state);

    if enable_ui {
        r = r
            .route("/ui", get(ui::index))
            .route("/ui/app.js", get(ui::app_js))
            .route("/ui/style.css", get(ui::style_css))
            .route("/docs", get(ui::docs))
            .route("/docs/openapi.yaml", get(ui::openapi_yaml));
    }

    r
}
