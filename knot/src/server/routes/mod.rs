pub mod health;
pub mod metadata;
pub mod namespaces;

use std::sync::Arc;

use axum::routing::{delete, get};
use axum::Router;

use super::AppState;

pub fn api_routes() -> Router<Arc<AppState<'static>>> {
    Router::new()
        // Namespace routes
        .route(
            "/namespaces",
            get(namespaces::list).post(namespaces::create),
        )
        // Metadata — tables
        .route(
            "/{ns}/m/tables",
            get(metadata::list_tables).post(metadata::create_table),
        )
        .route("/{ns}/m/tables/{table}", delete(metadata::drop_table))
        // Metadata — link tables
        .route(
            "/{ns}/m/links",
            get(metadata::list_links).post(metadata::create_link),
        )
        .route(
            "/{ns}/m/links/{link}",
            delete(metadata::drop_link).patch(metadata::alter_link),
        )
}
