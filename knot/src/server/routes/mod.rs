pub mod batch;
pub mod graph;
pub mod health;
pub mod links;
pub mod metadata;
pub mod namespaces;
pub mod nodes;

use std::sync::Arc;

use axum::routing::{delete, get, post};
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
        // Data table CRUD
        .route(
            "/{ns}/t/{table}/{key}",
            get(nodes::get_node)
                .put(nodes::put_node)
                .patch(nodes::patch_node)
                .delete(nodes::delete_node)
                .head(nodes::head_node),
        )
        .route("/{ns}/t/{table}", get(nodes::scan_nodes))
        // Link entry CRUD
        .route(
            "/{ns}/l/{link}/{from}/{to}",
            get(links::get_link)
                .put(links::put_link)
                .delete(links::delete_link),
        )
        .route("/{ns}/l/{link}", get(links::scan_links))
        // Graph traversal (catch-all for variable link path segments)
        .route("/{ns}/g/{*path}", get(graph::directed))
        // Batch
        .route("/{ns}/batch", post(batch::batch))
}
