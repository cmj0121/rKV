mod health;
mod keys;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;

use super::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(health::root))
        .route("/health", get(health::health))
        .route(
            "/api/{ns}/keys/{key}",
            get(keys::get_key)
                .put(keys::put_key)
                .delete(keys::delete_key)
                .head(keys::head_key),
        )
        .with_state(state)
}
