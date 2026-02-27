mod health;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;

use super::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(health::root))
        .route("/health", get(health::health))
        .with_state(state)
}
