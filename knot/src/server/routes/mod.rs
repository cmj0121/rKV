pub mod health;

use std::sync::Arc;

use axum::Router;

use super::AppState;

pub fn api_routes() -> Router<Arc<AppState<'static>>> {
    Router::new().route("/health", axum::routing::get(health::health))
}
