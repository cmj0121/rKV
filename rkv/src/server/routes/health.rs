use std::sync::Arc;

use axum::extract::State;
use axum::Json;

use crate::server::AppState;

pub async fn root() -> &'static str {
    "\"\""
}

pub async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let s = state.db.stats();
    Json(serde_json::json!({
        "status": "ok",
        "role": s.role,
        "peer_count": s.peer_count,
        "uptime_secs": s.uptime.as_secs(),
    }))
}
