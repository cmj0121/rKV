use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::server::error::ServerError;
use crate::server::types::parse_key;
use crate::server::AppState;

/// GET /api/{ns}/keys/{key}/revisions -> revision count
pub async fn rev_count(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<Json<u64>, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    Ok(Json(ns.rev_count(key)?))
}

/// GET /api/{ns}/keys/{key}/revisions/{index} -> value at revision
pub async fn rev_get(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key, index)): Path<(String, String, u64)>,
) -> Result<Response, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    let value = ns.rev_get(key, index)?;

    let body = super::keys::value_to_json_bytes(&value);
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    Ok(resp)
}

/// GET /api/{ns}/keys/{key}/ttl -> remaining TTL in seconds
pub async fn get_ttl(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;

    match ns.ttl(key)? {
        Some(duration) => Ok(Json(serde_json::json!(duration.as_secs()))),
        None => Ok(Json(serde_json::Value::Null)),
    }
}
