use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::server::error::ServerError;
use crate::server::AppState;

#[derive(Deserialize)]
pub struct CreateNamespaceRequest {
    name: String,
    password: Option<String>,
}

/// GET /api/namespaces -> list all namespaces
pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<String>>, ServerError> {
    Ok(Json(state.db.list_namespaces()?))
}

/// POST /api/namespaces -> create/open namespace
pub async fn create_namespace(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<StatusCode, ServerError> {
    if state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }
    state.db.namespace(&req.name, req.password.as_deref())?;

    // Cache password for subsequent CRUD requests
    if let Some(pw) = req.password {
        state
            .ns_passwords
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(req.name, pw);
    }
    Ok(StatusCode::CREATED)
}

/// DELETE /api/{ns} -> drop namespace
pub async fn drop_namespace(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
) -> Result<StatusCode, ServerError> {
    if state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }
    state.db.drop_namespace(&ns_name)?;
    state
        .ns_passwords
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&ns_name);
    Ok(StatusCode::ACCEPTED)
}

#[derive(Deserialize)]
pub struct SetNsDedupRequest {
    enabled: Option<bool>,
}

/// PUT /api/{ns}/dedup — set or clear per-namespace dedup override
///
/// Request body: `{"enabled": true}` to override, `{}` or `{"enabled": null}` to reset.
pub async fn set_ns_dedup(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
    Json(req): Json<SetNsDedupRequest>,
) -> Json<serde_json::Value> {
    match req.enabled {
        Some(v) => {
            state.db.set_namespace_dedup(&ns_name, v);
            Json(serde_json::json!({
                "ok": true,
                "namespace": ns_name,
                "dedup": v,
            }))
        }
        None => {
            state.db.clear_namespace_dedup(&ns_name);
            Json(serde_json::json!({
                "ok": true,
                "namespace": ns_name,
                "dedup": state.db.dedup_enabled(&ns_name),
                "source": "global",
            }))
        }
    }
}

/// GET /api/{ns}/dedup — check dedup status for a namespace
pub async fn get_ns_dedup(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "namespace": ns_name,
        "dedup": state.db.dedup_enabled(&ns_name),
        "global": state.db.dedup(),
    }))
}
