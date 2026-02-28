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
    state.db.namespace(&req.name, req.password.as_deref())?;

    // Cache password for subsequent CRUD requests
    if let Some(pw) = req.password {
        state
            .ns_passwords
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(req.name, pw);
    }
    Ok(StatusCode::OK)
}

/// DELETE /api/{ns} -> drop namespace
pub async fn drop_namespace(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
) -> Result<StatusCode, ServerError> {
    state.db.drop_namespace(&ns_name)?;
    state
        .ns_passwords
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&ns_name);
    Ok(StatusCode::ACCEPTED)
}
