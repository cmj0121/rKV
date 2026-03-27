use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::server::AppState;

#[derive(Deserialize)]
pub struct CreateNamespace {
    pub name: String,
}

#[derive(Serialize)]
pub struct NamespaceInfo {
    pub name: String,
}

pub async fn list(State(state): State<Arc<AppState>>) -> Json<Vec<String>> {
    Json(state.discover_namespaces())
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateNamespace>,
) -> Result<(StatusCode, Json<NamespaceInfo>), (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&body.name)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
    Ok((StatusCode::CREATED, Json(NamespaceInfo { name: body.name })))
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

pub async fn drop(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(ns): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    // Drop all tables (which cascades to link tables) then remove from memory
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    let mut namespaces = state.namespaces.write().unwrap();
    if let Some(mut knot) = namespaces.remove(&ns) {
        // Drop all tables (cascades to links)
        let tables: Vec<String> = knot.tables();
        for t in &tables {
            let _ = knot.drop_table(t);
        }
        // Drop remaining link tables
        let links: Vec<String> = knot.links();
        for l in &links {
            let _ = knot.drop_link(l);
        }
        // Drop metadata namespace
        let _ = state.backend.drop_namespace(&format!("knot.{ns}.meta"));
    }
    Ok(StatusCode::OK)
}

impl From<crate::Error> for ErrorBody {
    fn from(e: crate::Error) -> Self {
        Self {
            error: e.to_string(),
        }
    }
}
