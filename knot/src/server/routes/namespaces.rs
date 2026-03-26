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

pub async fn list(State(state): State<Arc<AppState<'static>>>) -> Json<Vec<String>> {
    let ns = state.namespaces.read().unwrap();
    let names: Vec<String> = ns.keys().cloned().collect();
    Json(names)
}

pub async fn create(
    State(state): State<Arc<AppState<'static>>>,
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

impl From<crate::Error> for ErrorBody {
    fn from(e: crate::Error) -> Self {
        Self {
            error: e.to_string(),
        }
    }
}
