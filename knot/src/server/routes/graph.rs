use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;

use super::namespaces::{not_found, ErrorBody};
use crate::server::AppState;

#[derive(serde::Deserialize)]
pub struct TraversalParams {
    pub detail: Option<bool>,
    pub page_size: Option<usize>,
    pub max: Option<usize>,
    pub bidi: Option<bool>,
}

pub async fn directed(
    State(state): State<Arc<AppState>>,
    Path((ns, path)): Path<(String, String)>,
    Query(params): Query<TraversalParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    // path = {table}/{key} or {table}/{key}/{link1}/{link2}/...
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody::new("expected /g/{table}/{key}/{links...}")),
        ));
    }

    let table = parts[0];
    let key = parts[1];

    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;

    if parts.len() == 2 {
        // Discovery mode: ?max=N required
        let max_hops = params.max.ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new("?max=N required for discovery")),
            )
        })?;
        let bidi = params.bidi.unwrap_or(false);

        let result = knot
            .discover(table, key, max_hops, bidi)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;

        let leaves: Vec<String> = result.leaves.iter().map(|(_, k)| k.clone()).collect();
        return Ok(Json(serde_json::json!({
            "leaves": leaves,
            "cursor": null,
        })));
    }

    // Directed: links are parts[2..]
    let link_names: Vec<&str> = parts[2..].to_vec();
    let with_paths = params.detail.unwrap_or(false);

    let result = knot
        .traverse(table, key, &link_names, None, None, with_paths)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;

    let leaves: Vec<String> = result.leaves.iter().map(|(_, k)| k.clone()).collect();

    if with_paths {
        let paths: Vec<Vec<String>> = result
            .paths
            .unwrap_or_default()
            .iter()
            .map(|p| p.iter().map(|(_, k)| k.clone()).collect())
            .collect();
        Ok(Json(serde_json::json!({
            "leaves": leaves,
            "paths": paths,
            "cursor": null,
        })))
    } else {
        Ok(Json(serde_json::json!({
            "leaves": leaves,
            "cursor": null,
        })))
    }
}
