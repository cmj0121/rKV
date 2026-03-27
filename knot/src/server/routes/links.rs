use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;

use super::namespaces::{not_found, ErrorBody};
use super::nodes::{json_map_to_props, props_to_json};
use crate::server::AppState;

pub async fn get_link(
    State(state): State<Arc<AppState>>,
    Path((ns, link, from, to)): Path<(String, String, String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let lnk = knot
        .link(&link)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    match lnk.get(&from, &to) {
        Ok(Some(entry)) => {
            let props = entry.properties.as_ref().map(props_to_json);
            Ok(Json(serde_json::json!({
                "from": entry.from,
                "to": entry.to,
                "properties": props,
            })))
        }
        Ok(None) => Err(not_found("link entry")),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e)))),
    }
}

pub async fn put_link(
    State(state): State<Arc<AppState>>,
    Path((ns, link, from, to)): Path<(String, String, String, String)>,
    body: Option<Json<HashMap<String, serde_json::Value>>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let lnk = knot
        .link(&link)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    match body {
        Some(Json(map)) if !map.is_empty() => {
            let props = json_map_to_props(&map)?;
            lnk.insert(&from, &to, &props)
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
        }
        _ => {
            lnk.insert_bare(&from, &to)
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
        }
    }
    Ok(StatusCode::CREATED)
}

pub async fn delete_link(
    State(state): State<Arc<AppState>>,
    Path((ns, link, from, to)): Path<(String, String, String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let lnk = knot
        .link(&link)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    lnk.delete(&from, &to)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

#[derive(serde::Deserialize)]
pub struct LinkScanParams {
    pub from: Option<String>,
    pub to: Option<String>,
    pub detail: Option<bool>,
}

pub async fn scan_links(
    State(state): State<Arc<AppState>>,
    Path((ns, link)): Path<(String, String)>,
    Query(params): Query<LinkScanParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let lnk = knot
        .link(&link)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    let entries = if let Some(from_key) = &params.from {
        lnk.from(from_key)
    } else if let Some(to_key) = &params.to {
        lnk.to(to_key)
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody::new("?from= or ?to= required")),
        ));
    }
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;

    if params.detail.unwrap_or(false) {
        let items: Vec<serde_json::Value> = entries
            .iter()
            .map(|e| {
                serde_json::json!({
                    "from": e.from,
                    "to": e.to,
                    "properties": e.properties.as_ref().map(props_to_json),
                })
            })
            .collect();
        Ok(Json(serde_json::json!({
            "entries": items,
            "has_more": false,
        })))
    } else {
        let keys: Vec<&str> = if params.from.is_some() {
            entries.iter().map(|e| e.to.as_str()).collect()
        } else {
            entries.iter().map(|e| e.from.as_str()).collect()
        };
        Ok(Json(serde_json::json!({
            "keys": keys,
            "has_more": false,
        })))
    }
}
