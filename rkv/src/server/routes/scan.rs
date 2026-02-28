use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::server::error::ServerError;
use crate::server::types::{format_key, parse_key};
use crate::server::AppState;

const SCAN_LIMIT: usize = 40;

#[derive(Deserialize)]
pub struct ScanParams {
    pub prefix: Option<String>,
    pub offset: Option<usize>,
    pub reverse: Option<bool>,
    pub deleted: Option<bool>,
}

#[derive(Deserialize)]
pub struct CountParams {
    pub prefix: Option<String>,
}

#[derive(Deserialize)]
pub struct DeleteParams {
    pub prefix: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub inclusive: Option<bool>,
}

/// GET /api/{ns}/keys -> scan keys, return plain JSON array
pub async fn list_keys(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
    Query(params): Query<ScanParams>,
) -> Result<Response, ServerError> {
    let ns = state.namespace(&ns_name)?;
    let offset = params.offset.unwrap_or(0);
    let prefix = params
        .prefix
        .as_deref()
        .map(parse_key)
        .unwrap_or_else(|| crate::Key::from(""));

    let include_deleted = params.deleted.unwrap_or(false);
    let keys = if params.reverse.unwrap_or(false) {
        ns.rscan(&prefix, SCAN_LIMIT + 1, offset, include_deleted)?
    } else {
        ns.scan(&prefix, SCAN_LIMIT + 1, offset, include_deleted)?
    };

    let has_more = keys.len() > SCAN_LIMIT;
    let keys: Vec<String> = keys.iter().take(SCAN_LIMIT).map(format_key).collect();

    let mut resp = Json(keys).into_response();
    if has_more {
        resp.headers_mut()
            .insert("X-RKV-Has-More", "true".parse().unwrap());
    }
    Ok(resp)
}

/// GET /api/{ns}/count -> key count
pub async fn count_keys(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
    Query(params): Query<CountParams>,
) -> Result<Json<u64>, ServerError> {
    let ns = state.namespace(&ns_name)?;

    if let Some(prefix) = params.prefix {
        let prefix_key = parse_key(&prefix);
        // Paginate to avoid loading all keys into memory at once
        let mut count = 0u64;
        let mut offset = 0;
        loop {
            let batch = ns.scan(&prefix_key, SCAN_LIMIT, offset, false)?;
            count += batch.len() as u64;
            if batch.len() < SCAN_LIMIT {
                break;
            }
            offset += SCAN_LIMIT;
        }
        Ok(Json(count))
    } else {
        Ok(Json(ns.count()?))
    }
}

/// DELETE /api/{ns}/keys -> delete by prefix or range
pub async fn delete_keys(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
    Query(params): Query<DeleteParams>,
) -> Result<Response, ServerError> {
    let ns = state.namespace(&ns_name)?;

    if let Some(prefix) = params.prefix {
        let n = ns.delete_prefix(&prefix)?;
        return Ok((StatusCode::ACCEPTED, Json(n)).into_response());
    }

    if let (Some(start), Some(end)) = (params.start, params.end) {
        let inclusive = params.inclusive.unwrap_or(false);
        let n = ns.delete_range(parse_key(&start), parse_key(&end), inclusive)?;
        return Ok((StatusCode::ACCEPTED, Json(n)).into_response());
    }

    Err(ServerError::BadRequest(
        "missing prefix or start/end parameters",
    ))
}
