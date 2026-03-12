use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::server::error::ServerError;
use crate::server::types::parse_key;
use crate::server::AppState;
use crate::Namespace;

/// GET /api/{ns}/keys/{key} -> 200 (data) / 204 (null) / 410 (deleted) / 404
pub async fn get_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;

    let (value, rev) = match ns.get_with_revision(key.clone()) {
        Ok(vr) => vr,
        Err(crate::Error::KeyNotFound) => {
            // Check if tombstoned via get_raw
            return match ns.get_raw(key)? {
                Some(v) if v.is_tombstone() => Ok(StatusCode::GONE.into_response()),
                _ => Err(ServerError::from(crate::Error::KeyNotFound)),
            };
        }
        Err(e) => return Err(ServerError::from(e)),
    };

    if value.is_null() {
        let mut resp = StatusCode::NO_CONTENT.into_response();
        resp.headers_mut()
            .insert("X-RKV-Revision", rev.to_string().parse().unwrap());
        append_ttl_header(&mut resp, &ns, &key);
        return Ok(resp);
    }

    let body = value_to_json_bytes(&value);
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    resp.headers_mut()
        .insert("X-RKV-Revision", rev.to_string().parse().unwrap());
    append_ttl_header(&mut resp, &ns, &key);
    Ok(resp)
}

#[derive(Deserialize)]
pub struct PutQuery {
    pub ttl: Option<String>,
}

/// PUT /api/{ns}/keys/{key}?ttl=30s -> 201
pub async fn put_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
    Query(query): Query<PutQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    if state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    let ttl = query
        .ttl
        .as_deref()
        .and_then(parse_ttl_string)
        .or_else(|| parse_expires_header(&headers))
        .or_else(|| parse_ttl_header(&headers));
    let value = json_body_to_value(&body)?;
    let rev = ns.put(key, value, ttl)?;

    let mut resp = StatusCode::CREATED.into_response();
    resp.headers_mut()
        .insert("X-RKV-Revision", rev.to_string().parse().unwrap());
    Ok(resp)
}

/// DELETE /api/{ns}/keys/{key} -> 202
pub async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<StatusCode, ServerError> {
    if state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    ns.delete(key)?;
    Ok(StatusCode::ACCEPTED)
}

/// HEAD /api/{ns}/keys/{key} -> 200 / 204 / 404
pub async fn head_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    let value = ns.get(key.clone())?;

    let status = if value.is_null() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::OK
    };
    let mut resp = status.into_response();
    append_ttl_header(&mut resp, &ns, &key);
    Ok(resp)
}

/// Append Expires header if the key has a TTL.
fn append_ttl_header(resp: &mut Response, ns: &Namespace<'_>, key: &crate::Key) {
    if let Ok(Some(ttl)) = ns.ttl(key.clone()) {
        if let Some(expires) = SystemTime::now().checked_add(ttl) {
            let datetime = httpdate::fmt_http_date(expires);
            if let Ok(val) = datetime.parse() {
                resp.headers_mut().insert("Expires", val);
            }
        }
    }
}

/// Parse the Expires header into a Duration (TTL from now).
fn parse_expires_header(headers: &HeaderMap) -> Option<Duration> {
    let val = headers.get("Expires")?.to_str().ok()?;
    let expires = httpdate::parse_http_date(val).ok()?;
    expires.duration_since(SystemTime::now()).ok()
}

/// Parse a human-readable TTL string into a Duration.
///
/// Accepts suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
/// A plain number is treated as seconds (e.g. `120` = 2 minutes).
fn parse_ttl_string(s: &str) -> Option<Duration> {
    let s = s.trim();
    let (num, unit) = if let Some(n) = s.strip_suffix('d') {
        (n.parse::<u64>().ok()?, 86400)
    } else if let Some(n) = s.strip_suffix('h') {
        (n.parse::<u64>().ok()?, 3600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n.parse::<u64>().ok()?, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n.parse::<u64>().ok()?, 1)
    } else {
        (s.parse::<u64>().ok()?, 1)
    };
    Some(Duration::from_secs(num * unit))
}

/// Parse the `X-RKV-TTL` header into a Duration.
fn parse_ttl_header(headers: &HeaderMap) -> Option<Duration> {
    let val = headers.get("X-RKV-TTL")?.to_str().ok()?;
    parse_ttl_string(val)
}

/// Convert a JSON body to a Value.
/// `"hello"` -> Data(b"hello"), `42` -> Data(b"42"), `null` -> Null
fn json_body_to_value(body: &[u8]) -> Result<crate::Value, ServerError> {
    let json: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ServerError::BadRequest("invalid JSON body"))?;

    match json {
        serde_json::Value::String(s) => Ok(crate::Value::from(s)),
        serde_json::Value::Number(n) => Ok(crate::Value::from(n.to_string())),
        serde_json::Value::Null => Ok(crate::Value::Null),
        serde_json::Value::Bool(b) => Ok(crate::Value::from(b.to_string())),
        _ => Err(ServerError::BadRequest(
            "value must be a JSON string, number, boolean, or null",
        )),
    }
}

/// Convert a Value to JSON bytes for the response body.
pub(super) fn value_to_json_bytes(value: &crate::Value) -> Vec<u8> {
    match value.as_bytes() {
        Some(bytes) => {
            if let Ok(s) = std::str::from_utf8(bytes) {
                serde_json::to_vec(&s).unwrap_or_else(|_| bytes.to_vec())
            } else {
                bytes.to_vec()
            }
        }
        None => b"null".to_vec(),
    }
}
