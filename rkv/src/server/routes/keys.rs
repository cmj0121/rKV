use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::server::error::ServerError;
use crate::server::types::parse_key;
use crate::server::AppState;

/// GET /api/{ns}/keys/{key} -> 200 (data) / 204 (null) / 404
pub async fn get_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    let value = ns.get(key.clone())?;

    if value.is_null() {
        let mut resp = StatusCode::NO_CONTENT.into_response();
        append_ttl_header(&mut resp, &state, &ns_name, &key);
        return Ok(resp);
    }

    let body = value_to_json_bytes(&value);
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    append_ttl_header(&mut resp, &state, &ns_name, &key);
    Ok(resp)
}

/// PUT /api/{ns}/keys/{key} -> 201
pub async fn put_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServerError> {
    let key = parse_key(&raw_key);
    let ns = state.namespace(&ns_name)?;
    let ttl = parse_expires_header(&headers);
    let value = json_body_to_value(&body)?;
    let rev = ns.put(key, value, ttl)?;

    let mut resp = StatusCode::CREATED.into_response();
    resp.headers_mut()
        .insert("X-RKV-Revision", rev.to_string().parse().unwrap());
    Ok(resp)
}

/// DELETE /api/{ns}/keys/{key} -> 202 / 404
pub async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path((ns_name, raw_key)): Path<(String, String)>,
) -> Result<StatusCode, ServerError> {
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
    append_ttl_header(&mut resp, &state, &ns_name, &key);
    Ok(resp)
}

/// Append Expires header if the key has a TTL.
fn append_ttl_header(resp: &mut Response, state: &Arc<AppState>, ns_name: &str, key: &rkv::Key) {
    if let Ok(ns) = state.namespace(ns_name) {
        if let Ok(Some(ttl)) = ns.ttl(key.clone()) {
            if let Some(expires) = SystemTime::now().checked_add(ttl) {
                let datetime = httpdate::fmt_http_date(expires);
                if let Ok(val) = datetime.parse() {
                    resp.headers_mut().insert("Expires", val);
                }
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

/// Convert a JSON body to a Value.
/// `"hello"` -> Data(b"hello"), `42` -> Data(b"42"), `null` -> Null
fn json_body_to_value(body: &[u8]) -> Result<rkv::Value, ServerError> {
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| ServerError(rkv::Error::InvalidKey("invalid JSON body".to_owned())))?;

    match json {
        serde_json::Value::String(s) => Ok(rkv::Value::from(s)),
        serde_json::Value::Number(n) => Ok(rkv::Value::from(n.to_string())),
        serde_json::Value::Null => Ok(rkv::Value::Null),
        serde_json::Value::Bool(b) => Ok(rkv::Value::from(b.to_string())),
        _ => Err(ServerError(rkv::Error::InvalidKey(
            "value must be a JSON string, number, boolean, or null".to_owned(),
        ))),
    }
}

/// Convert a Value to JSON bytes for the response body.
pub(super) fn value_to_json_bytes(value: &rkv::Value) -> Vec<u8> {
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
