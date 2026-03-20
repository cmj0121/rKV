use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::server::error::ServerError;
use crate::server::types::parse_key;
use crate::server::AppState;
use crate::WriteBatch;

#[derive(Deserialize)]
pub struct BatchRequest {
    pub ops: Vec<BatchOpRequest>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum BatchOpRequest {
    Put {
        key: String,
        value: serde_json::Value,
        ttl: Option<u64>,
        dedup: Option<bool>,
    },
    Delete {
        key: String,
    },
}

#[derive(Serialize)]
pub struct BatchResponse {
    pub results: Vec<BatchResultEntry>,
}

#[derive(Serialize)]
pub struct BatchResultEntry {
    pub key: String,
    pub revision: String,
}

/// POST /api/{ns}/batch -> 200 (results) / 400 (empty) / 403 (replica)
pub async fn write_batch(
    State(state): State<Arc<AppState>>,
    Path(ns_name): Path<String>,
    Json(req): Json<BatchRequest>,
) -> Result<Response, ServerError> {
    if state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }

    if req.ops.is_empty() {
        return Err(ServerError::BadRequest(
            "batch must contain at least one op",
        ));
    }

    let ns = state.namespace(&ns_name)?;

    let mut batch = WriteBatch::new();
    let mut keys: Vec<String> = Vec::with_capacity(req.ops.len());

    for op in req.ops {
        match op {
            BatchOpRequest::Put {
                key,
                value,
                ttl,
                dedup,
            } => {
                let parsed_key = parse_key(&key);
                let parsed_value = json_to_value(&value)?;
                let ttl = ttl.map(Duration::from_secs);
                batch = batch.put_dedup(parsed_key, parsed_value, ttl, dedup);
                keys.push(key);
            }
            BatchOpRequest::Delete { key } => {
                let parsed_key = parse_key(&key);
                batch = batch.delete(parsed_key);
                keys.push(key);
            }
        }
    }

    let revisions = ns.write_batch(batch)?;

    let results: Vec<BatchResultEntry> = keys
        .into_iter()
        .zip(revisions.iter())
        .map(|(key, rev)| BatchResultEntry {
            key,
            revision: rev.to_string(),
        })
        .collect();

    Ok((StatusCode::OK, Json(BatchResponse { results })).into_response())
}

/// Convert a serde_json::Value to a crate::Value.
fn json_to_value(json: &serde_json::Value) -> Result<crate::Value, ServerError> {
    match json {
        serde_json::Value::String(s) => Ok(crate::Value::from(s.as_str())),
        serde_json::Value::Number(n) => Ok(crate::Value::from(n.to_string())),
        serde_json::Value::Null => Ok(crate::Value::Null),
        serde_json::Value::Bool(b) => Ok(crate::Value::from(b.to_string())),
        _ => Err(ServerError::BadRequest(
            "value must be a JSON string, number, boolean, or null",
        )),
    }
}
