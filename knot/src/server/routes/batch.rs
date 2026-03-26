use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

use super::namespaces::ErrorBody;
use super::nodes::json_map_to_props;
use crate::server::AppState;

#[derive(serde::Deserialize)]
pub struct BatchRequest {
    pub ops: Vec<BatchOp>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "op")]
pub enum BatchOp {
    #[serde(rename = "put")]
    Put {
        table: String,
        key: String,
        properties: Option<HashMap<String, serde_json::Value>>,
    },
    #[serde(rename = "del")]
    Del {
        table: String,
        key: String,
        #[serde(default)]
        cascade: bool,
    },
    #[serde(rename = "put-link")]
    PutLink {
        link: String,
        from: String,
        to: String,
        properties: Option<HashMap<String, serde_json::Value>>,
    },
    #[serde(rename = "del-link")]
    DelLink {
        link: String,
        from: String,
        to: String,
    },
}

pub async fn batch(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    Json(body): Json<BatchRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;

    let mut results = Vec::new();

    for op in &body.ops {
        let result = match op {
            BatchOp::Put {
                table,
                key,
                properties,
            } => exec_put(knot, table, key, properties.as_ref()),
            BatchOp::Del {
                table,
                key,
                cascade,
            } => exec_del(knot, table, key, *cascade),
            BatchOp::PutLink {
                link,
                from,
                to,
                properties,
            } => exec_put_link(knot, link, from, to, properties.as_ref()),
            BatchOp::DelLink { link, from, to } => exec_del_link(knot, link, from, to),
        };

        results.push(match result {
            Ok(v) => v,
            Err(e) => serde_json::json!({"ok": false, "error": e}),
        });
    }

    Ok(Json(serde_json::json!({"results": results})))
}

fn exec_put(
    knot: &crate::Knot,
    table: &str,
    key: &str,
    properties: Option<&HashMap<String, serde_json::Value>>,
) -> Result<serde_json::Value, String> {
    let tbl = knot.table(table).map_err(|e| e.to_string())?;
    match properties {
        Some(map) if !map.is_empty() => {
            let props = json_map_to_props(map).map_err(|(_, Json(e))| e.error)?;
            tbl.insert(key, &props).map_err(|e| e.to_string())?;
        }
        _ => {
            tbl.insert_set(key).map_err(|e| e.to_string())?;
        }
    }
    Ok(serde_json::json!({"op": "put", "key": key, "ok": true}))
}

fn exec_del(
    knot: &crate::Knot,
    table: &str,
    key: &str,
    cascade: bool,
) -> Result<serde_json::Value, String> {
    let tbl = knot.table(table).map_err(|e| e.to_string())?;
    if cascade {
        tbl.delete_cascade(key, true).map_err(|e| e.to_string())?;
    } else {
        tbl.delete(key).map_err(|e| e.to_string())?;
    }
    Ok(serde_json::json!({"op": "del", "key": key, "ok": true}))
}

fn exec_put_link(
    knot: &crate::Knot,
    link: &str,
    from: &str,
    to: &str,
    properties: Option<&HashMap<String, serde_json::Value>>,
) -> Result<serde_json::Value, String> {
    let lnk = knot.link(link).map_err(|e| e.to_string())?;
    match properties {
        Some(map) if !map.is_empty() => {
            let props = json_map_to_props(map).map_err(|(_, Json(e))| e.error)?;
            lnk.insert(from, to, &props).map_err(|e| e.to_string())?;
        }
        _ => {
            lnk.insert_bare(from, to).map_err(|e| e.to_string())?;
        }
    }
    Ok(serde_json::json!({"op": "put-link", "from": from, "to": to, "ok": true}))
}

fn exec_del_link(
    knot: &crate::Knot,
    link: &str,
    from: &str,
    to: &str,
) -> Result<serde_json::Value, String> {
    let lnk = knot.link(link).map_err(|e| e.to_string())?;
    lnk.delete(from, to).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({"op": "del-link", "from": from, "to": to, "ok": true}))
}
